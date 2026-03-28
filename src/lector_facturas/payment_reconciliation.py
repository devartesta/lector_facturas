"""Reconcile accounting sales vs payment channel charges (Shopify Payments + PayPal).

Sources
-------
Accounting : finance.informe_vat_gestorias_detalle
             One row per order per period; shown_gross_presentment <> 0 covers both
             sales and refund lines.

Payment    : invoices.shopify_payout_transactions   (Shopify Payments)
             invoices.paypal_transactions_raw        (PayPal)

The join key is order_name (e.g. "AS-12345", "UK-29364").

Three output buckets per channel (Shopify / PayPal):
  only_accounting  – in accounting, no matching charge in payment channel
  only_payment     – charge in payment channel, no matching accounting entry for the period
  amount_diff      – in both, |accounting - payment| > 0.01

Chargeback lifecycle
--------------------
Shopify:
  dispute_withdrawal → money deducted from payout (dispute opened)
  dispute_reversal   → money returned (dispute WON)
  No reversal        → dispute still open or lost

PayPal:
  T1111 → dispute hold (money retained, stored as positive bruto, we negate it)
  T1112 → dispute resolved in our favour (money returned) — rare, tracked when present
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from decimal import Decimal

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover
    psycopg = None  # type: ignore[assignment]
    dict_row = None  # type: ignore[assignment]


SHOPIFY_ADMIN_BASE = "https://admin.shopify.com/store/artesta/orders"

# Maps company_code → accounting currency filter
COMPANY_CURRENCY: dict[str, str] = {
    "SL": "EUR",
    "LTD": "GBP",
    "INC": "USD",
}

# Chargeback status labels
CB_OPEN = "En disputa"   # withdrawal/hold present, no reversal yet
CB_WON  = "Ganado"       # reversal received — dispute resolved in our favour


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ReconciliationRow:
    order_name: str
    order_date: str | None              # "DD/MM/YYYY" as stored in DB
    shipping_country_code: str | None
    currency: str | None
    accounting_amount: Decimal | None   # shown_gross_presentment
    payment_amount: Decimal | None      # SUM of all movements from payment channel
    diff: Decimal | None                # payment_amount - accounting_amount (None if one side missing)
    shopify_url: str | None
    is_gift_card: bool
    is_chargeback: bool
    chargeback_status: str | None       # CB_OPEN / CB_WON / None


@dataclass
class ChannelReconciliation:
    only_accounting: list[ReconciliationRow] = field(default_factory=list)
    only_payment: list[ReconciliationRow] = field(default_factory=list)
    amount_diff: list[ReconciliationRow] = field(default_factory=list)


@dataclass
class ChargebackInventoryRow:
    """One row per order that has had a chargeback in the last 12 months."""
    channel: str                        # "Shopify" or "PayPal"
    order_name: str
    order_date: str | None              # original order date (from accounting)
    shipping_country_code: str | None
    currency: str | None
    accounting_amount: Decimal | None   # net sales amount booked in accounting
    withdrawal_date: str | None         # date dispute was opened / hold applied
    withdrawal_amount: Decimal | None   # amount retained (always negative — outflow)
    reversal_date: str | None           # date dispute was resolved (None if still open)
    reversal_amount: Decimal | None     # amount returned (positive, None if still open)
    net_impact: Decimal | None          # withdrawal + reversal (0 if won, negative if open)
    status: str                         # CB_OPEN or CB_WON
    shopify_url: str | None


@dataclass
class ReconciliationReport:
    period_yyyymm: str
    company_code: str
    shopify: ChannelReconciliation = field(default_factory=ChannelReconciliation)
    paypal: ChannelReconciliation = field(default_factory=ChannelReconciliation)
    chargeback_inventory: list[ChargebackInventoryRow] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_numeric_order_id(gid: str | None) -> str | None:
    """'gid://shopify/Order/6903815733586' → '6903815733586'"""
    if not gid:
        return None
    parts = gid.rsplit("/", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return parts[1]
    return None


def _shopify_url(pedido_id: str | None, order_id_gid: str | None) -> str | None:
    numeric = pedido_id or _extract_numeric_order_id(order_id_gid)
    if numeric:
        return f"{SHOPIFY_ADMIN_BASE}/{numeric}"
    return None


def _parse_gateways(raw: str | None) -> list[str]:
    try:
        return json.loads(raw) if raw else []
    except Exception:
        return []


def _dec(val: object) -> Decimal | None:
    if val is None:
        return None
    return Decimal(str(val))


def _qdec(val: object) -> Decimal | None:
    d = _dec(val)
    return d.quantize(Decimal("0.01")) if d is not None else None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def build_reconciliation(
    *,
    database_url: str,
    company_code: str,
    period_yyyymm: str,
) -> ReconciliationReport:
    """Query DB and return a ReconciliationReport for company_code / period_yyyymm."""
    if psycopg is None:
        raise RuntimeError("psycopg is not installed.")

    currency = COMPANY_CURRENCY.get(company_code)
    if not currency:
        raise ValueError(
            f"Unknown company_code '{company_code}'. Expected one of: {list(COMPANY_CURRENCY)}"
        )

    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        # -- Accounting: one row per order_name.
        # Includes both sales (positive) and refund lines (negative) so the net
        # per order matches what the payment channel should have settled.
        acct_rows = conn.execute(
            """
            SELECT
                order_name,
                order_date,
                shipping_country_code,
                payment_currency                                          AS currency,
                SUM(shown_gross_presentment)                             AS importe_contab,
                bool_or(payment_gateway_names @> '["gift_card"]'::jsonb) AS gift_card,
                payment_gateway_names::text                              AS gateways_raw
            FROM finance.informe_vat_gestorias_detalle
            WHERE order_month_yyyymm = %s
              AND payment_currency    = %s
              AND shown_gross_presentment <> 0
              AND (payment_gateway_names @> '["shopify_payments"]'::jsonb
                   OR payment_gateway_names @> '["paypal"]'::jsonb)
            GROUP BY order_name, order_date, shipping_country_code,
                     payment_currency, payment_gateway_names
            """,
            (period_yyyymm, currency),
        ).fetchall()

        # -- Payment: Shopify Payments.
        # Uses shopify_payout_transactions filtered by transaction_date in Madrid
        # timezone — this aligns with order_month_yyyymm in accounting.
        #
        # Chargeback detection:
        #   dispute_withdrawal → money deducted (dispute opened this period)
        #   dispute_reversal   → money returned (dispute WON this period)
        #   Both types mark the row as a chargeback; the status tells which phase.
        #   Note: withdrawal and reversal often fall in *different* months, so a row
        #   may show only the reversal (status = Ganado) or only the withdrawal
        #   (status = En disputa).
        shopify_pay_rows = conn.execute(
            """
            SELECT
                order_name,
                MAX(order_id)                          AS order_id,
                SUM(amount)                            AS importe_pago,
                bool_or(type IN (
                    'dispute_withdrawal', 'dispute_reversal'
                ))                                     AS tiene_chargeback,
                CASE
                    WHEN bool_or(type = 'dispute_reversal')   THEN 'Ganado'
                    WHEN bool_or(type = 'dispute_withdrawal')  THEN 'En disputa'
                    ELSE NULL
                END                                    AS chargeback_status
            FROM invoices.shopify_payout_transactions
            WHERE company_code  = %s
              AND to_char(transaction_date AT TIME ZONE 'Europe/Madrid', 'YYYYMM') = %s
              AND type IN ('charge', 'refund', 'dispute_withdrawal', 'dispute_reversal')
              AND order_name IS NOT NULL AND order_name <> ''
            GROUP BY order_name
            """,
            (company_code, period_yyyymm),
        ).fetchall()

        # -- Payment: PayPal.
        # Sums ALL order-linked movements: T0006 (sale), T1107 (refund),
        # T1111 (dispute hold — negated because bruto is stored positive but
        # represents a deduction), T1112 (dispute reversal — returned to us).
        #
        # T1111 often lacks shopify_order_name; linked via reference_transaction_id
        # to the parent T0006 transaction.
        paypal_pay_rows = conn.execute(
            """
            SELECT
                COALESCE(NULLIF(t.shopify_order_name, ''), parent.shopify_order_name)
                                        AS order_name,
                NULL::text              AS order_id,
                SUM(
                    CASE WHEN t.tipo = 'T1111' THEN -t.bruto ELSE t.bruto END
                )                       AS importe_pago,
                bool_or(t.tipo = 'T1111') AS tiene_chargeback,
                CASE
                    WHEN bool_or(t.tipo = 'T1112') THEN 'Ganado'
                    WHEN bool_or(t.tipo = 'T1111') THEN 'En disputa'
                    ELSE NULL
                END                     AS chargeback_status
            FROM invoices.paypal_transactions_raw t
            LEFT JOIN invoices.paypal_transactions_raw parent
                ON parent.transaction_id    = t.reference_transaction_id
               AND parent.shopify_order_name IS NOT NULL
               AND parent.shopify_order_name <> ''
            WHERE t.company_code = %s
              AND to_char(
                    t.transaction_date AT TIME ZONE 'Europe/Madrid',
                    'YYYYMM'
                  ) = %s
              AND (
                   (t.shopify_order_name IS NOT NULL AND t.shopify_order_name <> '')
                OR (parent.shopify_order_name IS NOT NULL AND parent.shopify_order_name <> '')
              )
            GROUP BY COALESCE(NULLIF(t.shopify_order_name, ''), parent.shopify_order_name)
            """,
            (company_code, period_yyyymm),
        ).fetchall()

        # -- pedido_id for Shopify links (accounting-only orders have no GID) --
        pedido_rows = conn.execute(
            """
            SELECT order_name, pedido_id::text AS pedido_id
            FROM finance.order_sales
            WHERE payment_currency = %s
              AND order_name IS NOT NULL AND order_name <> ''
            """,
            (currency,),
        ).fetchall()

        # -- Gift card orders (product is a gift card, not paid with gift card) --
        gift_card_rows = conn.execute(
            """
            SELECT DISTINCT pedido_id AS order_name
            FROM shopify.order_items
            WHERE gift_card = true
              AND pedido_id IS NOT NULL AND pedido_id <> ''
            """,
        ).fetchall()

        # -- Chargeback inventory: last 12 months, both channels --
        shopify_cb_rows = conn.execute(
            """
            SELECT
                order_name,
                MAX(order_id)  AS order_id,
                to_char(
                    MIN(CASE WHEN type = 'dispute_withdrawal'
                             THEN transaction_date AT TIME ZONE 'Europe/Madrid' END),
                    'DD/MM/YYYY'
                )              AS withdrawal_date,
                SUM(CASE WHEN type = 'dispute_withdrawal' THEN amount ELSE 0 END)
                               AS withdrawal_amount,
                to_char(
                    MIN(CASE WHEN type = 'dispute_reversal'
                             THEN transaction_date AT TIME ZONE 'Europe/Madrid' END),
                    'DD/MM/YYYY'
                )              AS reversal_date,
                SUM(CASE WHEN type = 'dispute_reversal' THEN amount ELSE 0 END)
                               AS reversal_amount,
                CASE
                    WHEN bool_or(type = 'dispute_reversal')  THEN 'Ganado'
                    ELSE 'En disputa'
                END            AS status
            FROM invoices.shopify_payout_transactions
            WHERE company_code = %s
              AND type IN ('dispute_withdrawal', 'dispute_reversal')
              AND transaction_date >= now() - interval '12 months'
              AND order_name IS NOT NULL AND order_name <> ''
            GROUP BY order_name
            ORDER BY
                CASE WHEN bool_or(type = 'dispute_reversal') THEN 1 ELSE 0 END ASC,  -- open first
                MIN(transaction_date) DESC
            """,
            (company_code,),
        ).fetchall()

        paypal_cb_rows = conn.execute(
            """
            SELECT
                COALESCE(NULLIF(t.shopify_order_name, ''), parent.shopify_order_name)
                               AS order_name,
                to_char(
                    MIN(CASE WHEN t.tipo = 'T1111'
                             THEN t.transaction_date AT TIME ZONE 'Europe/Madrid' END),
                    'DD/MM/YYYY'
                )              AS withdrawal_date,
                SUM(CASE WHEN t.tipo = 'T1111' THEN t.bruto ELSE 0 END)
                               AS withdrawal_amount,
                to_char(
                    MIN(CASE WHEN t.tipo = 'T1112'
                             THEN t.transaction_date AT TIME ZONE 'Europe/Madrid' END),
                    'DD/MM/YYYY'
                )              AS reversal_date,
                SUM(CASE WHEN t.tipo = 'T1112' THEN t.bruto ELSE 0 END)
                               AS reversal_amount,
                CASE
                    WHEN bool_or(t.tipo = 'T1112') THEN 'Ganado'
                    ELSE 'En disputa'
                END            AS status
            FROM invoices.paypal_transactions_raw t
            LEFT JOIN invoices.paypal_transactions_raw parent
                ON parent.transaction_id    = t.reference_transaction_id
               AND parent.shopify_order_name IS NOT NULL
               AND parent.shopify_order_name <> ''
            WHERE t.company_code = %s
              AND t.tipo IN ('T1111', 'T1112')
              AND t.transaction_date >= now() - interval '12 months'
              AND (
                   (t.shopify_order_name IS NOT NULL AND t.shopify_order_name <> '')
                OR (parent.shopify_order_name IS NOT NULL AND parent.shopify_order_name <> '')
              )
            GROUP BY COALESCE(NULLIF(t.shopify_order_name, ''), parent.shopify_order_name)
            ORDER BY
                CASE WHEN bool_or(t.tipo = 'T1112') THEN 1 ELSE 0 END ASC,
                MIN(t.transaction_date) DESC
            """,
            (company_code,),
        ).fetchall()

        # Accounting data for chargeback orders (to get order_date, country, amount)
        cb_order_names = (
            [r["order_name"] for r in shopify_cb_rows if r["order_name"]]
            + [r["order_name"] for r in paypal_cb_rows if r["order_name"]]
        )
        if cb_order_names:
            cb_acct_rows = conn.execute(
                """
                SELECT
                    order_name,
                    order_date,
                    shipping_country_code,
                    payment_currency AS currency,
                    SUM(shown_gross_presentment) AS importe_contab
                FROM finance.informe_vat_gestorias_detalle
                WHERE order_name = ANY(%s)
                  AND payment_currency = %s
                GROUP BY order_name, order_date, shipping_country_code, payment_currency
                """,
                (cb_order_names, currency),
            ).fetchall()
        else:
            cb_acct_rows = []

    # Build lookup maps
    pedido_id_map: dict[str, str] = {r["order_name"]: r["pedido_id"] for r in pedido_rows}
    gift_card_orders: set[str] = {r["order_name"] for r in gift_card_rows}
    cb_acct_map: dict[str, dict] = {r["order_name"]: dict(r) for r in cb_acct_rows}

    # -- Payment dicts (built first so mixed-gateway logic can reference them) --
    shopify_pay: dict[str, dict] = {r["order_name"]: dict(r) for r in shopify_pay_rows}
    paypal_pay: dict[str, dict] = {r["order_name"]: dict(r) for r in paypal_pay_rows}

    # -- Accounting dict, split by channel --
    # payment_gateway_names lists ALL attempted gateways (including failed ones),
    # so an order with ["shopify_payments","paypal"] means one gateway failed and
    # the other succeeded. We assign the accounting entry to whichever channel
    # actually has the charge in the payment tables.
    shopify_acct: dict[str, dict] = {}
    paypal_acct: dict[str, dict] = {}
    for r in acct_rows:
        gw = _parse_gateways(r.get("gateways_raw"))
        has_shopify = "shopify_payments" in gw
        has_paypal  = "paypal" in gw
        if has_shopify and has_paypal:
            # Mixed gateway: follow where the actual charge landed
            in_shopify_pay = r["order_name"] in shopify_pay
            in_paypal_pay  = r["order_name"] in paypal_pay
            if in_paypal_pay and not in_shopify_pay:
                paypal_acct[r["order_name"]] = dict(r)
            elif in_shopify_pay and not in_paypal_pay:
                shopify_acct[r["order_name"]] = dict(r)
            else:
                # Both or neither: add to both channels
                shopify_acct[r["order_name"]] = dict(r)
                paypal_acct[r["order_name"]] = dict(r)
        else:
            if has_shopify:
                shopify_acct[r["order_name"]] = dict(r)
            if has_paypal:
                paypal_acct[r["order_name"]] = dict(r)

    def _make_row(a: dict | None, p: dict | None, name: str) -> ReconciliationRow:
        accounting_amount = _qdec(a["importe_contab"]) if a else None
        payment_amount    = _qdec(_dec(p["importe_pago"])) if p else None
        diff: Decimal | None = None
        if accounting_amount is not None and payment_amount is not None:
            diff = (payment_amount - accounting_amount).quantize(Decimal("0.01"))
        url = _shopify_url(pedido_id_map.get(name), p["order_id"] if p else None)
        return ReconciliationRow(
            order_name=name,
            order_date=str(a["order_date"]) if a else None,
            shipping_country_code=str(a["shipping_country_code"]) if a else None,
            currency=str(a["currency"]) if a else None,
            accounting_amount=accounting_amount,
            payment_amount=payment_amount,
            diff=diff,
            shopify_url=url,
            is_gift_card=(name in gift_card_orders) or (bool(a.get("gift_card")) if a else False),
            is_chargeback=bool(p.get("tiene_chargeback")) if p else False,
            chargeback_status=str(p["chargeback_status"]) if p and p.get("chargeback_status") else None,
        )

    def _reconcile(
        acct: dict[str, dict],
        pay: dict[str, dict],
    ) -> ChannelReconciliation:
        all_names = set(acct.keys()) | set(pay.keys())
        result = ChannelReconciliation()
        for name in sorted(all_names):
            a, p = acct.get(name), pay.get(name)
            row = _make_row(a, p, name)
            if a and not p:
                result.only_accounting.append(row)
            elif p and not a:
                if row.payment_amount and row.payment_amount > Decimal("0"):
                    result.only_payment.append(row)
            elif a and p:
                if row.diff is not None and abs(row.diff) > Decimal("0.01"):
                    result.amount_diff.append(row)
        result.amount_diff.sort(
            key=lambda r: abs(r.diff or Decimal("0")), reverse=True
        )
        return result

    # -- Chargeback inventory --
    chargeback_inventory: list[ChargebackInventoryRow] = []

    for r in shopify_cb_rows:
        name  = r["order_name"]
        acct  = cb_acct_map.get(name)
        w_amt = _qdec(r["withdrawal_amount"])   # negative (Shopify stores as negative)
        v_amt = _qdec(r["reversal_amount"])      # positive (money returned)
        net   = None
        if w_amt is not None:
            net = (w_amt + (v_amt or Decimal("0"))).quantize(Decimal("0.01"))
        chargeback_inventory.append(ChargebackInventoryRow(
            channel="Shopify",
            order_name=name,
            order_date=str(acct["order_date"]) if acct else None,
            shipping_country_code=str(acct["shipping_country_code"]) if acct else None,
            currency=str(acct["currency"]) if acct else currency,
            accounting_amount=_qdec(acct["importe_contab"]) if acct else None,
            withdrawal_date=r["withdrawal_date"],
            withdrawal_amount=w_amt,
            reversal_date=r["reversal_date"] or None,
            reversal_amount=v_amt if v_amt and v_amt != Decimal("0") else None,
            net_impact=net,
            status=r["status"],
            shopify_url=_shopify_url(pedido_id_map.get(name), r.get("order_id")),
        ))

    for r in paypal_cb_rows:
        name  = r["order_name"]
        acct  = cb_acct_map.get(name)
        # PayPal stores T1111 bruto as positive; financial impact is negative
        w_raw = _qdec(r["withdrawal_amount"])
        w_amt = (-w_raw).quantize(Decimal("0.01")) if w_raw else None
        v_amt = _qdec(r["reversal_amount"])
        net   = None
        if w_amt is not None:
            net = (w_amt + (v_amt or Decimal("0"))).quantize(Decimal("0.01"))
        chargeback_inventory.append(ChargebackInventoryRow(
            channel="PayPal",
            order_name=name,
            order_date=str(acct["order_date"]) if acct else None,
            shipping_country_code=str(acct["shipping_country_code"]) if acct else None,
            currency=str(acct["currency"]) if acct else currency,
            accounting_amount=_qdec(acct["importe_contab"]) if acct else None,
            withdrawal_date=r["withdrawal_date"],
            withdrawal_amount=w_amt,
            reversal_date=r["reversal_date"] or None,
            reversal_amount=v_amt if v_amt and v_amt != Decimal("0") else None,
            net_impact=net,
            status=r["status"],
            shopify_url=_shopify_url(pedido_id_map.get(name), None),
        ))

    # Sort: open first, then won; within each group most recent first
    chargeback_inventory.sort(
        key=lambda x: (0 if x.status == CB_OPEN else 1, x.withdrawal_date or "")
    )

    return ReconciliationReport(
        period_yyyymm=period_yyyymm,
        company_code=company_code,
        shopify=_reconcile(shopify_acct, shopify_pay),
        paypal=_reconcile(paypal_acct, paypal_pay),
        chargeback_inventory=chargeback_inventory,
    )
