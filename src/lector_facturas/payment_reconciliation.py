"""Reconcile accounting sales vs payment channel charges (Shopify Payments + PayPal).

Sources
-------
Accounting : finance.informe_vat_gestorias_detalle
             One row per order per period; shown_gross_presentment > 0 means a real sale.

Payment    : invoices.payment_order_transactions
             Shopify  → transaction_type = 'charge',  platform = 'shopify'
             PayPal   → transaction_type = 't0006',   platform = 'paypal'

The join key is order_name (e.g. "AS-12345", "UK-29364").

Three output buckets per channel (Shopify / PayPal):
  only_accounting  – in accounting, no matching charge in payment channel
  only_payment     – charge in payment channel, no matching accounting entry for the period
  amount_diff      – in both, |accounting - payment| > 0.01
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

# PayPal transaction-type codes that represent a completed sale
PAYPAL_SALE_TYPES = ("t0006",)


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
    payment_amount: Decimal | None      # SUM(gross_amount) from payment_order_transactions
    diff: Decimal | None                # payment_amount - accounting_amount (None if one side missing)
    shopify_url: str | None
    is_gift_card: bool
    is_chargeback: bool


@dataclass
class ChannelReconciliation:
    only_accounting: list[ReconciliationRow] = field(default_factory=list)
    only_payment: list[ReconciliationRow] = field(default_factory=list)
    amount_diff: list[ReconciliationRow] = field(default_factory=list)


@dataclass
class ReconciliationReport:
    period_yyyymm: str
    company_code: str
    shopify: ChannelReconciliation = field(default_factory=ChannelReconciliation)
    paypal: ChannelReconciliation = field(default_factory=ChannelReconciliation)


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
        # -- Accounting: one row per order_name, only positive (real sales) --
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
              AND shown_gross_presentment > 0
              AND (payment_gateway_names @> '["shopify_payments"]'::jsonb
                   OR payment_gateway_names @> '["paypal"]'::jsonb)
            GROUP BY order_name, order_date, shipping_country_code,
                     payment_currency, payment_gateway_names
            """,
            (period_yyyymm, currency),
        ).fetchall()

        # -- Payment: Shopify charges for this period --
        # period_yyyymm on the payment side = when the charge was processed
        shopify_pay_rows = conn.execute(
            """
            SELECT
                order_name,
                MAX(order_id)          AS order_id,
                SUM(gross_amount)      AS importe_pago,
                bool_or(is_chargeback) AS tiene_chargeback
            FROM invoices.payment_order_transactions
            WHERE company_code      = %s
              AND platform          = 'shopify'
              AND transaction_type  = 'charge'
              AND period_yyyymm     = %s
              AND affects_balance   = true
              AND is_cancelled      = false
              AND order_name IS NOT NULL AND order_name <> ''
            GROUP BY order_name
            """,
            (company_code, period_yyyymm),
        ).fetchall()

        # -- Payment: PayPal sales for this period --
        paypal_pay_rows = conn.execute(
            """
            SELECT
                order_name,
                MAX(order_id)          AS order_id,
                SUM(gross_amount)      AS importe_pago,
                bool_or(is_chargeback) AS tiene_chargeback
            FROM invoices.payment_order_transactions
            WHERE company_code      = %s
              AND platform          = 'paypal'
              AND transaction_type  = ANY(%s)
              AND period_yyyymm     = %s
              AND affects_balance   = true
              AND is_cancelled      = false
              AND order_name IS NOT NULL AND order_name <> ''
            GROUP BY order_name
            """,
            (company_code, list(PAYPAL_SALE_TYPES), period_yyyymm),
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

    # Build lookup maps
    pedido_id_map: dict[str, str] = {r["order_name"]: r["pedido_id"] for r in pedido_rows}

    # -- Accounting dict, split by channel --
    shopify_acct: dict[str, dict] = {}
    paypal_acct: dict[str, dict] = {}
    for r in acct_rows:
        gw = _parse_gateways(r.get("gateways_raw"))
        if "shopify_payments" in gw:
            shopify_acct[r["order_name"]] = dict(r)
        if "paypal" in gw:
            paypal_acct[r["order_name"]] = dict(r)

    # -- Payment dicts --
    shopify_pay: dict[str, dict] = {r["order_name"]: dict(r) for r in shopify_pay_rows}
    paypal_pay: dict[str, dict] = {r["order_name"]: dict(r) for r in paypal_pay_rows}

    def _make_row(a: dict | None, p: dict | None, name: str) -> ReconciliationRow:
        accounting_amount = _dec(a["importe_contab"]) if a else None
        payment_amount    = _dec(p["importe_pago"]) if p else None
        if payment_amount is not None:
            payment_amount = payment_amount.quantize(Decimal("0.01"))
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
            is_gift_card=bool(a.get("gift_card")) if a else False,
            is_chargeback=bool(p.get("tiene_chargeback")) if p else False,
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

    return ReconciliationReport(
        period_yyyymm=period_yyyymm,
        company_code=company_code,
        shopify=_reconcile(shopify_acct, shopify_pay),
        paypal=_reconcile(paypal_acct, paypal_pay),
    )
