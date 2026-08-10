from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Mapping


CENT = Decimal("0.01")


def _decimal(value: Any) -> Decimal:
    if value in (None, ""):
        return Decimal("0")
    return Decimal(str(value))


def _money_set(raw: Mapping[str, Any], key: str, money_kind: str) -> Decimal:
    value = raw.get(key) or {}
    money = value.get(money_kind) or {}
    return _decimal(money.get("amount"))


def _round_money(value: Decimal) -> Decimal:
    return value.quantize(CENT, rounding=ROUND_HALF_UP)


def _refund_line_items_shop_amount(raw: Mapping[str, Any], period: str) -> Decimal | None:
    total = Decimal("0")
    found = False
    for refund in raw.get("refunds") or []:
        if not isinstance(refund, Mapping):
            continue
        refund_date = str(refund.get("processed_at") or refund.get("created_at") or "")
        if period and refund_date[:7].replace("-", "") != period:
            continue
        for line in refund.get("refund_line_items") or []:
            if not isinstance(line, Mapping):
                continue
            subtotal_set = line.get("subtotal_set") or {}
            if not isinstance(subtotal_set, Mapping):
                continue
            shop_money = subtotal_set.get("shop_money") or {}
            if not isinstance(shop_money, Mapping) or shop_money.get("amount") in (None, ""):
                continue
            total += abs(_decimal(shop_money.get("amount")))
            found = True
    return _round_money(total) if found else None


def _foreign_refund_in_shop_currency(row: Mapping[str, Any], raw: Mapping[str, Any]) -> Decimal | None:
    refund = abs(_decimal(row.get("_same_month_refund_amount_presentment")))
    if refund == 0:
        return None

    shop_currency = str(raw.get("currency") or "").upper()
    presentment_currency = str(raw.get("presentment_currency") or shop_currency).upper()
    if not shop_currency or presentment_currency == shop_currency:
        return None

    line_items_shop = _refund_line_items_shop_amount(
        raw,
        str(row.get("order_month_yyyymm") or ""),
    )
    if line_items_shop is not None:
        return line_items_shop

    presentment_tax = _money_set(raw, "total_tax_set", "presentment_money")
    shop_tax = _money_set(raw, "total_tax_set", "shop_money")
    if presentment_tax and abs(refund - presentment_tax) <= CENT:
        # Shopify can issue a manual refund for exactly the foreign-currency VAT.
        return shop_tax

    presentment_gross = _money_set(raw, "total_price_set", "presentment_money")
    shop_gross = _money_set(raw, "total_price_set", "shop_money")
    if presentment_gross and shop_gross:
        return _round_money(refund * shop_gross / presentment_gross)
    return None


def normalize_sl_sales_detail_row(row: Mapping[str, Any]) -> dict[str, Any]:
    """Return one SL sales row normalized to accounting EUR.

    Two source anomalies are handled here:
    * refund transactions can be in the customer's presentment currency even
      though the SL books are in EUR;
    * 40 Polish July 2026 orders were created while Shopify VAT was
      misconfigured. Their customer-facing gross is correct and includes 23%
      VAT, so accounting tax and net must be derived from that gross.
    """

    normalized = dict(row)
    raw = normalized.pop("_raw_json", None) or {}
    if not isinstance(raw, Mapping):
        raw = {}

    period = str(normalized.get("order_month_yyyymm") or "")
    country = str(normalized.get("shipping_country_code") or "XX").upper()
    standard_rate = _decimal(normalized.get("standard_rate"))
    gross = _decimal(normalized.get("shown_gross_presentment"))
    tax = _decimal(normalized.get("shown_tax_presentment"))
    net = _decimal(normalized.get("shown_net_presentment"))

    same_month_refund = str(normalized.get("_same_month_refund_yyyymm") or "") == period
    if same_month_refund:
        refund_shop = _foreign_refund_in_shop_currency(normalized, raw)
        if refund_shop is not None:
            original_gross = normalized.get("_gross_presentment_original")
            if original_gross is None:
                gross = -refund_shop
            else:
                gross = _round_money(_decimal(original_gross) - refund_shop)

        # Once a refund changes the gross, tax must follow the remaining
        # tax-inclusive consideration rather than the pre-refund Shopify tax.
        if standard_rate:
            tax = _round_money(gross - gross / (Decimal("1") + standard_rate))
            net = _round_money(gross - tax)

    original_shop_tax = _money_set(raw, "total_tax_set", "shop_money")
    polish_vat_incident = period == "202607" and country == "PL" and original_shop_tax == 0
    if polish_vat_incident:
        rate = Decimal("0.23")
        # Keep full precision in the accounting source so the country total is
        # gross / 1.23. Excel still displays each order to two decimals.
        tax = gross - gross / (Decimal("1") + rate)
        net = gross - tax
        normalized["tax_rate"] = Decimal("0")

    normalized["shown_gross_presentment"] = gross
    normalized["shown_tax_presentment"] = tax
    normalized["shown_net_presentment"] = net
    normalized["payment_currency"] = "EUR"
    normalized["descuadre"] = gross - tax - net
    return normalized
