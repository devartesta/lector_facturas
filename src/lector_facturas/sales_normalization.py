from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Mapping


CENT = Decimal("0.01")


class SalesCurrencyNormalizationError(RuntimeError):
    """Raised when an SL foreign-currency row cannot be converted to EUR."""


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


def _money_ratio(
    raw: Mapping[str, Any],
    *,
    primary_key: str,
    fallback_key: str,
) -> Decimal | None:
    for key in (primary_key, fallback_key):
        presentment = _money_set(raw, key, "presentment_money")
        shop = _money_set(raw, key, "shop_money")
        if presentment and shop:
            return shop / presentment
    return None


def _convert_period_movement_to_shop_currency(
    row: Mapping[str, Any],
    raw: Mapping[str, Any],
) -> tuple[Decimal, Decimal, Decimal, str, Decimal | None]:
    """Convert one monthly movement using the order's original Shopify FX.

    The monthly tables contain period movements, not necessarily the order's
    current balance. Using ``current_total_*`` directly therefore breaks
    refunds posted in a later month. The original order ratio converts both
    positive sales and negative refund movements consistently.
    """

    source_currency = str(row.get("payment_currency") or "EUR").upper()
    gross = _decimal(row.get("shown_gross_presentment"))
    tax = _decimal(row.get("shown_tax_presentment"))
    net = _decimal(row.get("shown_net_presentment"))
    if source_currency == "EUR":
        return gross, tax, net, source_currency, None
    if gross == 0 and tax == 0 and net == 0:
        return gross, tax, net, source_currency, None

    shop_currency = str(raw.get("currency") or "").upper()
    if shop_currency and shop_currency != "EUR":
        raise SalesCurrencyNormalizationError(
            f"SL order {row.get('order_name') or '?'} has shop currency "
            f"{shop_currency}; expected EUR"
        )

    gross_ratio = _money_ratio(
        raw,
        primary_key="total_price_set",
        fallback_key="current_total_price_set",
    )
    if gross_ratio is None:
        raise SalesCurrencyNormalizationError(
            f"SL order {row.get('order_name') or '?'} in {source_currency} "
            "has no Shopify shop/presentment FX ratio"
        )

    converted_gross = _round_money(gross * gross_ratio)
    tax_ratio = _money_ratio(
        raw,
        primary_key="total_tax_set",
        fallback_key="current_total_tax_set",
    )
    converted_tax = _round_money(tax * (tax_ratio or gross_ratio))
    converted_net = _round_money(converted_gross - converted_tax)
    return converted_gross, converted_tax, converted_net, source_currency, gross_ratio


def _refund_line_items_shop_amount(
    raw: Mapping[str, Any],
    period: str,
    presentment_refund: Decimal,
) -> Decimal | None:
    shop_total = Decimal("0")
    presentment_total = Decimal("0")
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
            presentment_money = subtotal_set.get("presentment_money") or {}
            if (
                not isinstance(shop_money, Mapping)
                or not isinstance(presentment_money, Mapping)
                or shop_money.get("amount") in (None, "")
                or presentment_money.get("amount") in (None, "")
            ):
                continue
            shop_total += abs(_decimal(shop_money.get("amount")))
            presentment_total += abs(_decimal(presentment_money.get("amount")))
            found = True
    if found and abs(_round_money(presentment_total) - presentment_refund) <= CENT:
        return _round_money(shop_total)
    return None


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
        refund,
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
    if presentment_gross and shop_gross and refund > shop_gross:
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
    gross, tax, net, source_currency, gross_ratio = _convert_period_movement_to_shop_currency(
        normalized,
        raw,
    )
    if gross_ratio is not None:
        if normalized.get("_gross_presentment_original") is not None:
            normalized["_gross_presentment_original"] = _round_money(
                _decimal(normalized["_gross_presentment_original"]) * gross_ratio
            )
        if normalized.get("_same_month_refund_amount_presentment") is not None:
            normalized["_same_month_refund_amount_presentment"] = _round_money(
                _decimal(normalized["_same_month_refund_amount_presentment"]) * gross_ratio
            )

    same_month_refund = str(normalized.get("_same_month_refund_yyyymm") or "") == period
    if same_month_refund and source_currency == "EUR":
        refund_shop = _foreign_refund_in_shop_currency(normalized, raw)
        if refund_shop is not None:
            original_gross = normalized.get("_gross_presentment_original")
            if original_gross is None:
                gross = -refund_shop
            else:
                gross = _round_money(_decimal(original_gross) - refund_shop)

        # Once a refund changes the gross, tax must follow the remaining
        # tax-inclusive consideration rather than the pre-refund Shopify tax.
        presentment_currency = str(raw.get("presentment_currency") or raw.get("currency") or "").upper()
        shop_currency = str(raw.get("currency") or "").upper()
        if standard_rate and (refund_shop is not None or presentment_currency == shop_currency):
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
    normalized["_source_payment_currency"] = source_currency
    normalized["descuadre"] = gross - tax - net
    return normalized
