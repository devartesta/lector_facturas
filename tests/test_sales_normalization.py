from decimal import Decimal

from lector_facturas.sales_normalization import normalize_sl_sales_detail_row


def test_full_swedish_refund_uses_shop_money_eur() -> None:
    row = {
        "order_month_yyyymm": "202607",
        "shipping_country_code": "SE",
        "standard_rate": Decimal("0.25"),
        "tax_rate": Decimal("0"),
        "payment_currency": "EUR",
        "shown_gross_presentment": Decimal("-3399.40"),
        "shown_tax_presentment": Decimal("0"),
        "shown_net_presentment": Decimal("-3399.40"),
        "_same_month_refund_yyyymm": "202607",
        "_same_month_refund_amount_presentment": Decimal("-3736.00"),
        "_gross_presentment_original": Decimal("336.60"),
        "_raw_json": {
            "currency": "EUR",
            "presentment_currency": "SEK",
            "total_price_set": {
                "shop_money": {"amount": "336.60"},
                "presentment_money": {"amount": "3736.00"},
            },
            "total_tax_set": {
                "shop_money": {"amount": "67.32"},
                "presentment_money": {"amount": "747.20"},
            },
        },
    }

    result = normalize_sl_sales_detail_row(row)

    assert result["shown_gross_presentment"] == Decimal("0.00")
    assert result["shown_tax_presentment"] == Decimal("0.00")
    assert result["shown_net_presentment"] == Decimal("0.00")


def test_foreign_refund_equal_to_presentment_tax_uses_shop_tax() -> None:
    row = {
        "order_month_yyyymm": "202607",
        "shipping_country_code": "PL",
        "standard_rate": Decimal("0.23"),
        "tax_rate": Decimal("0.23"),
        "payment_currency": "EUR",
        "shown_gross_presentment": Decimal("65.56"),
        "shown_tax_presentment": Decimal("62.96"),
        "shown_net_presentment": Decimal("2.60"),
        "_same_month_refund_yyyymm": "202607",
        "_same_month_refund_amount_presentment": Decimal("-271.14"),
        "_gross_presentment_original": Decimal("336.70"),
        "_raw_json": {
            "currency": "EUR",
            "presentment_currency": "PLN",
            "total_price_set": {
                "shop_money": {"amount": "336.70"},
                "presentment_money": {"amount": "1450.00"},
            },
            "total_tax_set": {
                "shop_money": {"amount": "62.96"},
                "presentment_money": {"amount": "271.14"},
            },
        },
    }

    result = normalize_sl_sales_detail_row(row)

    assert result["shown_gross_presentment"] == Decimal("273.74")
    assert result["shown_tax_presentment"] == Decimal("51.19")
    assert result["shown_net_presentment"] == Decimal("222.55")


def test_polish_zero_vat_incident_derives_tax_from_tax_inclusive_gross() -> None:
    row = {
        "order_month_yyyymm": "202607",
        "shipping_country_code": "PL",
        "standard_rate": Decimal("0.23"),
        "tax_rate": Decimal("0.23"),
        "payment_currency": "EUR",
        "shown_gross_presentment": Decimal("192.93"),
        "shown_tax_presentment": Decimal("36.08"),
        "shown_net_presentment": Decimal("156.85"),
        "_raw_json": {
            "currency": "EUR",
            "presentment_currency": "PLN",
            "total_tax_set": {
                "shop_money": {"amount": "0.00"},
                "presentment_money": {"amount": "0.00"},
            },
        },
    }

    result = normalize_sl_sales_detail_row(row)

    assert result["shown_gross_presentment"] == Decimal("192.93")
    assert result["shown_tax_presentment"] == Decimal("192.93") - Decimal("192.93") / Decimal("1.23")
    assert result["shown_net_presentment"] == Decimal("192.93") / Decimal("1.23")
    assert result["tax_rate"] == Decimal("0")


def test_eur_refund_recalculates_tax_from_remaining_gross() -> None:
    row = {
        "order_month_yyyymm": "202607",
        "shipping_country_code": "DE",
        "standard_rate": Decimal("0.19"),
        "tax_rate": Decimal("0.19"),
        "payment_currency": "EUR",
        "shown_gross_presentment": Decimal("26.20"),
        "shown_tax_presentment": Decimal("6.05"),
        "shown_net_presentment": Decimal("20.15"),
        "_same_month_refund_yyyymm": "202607",
        "_same_month_refund_amount_presentment": Decimal("-11.70"),
        "_gross_presentment_original": Decimal("37.90"),
        "_raw_json": {"currency": "EUR", "presentment_currency": "EUR"},
    }

    result = normalize_sl_sales_detail_row(row)

    assert result["shown_gross_presentment"] == Decimal("26.20")
    assert result["shown_tax_presentment"] == Decimal("4.18")
    assert result["shown_net_presentment"] == Decimal("22.02")
