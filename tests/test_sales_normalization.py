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
            "refunds": [
                {
                    "processed_at": "2026-07-31T11:51:12+02:00",
                    "refund_line_items": [
                        {
                            "subtotal_set": {
                                "shop_money": {"amount": "336.60"},
                                "presentment_money": {"amount": "3736.00"},
                            }
                        }
                    ],
                }
            ],
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


def test_foreign_refund_prefers_refund_line_shop_money_over_ratio() -> None:
    row = {
        "order_month_yyyymm": "202607",
        "shipping_country_code": "SE",
        "standard_rate": Decimal("0.25"),
        "tax_rate": Decimal("0.25"),
        "shown_gross_presentment": Decimal("50.00"),
        "shown_tax_presentment": Decimal("10.00"),
        "shown_net_presentment": Decimal("40.00"),
        "_same_month_refund_yyyymm": "202607",
        "_same_month_refund_amount_presentment": Decimal("-50.00"),
        "_gross_presentment_original": Decimal("100.00"),
        "_raw_json": {
            "currency": "EUR",
            "presentment_currency": "SEK",
            "refunds": [
                {
                    "processed_at": "2026-07-31T11:51:12+02:00",
                    "refund_line_items": [
                        {
                            "subtotal_set": {
                                "shop_money": {"amount": "10.00"},
                                "presentment_money": {"amount": "50.00"},
                            }
                        }
                    ],
                }
            ],
            "total_price_set": {
                "shop_money": {"amount": "100.00"},
                "presentment_money": {"amount": "400.00"},
            },
            "total_tax_set": {
                "shop_money": {"amount": "20.00"},
                "presentment_money": {"amount": "80.00"},
            },
        },
    }

    result = normalize_sl_sales_detail_row(row)

    assert result["shown_gross_presentment"] == Decimal("90.00")
    assert result["shown_tax_presentment"] == Decimal("18.00")
    assert result["shown_net_presentment"] == Decimal("72.00")


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


def test_foreign_sale_uses_original_shop_money_ratio() -> None:
    row = {
        "order_name": "AS-113402",
        "order_month_yyyymm": "202608",
        "shipping_country_code": "CZ",
        "standard_rate": Decimal("0.21"),
        "tax_rate": Decimal("0.21"),
        "payment_currency": "CZK",
        "shown_gross_presentment": Decimal("1483.00"),
        "shown_tax_presentment": Decimal("257.43"),
        "shown_net_presentment": Decimal("1225.57"),
        "_raw_json": {
            "currency": "EUR",
            "presentment_currency": "CZK",
            "total_price_set": {
                "shop_money": {"amount": "61.18"},
                "presentment_money": {"amount": "1483.00"},
            },
            "total_tax_set": {
                "shop_money": {"amount": "10.62"},
                "presentment_money": {"amount": "257.43"},
            },
        },
    }

    result = normalize_sl_sales_detail_row(row)

    assert result["shown_gross_presentment"] == Decimal("61.18")
    assert result["shown_tax_presentment"] == Decimal("10.62")
    assert result["shown_net_presentment"] == Decimal("50.56")
    assert result["payment_currency"] == "EUR"
    assert result["_source_payment_currency"] == "CZK"


def test_later_month_foreign_refund_converts_the_monthly_movement() -> None:
    row = {
        "order_name": "AS-111185",
        "order_month_yyyymm": "202608",
        "shipping_country_code": "DK",
        "standard_rate": Decimal("0.20"),
        "tax_rate": Decimal("0.20"),
        "payment_currency": "DKK",
        "shown_gross_presentment": Decimal("-458.00"),
        "shown_tax_presentment": Decimal("-91.60"),
        "shown_net_presentment": Decimal("-366.40"),
        "_same_month_refund_yyyymm": "202608",
        "_same_month_refund_amount_presentment": None,
        "_gross_presentment_original": Decimal("1984.00"),
        "_raw_json": {
            "currency": "EUR",
            "presentment_currency": "DKK",
            "total_price_set": {
                "shop_money": {"amount": "265.42"},
                "presentment_money": {"amount": "1984.00"},
            },
            "total_tax_set": {
                "shop_money": {"amount": "53.08"},
                "presentment_money": {"amount": "396.74"},
            },
            "current_total_price_set": {
                "shop_money": {"amount": "204.15"},
                "presentment_money": {"amount": "1526.00"},
            },
        },
    }

    result = normalize_sl_sales_detail_row(row)

    assert result["shown_gross_presentment"] == Decimal("-61.27")
    assert result["shown_tax_presentment"] == Decimal("-12.26")
    assert result["shown_net_presentment"] == Decimal("-49.01")
    assert result["_gross_presentment_original"] == Decimal("265.42")

    # Persisted rows are EUR on subsequent runs and must remain unchanged.
    second = normalize_sl_sales_detail_row({**result, "_raw_json": row["_raw_json"]})
    assert second["shown_gross_presentment"] == result["shown_gross_presentment"]
    assert second["shown_tax_presentment"] == result["shown_tax_presentment"]
    assert second["shown_net_presentment"] == result["shown_net_presentment"]


def test_same_month_foreign_refund_is_idempotent_from_current_shop_balance() -> None:
    row = {
        "order_name": "AS-113441",
        "order_month_yyyymm": "202608",
        "shipping_country_code": "DK",
        "standard_rate": Decimal("0.20"),
        "tax_rate": Decimal("0.20"),
        "payment_currency": "DKK",
        "shown_gross_presentment": Decimal("1144.00"),
        "shown_tax_presentment": Decimal("228.78"),
        "shown_net_presentment": Decimal("915.22"),
        "_same_month_refund_yyyymm": "202608",
        "_same_month_refund_amount_presentment": Decimal("-838.00"),
        "_gross_presentment_original": Decimal("1982.00"),
        "_raw_json": {
            "created_at": "2026-08-01T18:10:42+02:00",
            "currency": "EUR",
            "presentment_currency": "DKK",
            "total_price_set": {
                "shop_money": {"amount": "265.26"},
                "presentment_money": {"amount": "1982.00"},
            },
            "current_total_price_set": {
                "shop_money": {"amount": "153.11"},
                "presentment_money": {"amount": "1144.00"},
            },
            "total_tax_set": {
                "shop_money": {"amount": "53.05"},
                "presentment_money": {"amount": "396.38"},
            },
        },
    }

    result = normalize_sl_sales_detail_row(row)
    assert result["shown_gross_presentment"] == Decimal("153.11")
    assert result["shown_tax_presentment"] == Decimal("30.62")
    assert result["shown_net_presentment"] == Decimal("122.49")

    second = normalize_sl_sales_detail_row({**result, "_raw_json": row["_raw_json"]})
    assert second["shown_gross_presentment"] == result["shown_gross_presentment"]
    assert second["shown_tax_presentment"] == result["shown_tax_presentment"]
    assert second["shown_net_presentment"] == result["shown_net_presentment"]


def test_manual_foreign_refund_uses_shop_money_adjustment() -> None:
    row = {
        "order_name": "AS-113594",
        "order_month_yyyymm": "202608",
        "shipping_country_code": "PL",
        "standard_rate": Decimal("0.23"),
        "tax_rate": Decimal("0.23"),
        "payment_currency": "PLN",
        "shown_gross_presentment": Decimal("677.60"),
        "shown_tax_presentment": Decimal("126.72"),
        "shown_net_presentment": Decimal("550.88"),
        "_same_month_refund_yyyymm": "202608",
        "_same_month_refund_amount_presentment": Decimal("-290.40"),
        "_gross_presentment_original": Decimal("968.00"),
        "_raw_json": {
            "created_at": "2026-08-02T21:00:55+02:00",
            "currency": "EUR",
            "presentment_currency": "PLN",
            "total_price_set": {
                "shop_money": {"amount": "224.60"},
                "presentment_money": {"amount": "968.00"},
            },
            "current_total_price_set": {
                "shop_money": {"amount": "224.60"},
                "presentment_money": {"amount": "968.00"},
            },
            "total_tax_set": {
                "shop_money": {"amount": "42.00"},
                "presentment_money": {"amount": "181.02"},
            },
            "refunds": [
                {
                    "processed_at": "2026-08-06T16:28:18+02:00",
                    "order_adjustments": [
                        {
                            "kind": "refund_discrepancy",
                            "amount_set": {"shop_money": {"amount": "-67.56"}},
                        },
                        {
                            "kind": "refund_discrepancy",
                            "amount_set": {"shop_money": {"amount": "67.56"}},
                        },
                    ],
                }
            ],
        },
    }

    result = normalize_sl_sales_detail_row(row)
    assert result["shown_gross_presentment"] == Decimal("157.04")
    assert result["shown_tax_presentment"] == Decimal("29.37")
    assert result["shown_net_presentment"] == Decimal("127.67")
