from lector_facturas.pyg_sl_workbook import DEFAULT_SHOPIFY_MARKETS, _normalize_shopify_market


def test_pyg_keeps_requested_shopify_country_markets():
    for country_code in ("PL", "SE", "DK", "CZ"):
        assert country_code in DEFAULT_SHOPIFY_MARKETS[:-1]
        assert _normalize_shopify_market(country_code) == country_code


def test_pyg_still_groups_unknown_shopify_markets_as_rest_of_eu():
    assert _normalize_shopify_market("IE") == "XX"
