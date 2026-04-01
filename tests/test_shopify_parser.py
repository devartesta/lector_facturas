from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.shopify import parse_shopify_text


SAMPLE = dedent(
    """
    TOTAL DUE
    $2,674.40 USD
    Subtotal $2,674.40 USD
    VAT 0.0%* $0.00 USD
    Bill #479105361
    Paid on Jan 25, 2026
    Matrixify $50.00 USDMatrixify: Big: 2026-01-21 - 2026-02-20
    TrackingMore $55.00 USDTrackingMore app monthly billing - $ 55: 2025-12-26 - 2026-01-25
    """
)


class ShopifyParserTests(unittest.TestCase):
    def test_parse(self) -> None:
        parsed = parse_shopify_text(SAMPLE, original_filename="Artesta_479105361.pdf")
        self.assertEqual(parsed.invoice_number, "479105361")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-01-25")
        self.assertEqual(parsed.period_yyyymm, "202601")
        self.assertEqual(parsed.net_amount, Decimal("2674.40"))
        self.assertEqual(parsed.vat_amount, Decimal("0"))
        self.assertEqual(parsed.gross_amount, Decimal("2674.40"))
        self.assertEqual(parsed.billing_period_start.isoformat(), "2025-12-26")
        self.assertEqual(parsed.billing_period_end.isoformat(), "2026-02-20")


if __name__ == "__main__":
    unittest.main()
