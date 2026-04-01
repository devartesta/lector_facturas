from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.gorgias import parse_gorgias_text


SAMPLE = dedent(
    """
    INVOICE
    Invoice #—INC-02-2026-50341
    Invoice Date—Feb 14, 2026
    Billing Period—Feb 14 to Mar 14, 2026
    Invoice Amount—$120.00 (USD)
    Total $120.00
    """
)


class GorgiasParserTests(unittest.TestCase):
    def test_parse(self) -> None:
        parsed = parse_gorgias_text(SAMPLE, original_filename="gorgias.pdf")
        self.assertEqual(parsed.invoice_number, "INC-02-2026-50341")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-02-14")
        self.assertEqual(parsed.billing_period_start.isoformat(), "2026-02-14")
        self.assertEqual(parsed.billing_period_end.isoformat(), "2026-03-14")
        self.assertEqual(parsed.net_amount, Decimal("120.00"))
        self.assertEqual(parsed.gross_amount, Decimal("120.00"))


if __name__ == "__main__":
    unittest.main()
