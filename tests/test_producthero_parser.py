from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.producthero import parse_producthero_text


SAMPLE = dedent(
    """
    INVOICE
    Invoice #—205588
    Invoice Date—Jan 28, 2026
    Invoice Amount—134.00 € (EUR)
    Billing Period—Jan 28 to Feb 27, 2026
    Total 134.00 €
    """
)


class ProductheroParserTests(unittest.TestCase):
    def test_parse(self) -> None:
        parsed = parse_producthero_text(SAMPLE, original_filename="producthero.pdf")
        self.assertEqual(parsed.invoice_number, "205588")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-01-28")
        self.assertEqual(parsed.billing_period_start.isoformat(), "2026-01-28")
        self.assertEqual(parsed.billing_period_end.isoformat(), "2026-02-27")
        self.assertEqual(parsed.net_amount, Decimal("134.00"))


if __name__ == "__main__":
    unittest.main()
