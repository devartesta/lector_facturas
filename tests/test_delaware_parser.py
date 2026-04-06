from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.delaware import parse_delaware_text


DELAWARE_SAMPLE = dedent(
    """
    Delaware Corporate Headquarters LLC
    Artesta Inc.
    Invoice
    Invoice Number DZWHFH4
    Invoice Date April 01, 2026
    Due Date April 06, 2026
    Total $107.00
    Items Price Discount Amount
    Pennsylvania Renewal Filing: Annual Report $107.00 - $107.00
    Total $107.00
    Paid: $107.00
    """
)


class DelawareParserTests(unittest.TestCase):
    def test_parse_invoice(self) -> None:
        parsed = parse_delaware_text(DELAWARE_SAMPLE, original_filename="Invoice - Artesta Inc. 2026-04-05.pdf")
        self.assertEqual(parsed.supplier_code, "DELAWARE")
        self.assertEqual(parsed.invoice_number, "DZWHFH4")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-04-01")
        self.assertEqual(parsed.period_yyyymm, "202604")
        self.assertEqual(parsed.currency_code, "USD")
        self.assertEqual(parsed.net_amount, Decimal("107.00"))
        self.assertEqual(parsed.vat_amount, Decimal("0"))
        self.assertEqual(parsed.gross_amount, Decimal("107.00"))


if __name__ == "__main__":
    unittest.main()
