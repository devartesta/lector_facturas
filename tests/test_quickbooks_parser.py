from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.quickbooks import parse_quickbooks_text


QUICKBOOKS_SAMPLE = dedent(
    """
    Intuit Inc.
    Bill to
    ivan zamarbide
    Artesta
    10 West Rd|Ste 100
    Newtown, PA 18940-4301
    US
    Payment details
    Invoice
    Invoice number: 10001417716363 Total: $1.06
    Date: Aug 11, 2025
    Item Qty Unit price Amount
    QuickBooks Online Plus 1 $115.00 $115.00 $114.00 discount, expires Nov 11, 2025 -$114.00
    Price after discount / subtotal: $1.00
    Sales tax - Standard: $0.06
    Total invoice: $1.06
    Tax reporting information
    Period for monthly fees: Aug 11, 2025 - Sep 11, 2025
    Total without tax: $1.00
    Total tax: $0.06
    """
)


class QuickBooksParserTests(unittest.TestCase):
    def test_parse_quickbooks_invoice(self) -> None:
        parsed = parse_quickbooks_text(QUICKBOOKS_SAMPLE, original_filename="2025-08-11_10001417716363.pdf")
        self.assertEqual(parsed.supplier_code, "QUICKBOOKS")
        self.assertEqual(parsed.invoice_number, "10001417716363")
        self.assertEqual(parsed.invoice_date.isoformat(), "2025-08-11")
        self.assertEqual(parsed.period_yyyymm, "202508")
        self.assertEqual(parsed.currency_code, "USD")
        self.assertEqual(parsed.net_amount, Decimal("1.00"))
        self.assertEqual(parsed.vat_amount, Decimal("0.06"))
        self.assertEqual(parsed.gross_amount, Decimal("1.06"))


if __name__ == "__main__":
    unittest.main()
