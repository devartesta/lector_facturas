from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.youraccountstaxes import parse_youraccountstaxes_text


YAT_SAMPLE = dedent(
    """
    TAX INVOICE
    ARTESTA STORES (UK) LTD
    Invoice Date
    11 Dec 2025
    Invoice Number
    INV-0639
    Description Quantity Unit Price VAT Amount GBP
    Year end accounts , tax computation and filing for the year
    ending Dec 2025
    1.00 850.00 20% 850.00
    Subtotal 850.00
    TOTAL VAT 20% 170.00
    TOTAL GBP 1,020.00
    """
)


class YourAccountsAndTaxesParserTests(unittest.TestCase):
    def test_parse_invoice(self) -> None:
        parsed = parse_youraccountstaxes_text(YAT_SAMPLE, original_filename="Invoice INV-0639.pdf")
        self.assertEqual(parsed.supplier_code, "YOURACCOUNTSTAXES")
        self.assertEqual(parsed.invoice_number, "INV-0639")
        self.assertEqual(parsed.invoice_date.isoformat(), "2025-12-11")
        self.assertEqual(parsed.billing_period_start.isoformat(), "2025-01-01")
        self.assertEqual(parsed.billing_period_end.isoformat(), "2025-12-31")
        self.assertEqual(parsed.period_yyyymm, "202512")
        self.assertEqual(parsed.currency_code, "GBP")
        self.assertEqual(parsed.net_amount, Decimal("850.00"))
        self.assertEqual(parsed.vat_amount, Decimal("170.00"))
        self.assertEqual(parsed.gross_amount, Decimal("1020.00"))


if __name__ == "__main__":
    unittest.main()
