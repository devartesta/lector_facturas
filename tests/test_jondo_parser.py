from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.jondo import _parse_jondo_text, parse_jondo_pdf


class JondoParserSmokeTests(unittest.TestCase):
    def test_us_invoice_counts_tax_in_expense_subtotal(self) -> None:
        parsed = _parse_jondo_text(
            dedent(
                """
                Order Invoice
                Order: 4376035
                Date: 2026-03-01
                PO Number: AS-100189
                ARTESTA INC
                Product Status Quantity Price
                Matte Canvas Print Completed 1 USD $ 93.04
                Subtotal USD $ 93.04
                Shipping USD $ 5.00
                Shipping Tax USD $ 0.39
                US-CA-92646 USD $ 7.60
                Payment method Credit card (Stripe)
                Total USD $ 105.64
                """
            ),
            original_filename="AS-100189.pdf",
        )
        self.assertEqual(parsed.invoice_number, "AS-100189")
        self.assertEqual(parsed.net_amount, Decimal("105.64"))
        self.assertEqual(parsed.vat_amount, Decimal("7.60"))
        self.assertEqual(parsed.gross_amount, Decimal("105.64"))

    def test_full_order_po_filename_is_used_as_invoice_number(self) -> None:
        parsed = _parse_jondo_text(
            dedent(
                """
                Order Invoice
                Order: 4425413
                Date: 2026-04-20
                PO Number: AS-104525
                Product Status Quantity Price
                Matte Canvas Print Completed 1 USD $ 56.22
                Subtotal USD $ 56.22
                Shipping USD $ 5.00
                US-CA-92646 USD $ 3.24
                Total USD $ 64.46
                """
            ),
            original_filename="4425413-AS-104525.pdf",
        )
        self.assertEqual(parsed.invoice_number, "4425413-AS-104525")

    def test_real_file(self) -> None:
        parsed = parse_jondo_pdf(
            __import__("pathlib").Path(
                r"C:\Users\ADRISE~1\OneDrive - Artesta\ARTESTA - 6. Finances\Artesta Stores (UK) Ltd\2026\1Q\202601_UK\Operating Expenses\202601_jondogo\AS-94763.pdf"
            )
        )
        self.assertEqual(parsed.invoice_number, "AS-94763")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-01-04")
        self.assertEqual(parsed.net_amount, Decimal("44.06"))
        self.assertEqual(parsed.vat_amount, Decimal("8.81"))
        self.assertEqual(parsed.gross_amount, Decimal("52.87"))

    def test_uk_invoice_keeps_tax_outside_net_amount(self) -> None:
        parsed = _parse_jondo_text(
            dedent(
                """
                Order Invoice
                Order: 4500000
                Date: 2026-05-01
                PO Number: AS-110000
                ARTESTA STORES (UK) LTD
                Product Status Quantity Price
                Matte Canvas Print Completed 1 USD $ 40.00
                Subtotal USD $ 40.00
                Shipping USD $ 5.00
                GB VAT USD $ 9.00
                Total USD $ 54.00
                """
            ),
            original_filename="AS-110000.pdf",
        )
        self.assertEqual(parsed.billed_company_name, "ARTESTA STORES (UK) LTD")
        self.assertEqual(parsed.net_amount, Decimal("45.00"))
        self.assertEqual(parsed.vat_amount, Decimal("9.00"))
        self.assertEqual(parsed.gross_amount, Decimal("54.00"))


if __name__ == "__main__":
    unittest.main()
