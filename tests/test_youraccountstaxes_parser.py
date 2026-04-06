from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.invoice_ingestion import detect_parser_rule
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

YAT_ZERO_RATED_SAMPLE = dedent(
    """
    PAYMENT ADVICE
    Customer Hannun
    Invoice Number INV-0683
    TAX INVOICE
    Hannun
    Invoice Date
    30 Mar 2026
    Invoice Number
    INV-0683
    Description Quantity Unit Price VAT Amount GBP
    Filing of VAT - Q2 , Q3 and Q4 of 2025
    3.00 23.00 Zero Rated EC Services 69.00
    Subtotal 69.00
    TOTAL ZERO RATED EC SERVICES 0.00
    TOTAL GBP 69.00
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

    def test_parse_zero_rated_invoice_uses_invoice_month(self) -> None:
        parsed = parse_youraccountstaxes_text(YAT_ZERO_RATED_SAMPLE, original_filename="Invoice INV-0683.pdf")
        self.assertEqual(parsed.invoice_number, "INV-0683")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-03-30")
        self.assertEqual(parsed.period_yyyymm, "202603")
        self.assertEqual(parsed.currency_code, "GBP")
        self.assertEqual(parsed.net_amount, Decimal("69.00"))
        self.assertEqual(parsed.vat_amount, Decimal("0.00"))
        self.assertEqual(parsed.vat_percent, Decimal("0"))
        self.assertEqual(parsed.gross_amount, Decimal("69.00"))

    def test_detection_prefers_youraccountstaxes_over_hannun(self) -> None:
        rule = detect_parser_rule(
            filename="Invoice INV-0683.pdf",
            sender_email="20260330T141314Z_christina-youraccountsntaxes-co-uk_Invoice INV-0683.pdf",
            subject="Invoice INV-0683.pdf",
            pdf_text=YAT_ZERO_RATED_SAMPLE,
        )
        self.assertIsNotNone(rule)
        self.assertEqual(rule.supplier_code, "YOURACCOUNTSTAXES")


if __name__ == "__main__":
    unittest.main()
