from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.openai import parse_openai_text


IRELAND_SAMPLE = dedent(
    """
    Invoice
    Invoice number 7BSDV5AM-0001
    Date of issue January 20, 2026
    OpenAI Ireland Limited
    Bill to
    Artesta Store SL
    ChatGPT Plus Subscription (per seat)
    Jan 20 – Feb 20, 2026
    Total excluding tax €19.01
    VAT - Spain 21% on €19.01 €3.99
    Amount due €23.00
    """
)

RECEIPT_SAMPLE = dedent(
    """
    Receipt
    Invoice number BZHJNTUB-0004
    Receipt number 2699-8822-5500
    Date paid February 7, 2026
    OpenAI OpCo, LLC
    Bill to
    Artesta Store SL
    Total excluding tax $10.13
    VAT - Spain 21% on $10.13 $2.13
    Amount paid $12.26
    """
)


class OpenAIParserTests(unittest.TestCase):
    def test_parse_openai_ireland_invoice(self) -> None:
        parsed = parse_openai_text(IRELAND_SAMPLE, original_filename="Invoice-7BSDV5AM-0001.pdf")
        self.assertEqual(parsed.issuer_company_name, "OPENAI IRELAND LIMITED")
        self.assertEqual(parsed.invoice_number, "7BSDV5AM-0001")
        self.assertEqual(parsed.currency_code, "EUR")
        self.assertEqual(parsed.period_yyyymm, "202601")
        self.assertEqual(parsed.division_invoice, "chatgpt_plus")
        self.assertEqual(parsed.gross_amount, Decimal("23.00"))

    def test_parse_openai_receipt(self) -> None:
        parsed = parse_openai_text(RECEIPT_SAMPLE, original_filename="Receipt-2699-8822-5500.pdf")
        self.assertEqual(parsed.invoice_number, "BZHJNTUB-0004")
        self.assertEqual(parsed.document_type, "receipt")
        self.assertEqual(parsed.division_invoice, "receipt")
        self.assertEqual(parsed.currency_code, "USD")
        self.assertEqual(parsed.gross_amount, Decimal("12.26"))


if __name__ == "__main__":
    unittest.main()
