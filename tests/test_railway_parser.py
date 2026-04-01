from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.railway import parse_railway_text


RAILWAY_SAMPLE = dedent(
    """
    Invoice
    Invoice number 1602C2F5-0016
    Date of issue February 1, 2026
    Railway Corporation
    ARTESTA STORE, S.L
    Dec 31, 2025-Jan 31, 2026
    Jan 31, 2026-Feb 28, 2026
    Total excluding tax $20.00
    VAT - Spain 21% on $20.00 $4.20
    Amount due $24.20 USD
    """
)


class RailwayParserTests(unittest.TestCase):
    def test_parse_railway_invoice(self) -> None:
        parsed = parse_railway_text(RAILWAY_SAMPLE, original_filename="Invoice-1602C2F5-0016.pdf")
        self.assertEqual(parsed.invoice_number, "1602C2F5-0016")
        self.assertEqual(parsed.period_yyyymm, "202512")
        self.assertEqual(parsed.net_amount, Decimal("20.00"))
        self.assertEqual(parsed.vat_amount, Decimal("4.20"))
        self.assertEqual(parsed.gross_amount, Decimal("24.20"))


if __name__ == "__main__":
    unittest.main()
