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

    def test_parse_railway_receipt(self) -> None:
        sample = dedent(
            """
            Receipt
            Invoice number 1602C2F5-0019
            Receipt number 2812-9345
            Date paid May 1, 2026
            Railway Corporation
            Bill to
            ARTESTA STORE, S.L
            Disk (per GB / min)
            Apr 1-May 1, 2026
            Total excluding tax $40.70
            VAT - Spain -21% on $40.70- $8.55
            Subtotal $60.70
            Total $49.25
            Amount paid $49.25
            """
        )
        parsed = parse_railway_text(sample, original_filename="Receipt-2812-9345.pdf")
        self.assertEqual(parsed.invoice_number, "1602C2F5-0019")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-05-01")
        self.assertEqual(parsed.period_yyyymm, "202604")
        self.assertEqual(parsed.net_amount, Decimal("40.70"))
        self.assertEqual(parsed.vat_amount, Decimal("8.55"))
        self.assertEqual(parsed.gross_amount, Decimal("49.25"))

    def test_parse_railway_reverse_charge_invoice(self) -> None:
        sample = dedent(
            """
            Invoice
            Invoice number 1602C2F5-0024
            Date of issue July 1, 2026
            Railway Corporation
            Bill to
            ARTESTA STORE, S.L
            Jun 1-Jul 1, 2026
            Jul 1-Aug 1, 2026
            Subtotal $131.79
            Total $111.79
            Amount due $111.79 USD
            Tax to be paid on reverse charge basis
            """
        )
        parsed = parse_railway_text(sample, original_filename="Invoice-1602C2F5-0024.pdf")
        self.assertEqual(parsed.invoice_number, "1602C2F5-0024")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-07-01")
        self.assertEqual(parsed.period_yyyymm, "202606")
        self.assertEqual(parsed.net_amount, Decimal("111.79"))
        self.assertEqual(parsed.vat_amount, Decimal("0"))
        self.assertEqual(parsed.gross_amount, Decimal("111.79"))
        self.assertEqual(parsed.vat_percent, Decimal("0"))


if __name__ == "__main__":
    unittest.main()
