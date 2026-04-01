from __future__ import annotations

from decimal import Decimal
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from lector_facturas.parsers.dct import parse_dct_text


RANGE_SAMPLE = """
DCT GmbH
Artesta Store, S.L.
SAMMELRECHNUNG NR. 26-0045 Rechnungsdatum: 15.01.2026
diverse Drucksachen „artesta store“ 01.01.2026 - 14.01.2026
Nettobetrag: 5.835,02 EUR
19 % MwSt 1.108,65 EUR
Gesamtbetrag: 6.943,67 EUR
"""

RESHIP_SAMPLE = """
DCT GmbH
Artesta Store, S.L.
SAMMELRECHNUNG NR. 26-0167 Rechnungsdatum: 31.01.2026
Versandarbeiten Handlingspauschale für den 2. Versand
Nettobetrag: 92,50 EUR
19 % MwSt 17,58 EUR
Gesamtbetrag: 110,08 EUR
"""


class DctParserTests(unittest.TestCase):
    def test_parse_range_invoice(self) -> None:
        invoice = parse_dct_text(RANGE_SAMPLE, original_filename="RE_26-0045.pdf")

        self.assertEqual(invoice.supplier_code, "DCT")
        self.assertEqual(invoice.invoice_number, "26-0045")
        self.assertEqual(invoice.invoice_date.isoformat(), "2026-01-15")
        self.assertEqual(invoice.billing_period_start.isoformat(), "2026-01-01")
        self.assertEqual(invoice.billing_period_end.isoformat(), "2026-01-14")
        self.assertEqual(invoice.period_yyyymm, "202601")
        self.assertEqual(invoice.vat_percent, Decimal("19"))
        self.assertEqual(invoice.net_amount, Decimal("5835.02"))
        self.assertEqual(invoice.vat_amount, Decimal("1108.65"))
        self.assertEqual(invoice.gross_amount, Decimal("6943.67"))

    def test_parse_reship_invoice_without_range(self) -> None:
        invoice = parse_dct_text(RESHIP_SAMPLE, original_filename="RE_26-0167.pdf")

        self.assertEqual(invoice.invoice_number, "26-0167")
        self.assertEqual(invoice.invoice_date.isoformat(), "2026-01-31")
        self.assertEqual(invoice.billing_period_start.isoformat(), "2026-01-31")
        self.assertEqual(invoice.billing_period_end.isoformat(), "2026-01-31")
        self.assertEqual(invoice.period_yyyymm, "202601")
        self.assertEqual(invoice.vat_percent, Decimal("19"))
        self.assertEqual(invoice.net_amount, Decimal("92.50"))
        self.assertEqual(invoice.vat_amount, Decimal("17.58"))
        self.assertEqual(invoice.gross_amount, Decimal("110.08"))


if __name__ == "__main__":
    unittest.main()
