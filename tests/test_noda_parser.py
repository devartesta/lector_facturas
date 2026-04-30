from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from lector_facturas.parsers.noda import parse_noda_text


class NodaParserTests(unittest.TestCase):
    def test_parses_2025_quarterly_invoice(self) -> None:
        text = """
ASESORIA FISCAL NODA Y ASOCIADOS, S.L.
SRES. ARTESTA STORE, S.L.
F A C T U R A N° 0 20 /26
En Santa Cruz de Tenerife a, 20 de Enero de 2.026
Factura en concepto de honorarios profesionales por el servicio de Asesoría Contable, Fiscal y de Organización Administrativa, correspondiente al período trimestral de Octubre a Diciembre de 2.025.
HONORARIOS 170,00 €
+ 7% I.G.I.C. 11,90 €
TOTAL 181,90 €
"""
        parsed = parse_noda_text(text, original_filename="FACTURA Enero 2026.pdf")
        self.assertEqual(parsed.supplier_code, "NODA")
        self.assertEqual(parsed.invoice_number, "020/26")
        self.assertEqual(parsed.invoice_date, date(2026, 1, 20))
        self.assertEqual(parsed.billing_period_start, date(2025, 10, 1))
        self.assertEqual(parsed.billing_period_end, date(2025, 12, 31))
        self.assertEqual(parsed.period_yyyymm, "202512")
        self.assertEqual(parsed.net_amount, Decimal("170.00"))
        self.assertEqual(parsed.vat_amount, Decimal("11.90"))
        self.assertEqual(parsed.gross_amount, Decimal("181.90"))
        self.assertEqual(parsed.vat_percent, Decimal("7"))

    def test_parses_2026_quarterly_invoice(self) -> None:
        text = """
ASESORIA FISCAL NODA Y ASOCIADOS, S.L.
SRES. ARTESTA STORE, S.L.
F A C T U R A N° 0 81 /26
En Santa Cruz de Tenerife a, 20 de Abril de 2.026
Factura en concepto de honorarios profesionales por el servicio de Asesoría Contable, Fiscal y de Organización Administrativa, correspondiente al período trimestral de Enero a Marzo de 2.026.
HONORARIOS 180,00 €
+ 7% I.G.I.C. 12,60 €
TOTAL 192,60 €
"""
        parsed = parse_noda_text(text, original_filename="FACTURA Abril 2026 ARTESTA STORE, S.L.pdf")
        self.assertEqual(parsed.invoice_number, "081/26")
        self.assertEqual(parsed.invoice_date, date(2026, 4, 20))
        self.assertEqual(parsed.billing_period_start, date(2026, 1, 1))
        self.assertEqual(parsed.billing_period_end, date(2026, 3, 31))
        self.assertEqual(parsed.period_yyyymm, "202603")


if __name__ == "__main__":
    unittest.main()
