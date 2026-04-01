from __future__ import annotations

from decimal import Decimal
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from lector_facturas.parsers.pressing import parse_pressing_text


JAN_SAMPLE = """
Factura: FAC/2026/26100000691
Fecha: 31-01-2026
ARTESTA STORE, S.L.
Pedidos realizados durante el mes de Enero 2026 en Sant Feliu
Pedidos realizados durante el mes de Enero 2026 en Cornellá.
Importe % Dto Importe Dto B. Imponible IVA T. Impuesto Total Factura
28.979,31€ 0,00% 0,00€ 28.979,31€ 21,00% 6.085,66€ 35.064,97€
PRESSING Impressió Digital, S.A.
"""

FEB_SAMPLE = """
Factura: FAC/2026/26100001651
Fecha: 28-02-2026
ARTESTA STORE, S.L.
Pedidos realizados durante el mes de Febrero 2026 en Sant Feliu
Pedidos realizados durante el mes de Febrero 2026 en Cornellá.
Importe % Dto Importe Dto B. Imponible IVA T. Impuesto Total Factura
33.510,62€ 0,00% 0,00€ 33.510,62€ 21,00% 7.037,23€ 40.547,85€
PRESSING Impressió Digital, S.A.
"""


class PressingParserTests(unittest.TestCase):
    def test_parse_january_sample(self) -> None:
        invoice = parse_pressing_text(JAN_SAMPLE, original_filename="factura_26100000691-normal.pdf")

        self.assertEqual(invoice.supplier_code, "PRESSING")
        self.assertEqual(invoice.invoice_number, "FAC/2026/26100000691")
        self.assertEqual(invoice.invoice_date.isoformat(), "2026-01-31")
        self.assertEqual(invoice.billing_period_start.isoformat(), "2026-01-01")
        self.assertEqual(invoice.billing_period_end.isoformat(), "2026-01-31")
        self.assertEqual(invoice.period_yyyymm, "202601")
        self.assertEqual(invoice.vat_percent, Decimal("21.00"))
        self.assertEqual(invoice.net_amount, Decimal("28979.31"))
        self.assertEqual(invoice.vat_amount, Decimal("6085.66"))
        self.assertEqual(invoice.gross_amount, Decimal("35064.97"))

    def test_parse_february_sample(self) -> None:
        invoice = parse_pressing_text(FEB_SAMPLE, original_filename="factura_26100001651-normal (2).pdf")

        self.assertEqual(invoice.supplier_code, "PRESSING")
        self.assertEqual(invoice.invoice_number, "FAC/2026/26100001651")
        self.assertEqual(invoice.invoice_date.isoformat(), "2026-02-28")
        self.assertEqual(invoice.billing_period_start.isoformat(), "2026-02-01")
        self.assertEqual(invoice.billing_period_end.isoformat(), "2026-02-28")
        self.assertEqual(invoice.period_yyyymm, "202602")
        self.assertEqual(invoice.vat_percent, Decimal("21.00"))
        self.assertEqual(invoice.net_amount, Decimal("33510.62"))
        self.assertEqual(invoice.vat_amount, Decimal("7037.23"))
        self.assertEqual(invoice.gross_amount, Decimal("40547.85"))


if __name__ == "__main__":
    unittest.main()
