from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.torras import parse_torras_text


TORRAS_SAMPLE = dedent(
    """
    TORRAS ABOGADOS Y ECONOMISTAS ASOCIADOS, S.L.P.
    02/01/26        9438 7/186 1
    15,00 21
    15,00      3,15
    18,15
    ARTESTA STORE SL
    DEH Cuota por GESTIÓN-CONSULTAS de la Dirección
    Electrónica Habilitada (DEH) correspondiente
    al período Enero de 2026
    IMPORTE LÍQUIDO
    18,15
    """
)


class TorrasParserTests(unittest.TestCase):
    def test_parse_torras_invoice(self) -> None:
        parsed = parse_torras_text(TORRAS_SAMPLE, original_filename="267F0000186-09438F.PDF")
        self.assertEqual(parsed.supplier_code, "TORRAS")
        self.assertEqual(parsed.invoice_number, "7/186")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-01-02")
        self.assertEqual(parsed.billing_period_start.isoformat(), "2026-01-01")
        self.assertEqual(parsed.billing_period_end.isoformat(), "2026-01-31")
        self.assertEqual(parsed.period_yyyymm, "202601")
        self.assertEqual(parsed.net_amount, Decimal("15.00"))
        self.assertEqual(parsed.vat_amount, Decimal("3.15"))
        self.assertEqual(parsed.gross_amount, Decimal("18.15"))


if __name__ == "__main__":
    unittest.main()
