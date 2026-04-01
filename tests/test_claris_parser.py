from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.claris import parse_claris_text


CLARIS_SAMPLE = dedent(
    """
    CLARÍS GESTIÓ I DOCUMENTACIÓ, S.L.
    ARTESTA STORE, S.L.
    Factura Nº: F00036/26 Fecha de vencimiento: 28/01/2026
    Fecha de expedición: 21/01/2026 Nuestra Referencia: FIJO20-208
    Asunto: Asesoramiento fiscal y contable
    ASESORAMIENTO FISCAL CONTABLE
    Correspondiente al mes de enero
    808,08 €
    I.V.A 21,00 % S/ 808,08 €
    169,70 €
    TOTAL HONORARIOS
    977,78 €
    TOTAL A PAGAR
    977,78 €
    """
)


class ClarisParserTests(unittest.TestCase):
    def test_parse_claris_invoice(self) -> None:
        parsed = parse_claris_text(CLARIS_SAMPLE, original_filename="FACTURAF00036_26..PDF")
        self.assertEqual(parsed.supplier_code, "CLARIS")
        self.assertEqual(parsed.invoice_number, "F00036/26")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-01-21")
        self.assertEqual(parsed.billing_period_start.isoformat(), "2026-01-01")
        self.assertEqual(parsed.billing_period_end.isoformat(), "2026-01-31")
        self.assertEqual(parsed.period_yyyymm, "202601")
        self.assertEqual(parsed.net_amount, Decimal("808.08"))
        self.assertEqual(parsed.vat_amount, Decimal("169.70"))
        self.assertEqual(parsed.gross_amount, Decimal("977.78"))
        self.assertEqual(parsed.vat_percent, Decimal("21.00"))


if __name__ == "__main__":
    unittest.main()
