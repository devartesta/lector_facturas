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

CLARIS_SUP_SAMPLE = dedent(
    """
    CLARÍS GESTIÓ I DOCUMENTACIÓ, S.L.
    ARTESTA STORE, S.L.
    Factura SUP Nº: S00119/26 Fecha de vencimiento: 27/05/2026
    Fecha de expedición: 22/05/2026 Nuestra Referencia: FIS21-68
    Asunto: CUENTAS ANUALES Y LIBROS
    SUPLIDOS
    Tasas Registro Mercantil legalización libros oficiales 2025
    46,15 €
    TOTAL A PAGAR
    46,15 €
    FACTURA
    Nº FACTURA: 2026/1098086389
    FECHA: 21/05/2026
    Importe base total:
    43,54 €
    IVA (21,00%)
    9,14 €
    Total factura:
    52,68 €
    IRPF (15,00%)
    -6,53 €
    Total a pagar:
    46,15 €
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

    def test_parse_claris_sup_invoice_with_withholding(self) -> None:
        parsed = parse_claris_text(CLARIS_SUP_SAMPLE, original_filename="S00119_26.PDF")
        self.assertEqual(parsed.supplier_code, "CLARIS")
        self.assertEqual(parsed.invoice_number, "S00119/26")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-05-22")
        self.assertEqual(parsed.billing_period_start.isoformat(), "2026-05-01")
        self.assertEqual(parsed.billing_period_end.isoformat(), "2026-05-31")
        self.assertEqual(parsed.period_yyyymm, "202605")
        self.assertEqual(parsed.net_amount, Decimal("43.54"))
        self.assertEqual(parsed.vat_amount, Decimal("9.14"))
        self.assertEqual(parsed.gross_amount, Decimal("52.68"))
        self.assertEqual(parsed.vat_percent, Decimal("21.00"))
        self.assertEqual(parsed.withholding_percent, Decimal("15.00"))
        self.assertEqual(parsed.withholding_amount, Decimal("6.53"))
        self.assertEqual(parsed.payable_amount, Decimal("46.15"))


if __name__ == "__main__":
    unittest.main()
