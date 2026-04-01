from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.gls import parse_gls_ocr_text


JANUARY_SAMPLE = dedent(
    """
    RGT
    CLIENTE: DIRECCION: POBLACION:
    RGT LOGISTICA - RGT MENSAJEROS
    ARTESTA STORE, S.L.
    Passatge Sant Jaume, 20
    08035 BARCELONA B67503250
    N Factura:
    20260014
    Fecha Fact:
    31,01,26
    FECHA
    31,01,26
    segun listado anexo
    finance@artestastore.com
    EUROS
    EXENTO
    12.319,26
    VENCIMIENTO
    28,02,2026
    EXENTO DE IVA: SUBTOTAL: 21 % IVA:
    TOTAL:
    12.319,26
    2.587,04
    14.906,30
    info@rgtmensajeros.com
    """
)

FEBRUARY_SAMPLE = dedent(
    """
    RGT
    RGT LOGISTICA - RGT MENSAJEROS
    ARTESTA STORE, S.L.
    N Factura:
    20260082
    Fecha Fact:
    28,02,26
    segun listado anexo
    15.947,02
    EXENTO DE IVA: SUBTOTAL: 21 % IVA: TOTAL:
    15.947,02
    3.348,87
    19.295,89
    info@rgtmensajeros.com
    """
)


class GlsParserTests(unittest.TestCase):
    def test_parse_january_gls_invoice(self) -> None:
        parsed = parse_gls_ocr_text(JANUARY_SAMPLE, original_filename="Escaneo0160.pdf")
        self.assertEqual(parsed.invoice_number, "20260014")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-01-31")
        self.assertEqual(parsed.billing_period_start.isoformat(), "2026-01-01")
        self.assertEqual(parsed.billing_period_end.isoformat(), "2026-01-31")
        self.assertEqual(parsed.period_yyyymm, "202601")
        self.assertEqual(parsed.net_amount, Decimal("12319.26"))
        self.assertEqual(parsed.vat_amount, Decimal("2587.04"))
        self.assertEqual(parsed.gross_amount, Decimal("14906.30"))
        self.assertEqual(parsed.vat_percent, Decimal("21.00"))

    def test_parse_february_gls_invoice(self) -> None:
        parsed = parse_gls_ocr_text(FEBRUARY_SAMPLE, original_filename="Escaneo0011.pdf")
        self.assertEqual(parsed.invoice_number, "20260082")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-02-28")
        self.assertEqual(parsed.billing_period_start.isoformat(), "2026-02-01")
        self.assertEqual(parsed.billing_period_end.isoformat(), "2026-02-28")
        self.assertEqual(parsed.period_yyyymm, "202602")
        self.assertEqual(parsed.net_amount, Decimal("15947.02"))
        self.assertEqual(parsed.vat_amount, Decimal("3348.87"))
        self.assertEqual(parsed.gross_amount, Decimal("19295.89"))


if __name__ == "__main__":
    unittest.main()
