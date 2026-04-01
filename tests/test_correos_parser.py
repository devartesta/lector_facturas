from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.correos import parse_correos_text


PENINSULA_SAMPLE = dedent(
    """
    FACTURA
    NIF CONTRATO CLIENTE FECHA Nº FACTURA
    B67503250 54061813 9981387579 31.01.2026 4004566874
    Titular: Artesta Store, S.L., Sant Jaume 20, 08035 BARCELONA , BARCELONA.
    Período de Facturación: Enero 2026
    01.01.2026/31.01.2026
    Resumen
    Base imponible sujeta a impuesto (IVA)                397,96
    Importe Bruto                506,27
    Importe bonificación               -108,31
    Tipo impositivo: 21,00 %
    Cuota:                 83,57
    Total factura en Euros                481,53
    """
)

CANARIAS_SAMPLE = dedent(
    """
    FACTURA
    NIF CONTRATO CLIENTE FECHA Nº FACTURA
    B67503250 54061813 9981387579 28.02.2026 4004608216
    Titular: Artesta Store, S.L., Sant Jaume 20, 08035 BARCELONA , BARCELONA.
    Período de Facturación: Febrero 2026
    01.02.2026/28.02.2026
    Resumen
    Base imponible sujeta a impuesto (IGIC)                 16,45
    Importe Bruto                 22,68
    Importe bonificación                 -6,23
    Tipo impositivo: 7,00 %
    Cuota:                  1,15
    Total factura en Euros                 17,60
    """
)


class CorreosParserTests(unittest.TestCase):
    def test_parse_peninsula_invoice(self) -> None:
        parsed = parse_correos_text(PENINSULA_SAMPLE, original_filename="4004566874.PDF")
        self.assertEqual(parsed.supplier_code, "CORREOS")
        self.assertEqual(parsed.invoice_number, "4004566874")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-01-31")
        self.assertEqual(parsed.billing_period_start.isoformat(), "2026-01-01")
        self.assertEqual(parsed.billing_period_end.isoformat(), "2026-01-31")
        self.assertEqual(parsed.vat_percent, Decimal("21.00"))
        self.assertEqual(parsed.net_amount, Decimal("397.96"))
        self.assertEqual(parsed.vat_amount, Decimal("83.57"))
        self.assertEqual(parsed.gross_amount, Decimal("481.53"))

    def test_parse_canarias_invoice(self) -> None:
        parsed = parse_correos_text(CANARIAS_SAMPLE, original_filename="4004608216.PDF")
        self.assertEqual(parsed.supplier_code, "CORREOSCAN")
        self.assertEqual(parsed.invoice_number, "4004608216")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-02-28")
        self.assertEqual(parsed.billing_period_start.isoformat(), "2026-02-01")
        self.assertEqual(parsed.billing_period_end.isoformat(), "2026-02-28")
        self.assertEqual(parsed.vat_percent, Decimal("7.00"))
        self.assertEqual(parsed.net_amount, Decimal("16.45"))
        self.assertEqual(parsed.vat_amount, Decimal("1.15"))
        self.assertEqual(parsed.gross_amount, Decimal("17.60"))


if __name__ == "__main__":
    unittest.main()
