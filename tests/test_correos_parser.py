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

CANARIAS_MIXED_SAMPLE = dedent(
    """
    FACTURA
    NIF CONTRATO CLIENTE FECHA NÂº FACTURA
    B67503250 54061813 9981387579 30.04.2026 4004673698
    Titular: Artesta Store, S.L., Sant Jaume 20, 08035 BARCELONA , BARCELONA.
    PerÃ­odo de FacturaciÃ³n: Abril 2026
    01.04.2026/30.04.2026
    Resumen
    Base imponible sujeta a impuesto (IGIC) 4,75
    Importe Bruto 6,66
    Importe bonificaciÃ³n -1,91
    Tipo impositivo: IGIC repercutido 0%
    Cuota: 0,00
    Base imponible sujeta a impuesto (IGIC) 8,23
    Importe Bruto 11,34
    Importe bonificaciÃ³n -3,11
    Tipo impositivo: 7,00 %
    Cuota: 0,58
    Totales
    Total importe bruto 18,00
    Total importe bonificaciÃ³n -5,02
    Total importe neto antes de impuesto 12,98
    Total impuesto 0,58
    Total factura en Euros 13,56
    """
)

CANARIAS_EXENTO_SAMPLE = dedent(
    """
    FACTURA
    NIF CONTRATO CLIENTE FECHA NÂº FACTURA
    B67503250 54061813 9981387579 31.03.2026 4004641447
    Titular: Artesta Store, S.L., Sant Jaume 20, 08035 BARCELONA , BARCELONA.
    PerÃ­odo de FacturaciÃ³n: Marzo 2026
    01.03.2026/31.03.2026
    Resumen
    Base imponible sujeta a impuesto (IGIC) 8,83
    Importe Bruto 12,28
    Importe bonificaciÃ³n -3,45
    Tipo impositivo: EXENTO
    Cuota: 0,00
    Base imponible sujeta a impuesto (IGIC) 14,83
    Importe Bruto 16,99
    Importe bonificaciÃ³n -2,16
    Tipo impositivo: 7,00 %
    Cuota: 1,04
    Totales
    Total importe bruto 29,27
    Total importe bonificaciÃ³n -5,61
    Total importe neto antes de impuesto 23,66
    Total impuesto 1,04
    Total factura en Euros 24,70
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

    def test_parse_canarias_invoice_with_exempt_and_taxable_bases(self) -> None:
        parsed = parse_correos_text(CANARIAS_MIXED_SAMPLE, original_filename="4004673698.PDF")
        self.assertEqual(parsed.supplier_code, "CORREOSCAN")
        self.assertEqual(parsed.invoice_number, "4004673698")
        self.assertEqual(parsed.vat_percent, Decimal("7.00"))
        self.assertEqual(parsed.net_amount, Decimal("12.98"))
        self.assertEqual(parsed.net_amount, sum(item.net_amount for item in parsed.tax_breakdowns))
        self.assertEqual(parsed.vat_amount, Decimal("0.58"))
        self.assertEqual(parsed.gross_amount, Decimal("13.56"))
        self.assertEqual(len(parsed.tax_breakdowns), 2)
        self.assertEqual(parsed.tax_breakdowns[0].vat_percent, Decimal("0"))
        self.assertEqual(parsed.tax_breakdowns[0].net_amount, Decimal("4.75"))
        self.assertEqual(parsed.tax_breakdowns[1].vat_percent, Decimal("7.00"))
        self.assertEqual(parsed.tax_breakdowns[1].net_amount, Decimal("8.23"))

    def test_parse_canarias_invoice_with_exento_label_uses_declared_net_total(self) -> None:
        parsed = parse_correos_text(CANARIAS_EXENTO_SAMPLE, original_filename="4004641447.PDF")
        self.assertEqual(parsed.supplier_code, "CORREOSCAN")
        self.assertEqual(parsed.invoice_number, "4004641447")
        self.assertEqual(parsed.vat_percent, Decimal("7.00"))
        self.assertEqual(parsed.net_amount, Decimal("23.66"))
        self.assertEqual(parsed.vat_amount, Decimal("1.04"))
        self.assertEqual(parsed.gross_amount, Decimal("24.70"))
        self.assertEqual(len(parsed.tax_breakdowns), 2)
        self.assertEqual(parsed.tax_breakdowns[0].vat_percent, Decimal("0.00"))
        self.assertEqual(parsed.tax_breakdowns[0].net_amount, Decimal("8.83"))
        self.assertEqual(parsed.tax_breakdowns[1].vat_percent, Decimal("7.00"))
        self.assertEqual(parsed.tax_breakdowns[1].net_amount, Decimal("14.83"))


if __name__ == "__main__":
    unittest.main()
