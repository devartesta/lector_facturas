from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.marketing_ads import parse_google_ads_text, parse_meta_ads_text


GOOGLE_SAMPLE = dedent(
    """
    Factura
    Numero de factura: 5489506571
    31 ene 2026
    29.164,06
    Fecha de vencimiento: 1 abr 2026
    Google Ads
    Importe total pendiente de pago en EUR
    Resumen de 1 ene 2026 - 31 ene 2026
    Impuesto sobre servicios digitales de Austria * 62,36
    Coste de operaciones normativo de Turqua * 0,13
    Impuesto sobre servicios digitales del Reino Unido * 49,63
    Coste de operaciones normativo de Espaa * 261,83
    Coste de operaciones normativo de Francia * 111,27
    Coste de operaciones normativo de Italia * 95,27
    Search - ES - Search General 9717 Clics 3.352,17
    PMAX - US - All Products 2972 Clics 1.328,51
    Search - UK - Search General 1997 Clics 1.229,26
    Shopping - US - Heroe 1687 Clics 271,20
    Brand - UK - (own & others) 439 Clics 204,25
    PMAX - DE - ALL Products 6153 Clics 1.891,64
    PMax - UK - All Products 2687 Clics 854,72
    """
)


META_SAMPLE = dedent(
    """
    Factura Numero: 251380763
    Fecha de Factura: 01-Mar-2026
    Periodo de facturacion:Feb-26
    Total Factura: 20,074.99
    1 AT Advantage+ shopping campaign 289.82
    2 DE Advantage+ shopping campaign 962.56
    3 ES Advantage+ shopping campaign 1,267.25
    4 FR Advantage+ shopping campaign 1,547.31
    5 GLOBAL 2025 918.51
    11 Instagram - IT Advantage+ shopping campaign 975.32
    12 Instagram - UK Advantage+ shopping campaign 1,078.75
    13 Instagram - US Advantage+ shopping campaign 515.34
    14 IT Advantage+ shopping campaign 703.02
    15 UK Advantage+ shopping campaign 1,715.29
    16 US Advantage+ shopping campaign 882.06
    """
)


class MarketingAdsParserTests(unittest.TestCase):
    def test_parse_google_ads_divisions(self) -> None:
        rows = parse_google_ads_text(GOOGLE_SAMPLE, original_filename="5489506571.pdf")
        self.assertEqual({row.division_invoice for row in rows}, {"uk", "us", "eu"})
        by_division = {row.division_invoice: row for row in rows}
        self.assertEqual(by_division["uk"].invoice_number, "5489506571")
        self.assertEqual(by_division["uk"].period_yyyymm, "202601")
        self.assertEqual(by_division["us"].net_amount, Decimal("1599.71"))
        self.assertEqual(by_division["uk"].net_amount, Decimal("2337.86"))
        self.assertEqual(by_division["eu"].gross_amount, Decimal("25226.49"))

    def test_parse_meta_ads_divisions(self) -> None:
        rows = parse_meta_ads_text(META_SAMPLE, original_filename="Transaction_251380763.pdf")
        self.assertEqual({row.division_invoice for row in rows}, {"uk", "us", "eu"})
        by_division = {row.division_invoice: row for row in rows}
        self.assertEqual(by_division["uk"].invoice_number, "251380763")
        self.assertEqual(by_division["uk"].period_yyyymm, "202602")
        self.assertEqual(by_division["uk"].net_amount, Decimal("2794.04"))
        self.assertEqual(by_division["us"].net_amount, Decimal("1397.40"))
        self.assertEqual(by_division["eu"].gross_amount, Decimal("15883.55"))


if __name__ == "__main__":
    unittest.main()
