from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.artesta_income import parse_qhands_text, parse_rappel_text
from lector_facturas.parsers.partner_income_fr import parse_choose_text, parse_toasty_text


QHANDS_SAMPLE = dedent(
    """
    Artesta Store, S.L.
    28/02/2026
    FACTURA
    Número de factura: 2026-0012
    Qhands design SL.
    Renting CNC 1 1.661,16 € 1.661,16 € 21% 348,84 €
    Total Base Imponible: 1.661,16 €
    Total IVA: 348,84 €
    TOTAL: 2.010,00 €
    """
)

QHANDS_MARCH_SAMPLE = dedent(
    """
    Artesta Store, S.L.
    31/03/2026
    FACTURA
    NÃºmero de factura: 2026-0020
    Qhands design SL.
    Renting CNC 1 1.800,00 â‚¬ 1.800,00 â‚¬ 21% 378,00 â‚¬
    Total Base Imponible: 1.800,00 â‚¬
    Total IVA: 378,00 â‚¬
    TOTAL: 2.178,00 â‚¬
    """
)

RAPPEL_SAMPLE = dedent(
    """
    Artesta Store, S.L.
    26/01/2026
    FACTURA
    Número de factura: A_2026-0006
    Home design labs S.L.
    Rappel 2025 -1 1.118,95 € -1.118,95 € 21% -234,98 €
    Total Base Imponible: -1.118,95 €
    Total IVA: -234,98 €
    TOTAL: -1.353,93 €
    """
)

TOASTY_SAMPLE = dedent(
    """
    FACTURE
    COMMANDE NO
    AS-99158
    DATE DE COMMANDE
    2026/02/19
    CLIENT
    Toasty SAS
    TOTAL TTC: € 1,746.60
    """
)

CHOOSE_SAMPLE = dedent(
    """
    FACTURE
    COMMANDE NO
    AS-101940
    DATE DE COMMANDE
    2026/03/20
    CLIENT
    CHOOSE SAS
    TOTAL TTC: € 15,609.96
    """
)
class ArtestaIncomeParserTests(unittest.TestCase):
    def test_parse_qhands(self) -> None:
        parsed = parse_qhands_text(QHANDS_SAMPLE, original_filename="Factura_2026-0012.pdf")
        self.assertEqual(parsed.supplier_code, "QHANDS")
        self.assertEqual(parsed.division_invoice, "renting_cnc")
        self.assertEqual(parsed.net_amount, Decimal("1661.16"))
        self.assertEqual(parsed.period_yyyymm, "202602")

    def test_parse_qhands_uses_invoice_month_for_period(self) -> None:
        parsed = parse_qhands_text(QHANDS_MARCH_SAMPLE, original_filename="Factura_2026-0020.pdf")
        self.assertEqual(parsed.invoice_number, "2026-0020")
        self.assertEqual(parsed.period_yyyymm, "202603")
        self.assertEqual(parsed.gross_amount, Decimal("2178.00"))

    def test_parse_rappel(self) -> None:
        parsed = parse_rappel_text(RAPPEL_SAMPLE, original_filename="Factura_A_2026-0006.pdf")
        self.assertEqual(parsed.supplier_code, "LIVITUM")
        self.assertEqual(parsed.division_invoice, "rappels")
        self.assertEqual(parsed.gross_amount, Decimal("-1353.93"))

    def test_parse_toasty(self) -> None:
        parsed = parse_toasty_text(TOASTY_SAMPLE, original_filename="invoice-AS-99158.pdf")
        self.assertEqual(parsed.supplier_code, "TOASTY")
        self.assertEqual(parsed.gross_amount, Decimal("1746.60"))

    def test_parse_choose(self) -> None:
        parsed = parse_choose_text(CHOOSE_SAMPLE, original_filename="invoice-AS-101940.pdf")
        self.assertEqual(parsed.supplier_code, "CHOOSE")
        self.assertEqual(parsed.division_invoice, "campaign")
        self.assertEqual(parsed.gross_amount, Decimal("15609.96"))


if __name__ == "__main__":
    unittest.main()
