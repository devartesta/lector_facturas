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
    NÃºmero de factura: 2026-0012
    Qhands design SL.
    Renting CNC 1 1.661,16 â‚¬ 1.661,16 â‚¬ 21% 348,84 â‚¬
    Total Base Imponible: 1.661,16 â‚¬
    Total IVA: 348,84 â‚¬
    TOTAL: 2.010,00 â‚¬
    """
)

QHANDS_MARCH_SAMPLE = dedent(
    """
    Artesta Store, S.L.
    31/03/2026
    FACTURA
    NÃƒÂºmero de factura: 2026-0020
    Qhands design SL.
    Renting CNC 1 1.800,00 Ã¢â€šÂ¬ 1.800,00 Ã¢â€šÂ¬ 21% 378,00 Ã¢â€šÂ¬
    Total Base Imponible: 1.800,00 Ã¢â€šÂ¬
    Total IVA: 378,00 Ã¢â€šÂ¬
    TOTAL: 2.178,00 Ã¢â€šÂ¬
    """
)

RAPPEL_SAMPLE = dedent(
    """
    Artesta Store, S.L.
    26/01/2026
    FACTURA
    NÃºmero de factura: A_2026-0006
    Home design labs S.L.
    Rappel 2025 -1 1.118,95 â‚¬ -1.118,95 â‚¬ 21% -234,98 â‚¬
    Total Base Imponible: -1.118,95 â‚¬
    Total IVA: -234,98 â‚¬
    TOTAL: -1.353,93 â‚¬
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
    TOTAL TTC: â‚¬ 1,746.60
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
    TOTAL TTC: â‚¬ 15,609.96
    """
)

CHOOSE_MODERN_SAMPLE = dedent(
    """
    Artesta Store, S.L.
    FACTURA
    NÚMERO
    AS-110388
    FECHA
    2026/06/29
    DATOS DE CLIENTE
    CHOOSE SAS
    Artículo Cantidad IVA Precio unitario Precio unitario sin IVA Total
    Choose campaign 1 0% € 6,777.88 € 6,777.88 € 6,777.88
    Subtotal: € 6,777.88
    Total : € 6,777.88
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

    def test_parse_choose_modern_format(self) -> None:
        parsed = parse_choose_text(CHOOSE_MODERN_SAMPLE, original_filename="invoice-AS-110388.pdf")
        self.assertEqual(parsed.supplier_code, "CHOOSE")
        self.assertEqual(parsed.invoice_number, "AS-110388")
        self.assertEqual(parsed.period_yyyymm, "202606")
        self.assertEqual(parsed.gross_amount, Decimal("6777.88"))


if __name__ == "__main__":
    unittest.main()
