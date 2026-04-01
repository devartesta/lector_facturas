from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.masmovil import parse_masmovil_text


SAMPLE = dedent(
    """
    TOTAL A PAGAR
    MC260001070524
    Fecha de emisión:
    22/01/2026
    Periodo facturado:
    Del 22/12/2025 al 21/01/2026
    Base imponible 5,00€
    IVA 21% 1,05€
    TOTAL A PAGAR 6,05€
    """
)


class MasMovilParserTests(unittest.TestCase):
    def test_parse(self) -> None:
        parsed = parse_masmovil_text(SAMPLE, original_filename="masmovil.pdf")
        self.assertEqual(parsed.invoice_number, "MC260001070524")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-01-22")
        self.assertEqual(parsed.period_yyyymm, "202601")
        self.assertEqual(parsed.net_amount, Decimal("5.00"))
        self.assertEqual(parsed.vat_amount, Decimal("1.05"))
        self.assertEqual(parsed.gross_amount, Decimal("6.05"))


if __name__ == "__main__":
    unittest.main()
