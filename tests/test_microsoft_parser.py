from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.microsoft import parse_microsoft_text


SAMPLE = dedent(
    """
    Número de facturación G135222851
    Fecha del documento 13/01/2026
    12/01/2026-
    11/02/2026
    Total (sin incluir impuestos) 30.87
    Importe de impuestos 6.48
    Total con impuestos incluidos 37.35
    Ventas nacionales con tasa estándar 21.00%
    """
)


class MicrosoftParserTests(unittest.TestCase):
    def test_parse(self) -> None:
        parsed = parse_microsoft_text(SAMPLE, original_filename="microsoft.pdf")
        self.assertEqual(parsed.invoice_number, "G135222851")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-01-13")
        self.assertEqual(parsed.period_yyyymm, "202601")
        self.assertEqual(parsed.net_amount, Decimal("30.87"))
        self.assertEqual(parsed.vat_amount, Decimal("6.48"))
        self.assertEqual(parsed.gross_amount, Decimal("37.35"))


if __name__ == "__main__":
    unittest.main()
