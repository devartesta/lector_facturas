from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.googleworkspace import parse_googleworkspace_text


SAMPLE = dedent(
    """
    Número de factura: 5472439457
    ..............................................................31 ene 2026
    Total en EUR
    194,40 €
    0,00 €
    194,40 €
    Resumen de 1 ene 2026 - 31 ene 2026
    Subtotal en EUR
    I.V.A. (0%)
    """
)


class GoogleWorkspaceParserTests(unittest.TestCase):
    def test_parse(self) -> None:
        parsed = parse_googleworkspace_text(SAMPLE, original_filename="5472439457.pdf")
        self.assertEqual(parsed.invoice_number, "5472439457")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-01-31")
        self.assertEqual(parsed.period_yyyymm, "202601")
        self.assertEqual(parsed.net_amount, Decimal("194.40"))
        self.assertEqual(parsed.vat_amount, Decimal("0"))
        self.assertEqual(parsed.gross_amount, Decimal("194.40"))


if __name__ == "__main__":
    unittest.main()
