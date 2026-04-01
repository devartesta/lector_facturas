from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.godaddy import parse_godaddy_text


SAMPLE = dedent(
    """
    Recibo
    № 3984999752
    FECHA:
    5/1/2026
    Subtotal 16,99 €
    Impuestos 3,57 €
    Total (EUR) 20,56 €
    A1 Neto 16,99 € VAT (21,00 %) 3,57 €
    """
)


class GoDaddyParserTests(unittest.TestCase):
    def test_parse(self) -> None:
        parsed = parse_godaddy_text(SAMPLE, original_filename="godaddy.pdf")
        self.assertEqual(parsed.invoice_number, "3984999752")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-01-05")
        self.assertEqual(parsed.net_amount, Decimal("16.99"))
        self.assertEqual(parsed.vat_amount, Decimal("3.57"))
        self.assertEqual(parsed.gross_amount, Decimal("20.56"))


if __name__ == "__main__":
    unittest.main()
