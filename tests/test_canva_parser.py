from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.canva import parse_canva_text


SAMPLE = dedent(
    """
    Factura fiscal
    Fecha de factura
    18 de enero de 2026
    Nro. de factura
    04765-19148572
    Total 11,99 €
    Impuestos incluidos 2,08 €
    Importe total 11,99 €
    """
)


class CanvaParserTests(unittest.TestCase):
    def test_parse(self) -> None:
        parsed = parse_canva_text(SAMPLE, original_filename="invoice.pdf")
        self.assertEqual(parsed.invoice_number, "04765-19148572")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-01-18")
        self.assertEqual(parsed.net_amount, Decimal("9.91"))
        self.assertEqual(parsed.vat_amount, Decimal("2.08"))
        self.assertEqual(parsed.gross_amount, Decimal("11.99"))


if __name__ == "__main__":
    unittest.main()
