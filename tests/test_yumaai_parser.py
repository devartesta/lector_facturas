from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.yumaai import parse_yumaai_text


SAMPLE = dedent(
    """
    Número de factura OQXBYXMP-0004
    Fecha de emisión 13 de enero de 2026
    13 dic 2025 – 13 ene 2026
    13 ene 2026 – 13 feb 2026
    Subtotal 507,20 €
    Total 507,20 €
    Importe adeudado 507,20 €
    """
)


class YumaParserTests(unittest.TestCase):
    def test_parse(self) -> None:
        parsed = parse_yumaai_text(SAMPLE, original_filename="yuma.pdf")
        self.assertEqual(parsed.invoice_number, "OQXBYXMP-0004")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-01-13")
        self.assertEqual(parsed.billing_period_start.isoformat(), "2025-12-13")
        self.assertEqual(parsed.billing_period_end.isoformat(), "2026-02-13")
        self.assertEqual(parsed.net_amount, Decimal("507.20"))


if __name__ == "__main__":
    unittest.main()
