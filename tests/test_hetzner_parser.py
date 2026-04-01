from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.hetzner import parse_hetzner_text


SAMPLE = dedent(
    """
    Invoice no.: 084000638408
    Invoice date: 16/01/2026
    Storage (12/2025)
    Total € 10.90 € 2.29 € 13.19
    SJ 21 % € 10.90 € 2.29 € 13.19
    Amount due: € 13.19
    """
)


class HetznerParserTests(unittest.TestCase):
    def test_parse(self) -> None:
        parsed = parse_hetzner_text(SAMPLE, original_filename="hetzner.pdf")
        self.assertEqual(parsed.invoice_number, "084000638408")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-01-16")
        self.assertEqual(parsed.period_yyyymm, "202512")
        self.assertEqual(parsed.net_amount, Decimal("10.90"))
        self.assertEqual(parsed.vat_amount, Decimal("2.29"))
        self.assertEqual(parsed.gross_amount, Decimal("13.19"))


if __name__ == "__main__":
    unittest.main()
