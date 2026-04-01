from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.jondo import parse_jondo_pdf


class JondoParserSmokeTests(unittest.TestCase):
    def test_real_file(self) -> None:
        parsed = parse_jondo_pdf(
            __import__("pathlib").Path(
                r"C:\Users\ADRISE~1\OneDrive - Artesta\ARTESTA - 6. Finances\Artesta Stores (UK) Ltd\2026\1Q\202601_UK\Operating Expenses\202601_jondogo\AS-94763.pdf"
            )
        )
        self.assertEqual(parsed.invoice_number, "AS-94763")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-01-04")
        self.assertEqual(parsed.net_amount, Decimal("44.06"))
        self.assertEqual(parsed.vat_amount, Decimal("8.81"))
        self.assertEqual(parsed.gross_amount, Decimal("52.87"))


if __name__ == "__main__":
    unittest.main()
