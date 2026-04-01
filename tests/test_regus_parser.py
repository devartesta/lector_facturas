from __future__ import annotations

from decimal import Decimal
import unittest

from lector_facturas.parsers.regus import parse_regus_pdf


class RegusParserSmokeTests(unittest.TestCase):
    def test_real_file(self) -> None:
        parsed = parse_regus_pdf(
            __import__("pathlib").Path(
                r"C:\Users\ADRISE~1\OneDrive - Artesta\ARTESTA - 6. Finances\Artesta Inc\2026\1Q\202601\Operating Expenses\regus\Invoice(3313-43718).pdf"
            )
        )
        self.assertEqual(parsed.invoice_number, "3313-43718")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-02-01")
        self.assertEqual(parsed.billing_period_start.isoformat(), "2026-01-01")
        self.assertEqual(parsed.billing_period_end.isoformat(), "2026-01-31")
        self.assertEqual(parsed.net_amount, Decimal("30.00"))
        self.assertEqual(parsed.vat_amount, Decimal("1.80"))
        self.assertEqual(parsed.gross_amount, Decimal("31.80"))


if __name__ == "__main__":
    unittest.main()
