from __future__ import annotations

from decimal import Decimal
from pathlib import Path
import unittest

from lector_facturas.parsers.shared_services import parse_shared_services_pdf


class SharedServicesParserTests(unittest.TestCase):
    def test_ltd_invoice(self) -> None:
        parsed = parse_shared_services_pdf(
            Path(
                r"C:\Users\ADRISE~1\OneDrive - Artesta\ARTESTA - 6. Finances\Artesta Stores (UK) Ltd\2026\1Q\202601_UK\Operating Expenses\Shared Services\Factura_2026-0010.pdf"
            )
        )
        self.assertEqual(parsed.invoice_number, "2026-0010")
        self.assertEqual(parsed.period_yyyymm, "202601")
        self.assertEqual(parsed.net_amount, Decimal("10732.87"))
        self.assertEqual(len(parsed.line_items), 4)

    def test_inc_invoice(self) -> None:
        parsed = parse_shared_services_pdf(
            Path(
                r"C:\Users\ADRISE~1\OneDrive - Artesta\ARTESTA - 6. Finances\Artesta Inc\2026\1Q\202601\Operating Expenses\HANNUN\Factura_2026-0009.pdf"
            )
        )
        self.assertEqual(parsed.invoice_number, "2026-0009")
        self.assertEqual(parsed.period_yyyymm, "202601")
        self.assertEqual(parsed.net_amount, Decimal("7334.44"))

    def test_inc_february_invoice_from_income_folder(self) -> None:
        parsed = parse_shared_services_pdf(
            Path(
                r"C:\Users\ADRISE~1\OneDrive - Artesta\ARTESTA - 6. Finances\Artesta Store, S.L\2026\1Q\202602\Ingresos\INC\Factura_2026-0014.pdf"
            )
        )
        self.assertEqual(parsed.invoice_number, "2026-0014")
        self.assertEqual(parsed.period_yyyymm, "202602")
        self.assertEqual(parsed.net_amount, Decimal("8462.36"))


if __name__ == "__main__":
    unittest.main()
