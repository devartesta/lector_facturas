from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.rever import parse_rever_invoice_text, parse_rever_supplied_note_text


INVOICE_SAMPLE = dedent(
    """
    Número de factura RVR-16823
    Fecha de emisión 28 de enero de 2026
    REVER- Suscripción Mensual
    28 ene 2026 – 28 feb 2026
    Total sin impuestos 79,00 €
    IVA - España (21 % en 79,00 €) 16,59 €
    Total 95,59 €
    """
)

SUPPLIED_SAMPLE = dedent(
    """
    Billing Period: 01/01/2026 - 31/01/2026
    Supplied Expenses 7 €422,97 €0,00 €422,97
    """
)


class ReverParserTests(unittest.TestCase):
    def test_invoice_parse(self) -> None:
        parsed = parse_rever_invoice_text(INVOICE_SAMPLE, original_filename="Invoice-RVR-16823.pdf")
        self.assertEqual(parsed.document_type, "invoice")
        self.assertEqual(parsed.invoice_number, "RVR-16823")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-01-28")
        self.assertEqual(parsed.net_amount, Decimal("79.00"))
        self.assertEqual(parsed.vat_amount, Decimal("16.59"))
        self.assertEqual(parsed.gross_amount, Decimal("95.59"))

    def test_supplied_note_parse(self) -> None:
        parsed = parse_rever_supplied_note_text(SUPPLIED_SAMPLE, original_filename="suppliedNote.pdf")
        self.assertEqual(parsed.document_type, "supplied_note")
        self.assertEqual(parsed.invoice_number, "SUPPLIED-202601")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-01-31")
        self.assertEqual(parsed.net_amount, Decimal("422.97"))
        self.assertEqual(parsed.vat_amount, Decimal("0"))
        self.assertEqual(parsed.gross_amount, Decimal("422.97"))

    def test_invoice_with_supplied_expenses_tax_line_is_not_suplidos(self) -> None:
        sample = dedent(
            """
            Número de factura RVR-19979
            Fecha de emisión 1 de mayo de 2026
            REVER- Suscripción Mensual
            Total sin impuestos 268,81 €
            Supplied Expenses (0 % en -139,61 €) 0,00 €
            IVA (ESPAÑA) (21 % en 408,42 €) 85,78 €
            Total 354,59 €
            """
        )

        parsed = parse_rever_invoice_text(sample, original_filename="Invoice-RVR-19979.pdf")

        self.assertEqual(parsed.document_type, "invoice")
        self.assertEqual(parsed.division_invoice, "")
        self.assertEqual(parsed.invoice_number, "RVR-19979")
        self.assertEqual(parsed.net_amount, Decimal("268.81"))
        self.assertEqual(parsed.vat_amount, Decimal("85.78"))
        self.assertEqual(parsed.gross_amount, Decimal("354.59"))


if __name__ == "__main__":
    unittest.main()
