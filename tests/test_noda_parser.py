from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.noda import parse_noda_text


NODA_SAMPLE = dedent(
    """
    ASESORIA FISCAL NODA Y ASOCIADOS, S.L.
    ARTESTA STORE, S.L.
    F A C T U R A  Nº 0 20 /26
    En Santa Cruz de Tenerife a, 20 de Enero de 2.026
    Factura en concepto de honorarios profesionales por el servicio de Asesoría Contable,
    Fiscal y de Organización Administrativa, correspondiente al período trimestral de Octubre a
    Diciembre  de 2.025.
    HONORARIOS 170,00 €
    + 7% I.G.I.C. 11,90 €
    TOTAL 181,90 €
    """
)


class NodaParserTests(unittest.TestCase):
    def test_parse_noda_invoice(self) -> None:
        parsed = parse_noda_text(NODA_SAMPLE, original_filename="FACTURA Enero 2026.pdf")
        self.assertEqual(parsed.supplier_code, "NODA")
        self.assertEqual(parsed.invoice_number, "020/26")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-01-20")
        self.assertEqual(parsed.billing_period_start.isoformat(), "2025-10-01")
        self.assertEqual(parsed.billing_period_end.isoformat(), "2025-12-31")
        self.assertEqual(parsed.period_yyyymm, "202512")
        self.assertEqual(parsed.net_amount, Decimal("170.00"))
        self.assertEqual(parsed.vat_amount, Decimal("11.90"))
        self.assertEqual(parsed.gross_amount, Decimal("181.90"))
        self.assertEqual(parsed.vat_percent, Decimal("7"))


if __name__ == "__main__":
    unittest.main()
