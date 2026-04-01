from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.continuum import parse_continuum_text


CONTINUUM_SAMPLE = dedent(
    """
    INVOICE
    Continuum Advisory LLC
    Bill to
    Artesta Inc
    Invoice details
    Invoice no.: 1220
    Terms: Due on receipt
    Invoice date: 01/01/2026
    Due date: 01/01/2026
    1. Monthly Retainer Service Fee Accounting & taxes 1 $1,150.00 $1,150.00
    Total $1,150.00
    """
)


class ContinuumParserTests(unittest.TestCase):
    def test_parse_invoice(self) -> None:
        parsed = parse_continuum_text(CONTINUUM_SAMPLE, original_filename="INVOICE_1220.pdf")
        self.assertEqual(parsed.supplier_code, "CONTINUUM")
        self.assertEqual(parsed.invoice_number, "1220")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-01-01")
        self.assertEqual(parsed.period_yyyymm, "202601")
        self.assertEqual(parsed.currency_code, "USD")
        self.assertEqual(parsed.net_amount, Decimal("1150.00"))
        self.assertEqual(parsed.vat_amount, Decimal("0"))
        self.assertEqual(parsed.gross_amount, Decimal("1150.00"))


if __name__ == "__main__":
    unittest.main()
