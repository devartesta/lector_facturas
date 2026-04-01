from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.artlink import parse_artlink_text
from lector_facturas.parsers.portclearance import parse_portclearance_text


ARTLINK_SAMPLE = dedent(
    """
    FAKTURA INVOICE 000203570
    Date: 13-03-26
    T O T A L EUR 570,00
    """
)

PORT_SAMPLE = dedent(
    """
    SALES INVOICE
    ARTESTA STORES LTD
    16.03.2026 000203519 PCSI2601529
    TOTAL 0.00 52.00 GBP
    """
)
class LogisticsMiscParserTests(unittest.TestCase):
    def test_parse_artlink(self) -> None:
        parsed = parse_artlink_text(ARTLINK_SAMPLE, original_filename="artlink.pdf")
        self.assertEqual(parsed.invoice_number, "000203570")
        self.assertEqual(parsed.gross_amount, Decimal("570.00"))

    def test_parse_portclearance(self) -> None:
        parsed = parse_portclearance_text(PORT_SAMPLE, original_filename="pcs.pdf")
        self.assertEqual(parsed.invoice_number, "PCSI2601529")
        self.assertEqual(parsed.currency_code, "GBP")
        self.assertEqual(parsed.gross_amount, Decimal("52.00"))


if __name__ == "__main__":
    unittest.main()
