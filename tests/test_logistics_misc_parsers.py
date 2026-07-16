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
    Delivery Address: Artesta Store S.L.
    T O T A L EUR 570,00
    """
)

ARTLINK_LTD_SAMPLE = dedent(
    """
    FAKTURA INVOICE 000205098
    Date: 01-06-26
    Delivery Address: Precision Proco
    Artesta Stores Ltd
    Order no. 0003001629-1
    21623 Artesta White 20x30           10           10               36,50                3,65 4740100216234
    TOTAL            16525,55
    T O T A L EUR            16525,55
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

PORT_SAMPLE_WITH_VAT = dedent(
    """
    SALES INVOICE
    ARTESTA STORES LTD
    04.06.2026 000205098 PCSI2603240
    CUSTOMS CLEARANCE 45.00 GBP
    ADMIN FEE 3.50 GBP
    ENS FEE 3.50 GBP
    TOTAL 1.40 53.40 GBP
    """
)
class LogisticsMiscParserTests(unittest.TestCase):
    def test_parse_artlink(self) -> None:
        parsed = parse_artlink_text(ARTLINK_SAMPLE, original_filename="artlink.pdf")
        self.assertEqual(parsed.invoice_number, "000203570")
        self.assertEqual(parsed.gross_amount, Decimal("570.00"))
        self.assertEqual(parsed.billed_company_name, "ARTESTA STORE, S.L.")
        self.assertEqual(parsed.division_invoice, "manufacturing")

    def test_parse_artlink_ltd_stock_invoice(self) -> None:
        parsed = parse_artlink_text(ARTLINK_LTD_SAMPLE, original_filename="artlink-ltd.pdf")
        self.assertEqual(parsed.invoice_number, "000205098")
        self.assertEqual(parsed.billed_company_name, "ARTESTA STORES (UK) LTD")
        self.assertEqual(parsed.division_invoice, "manufacturing")
        self.assertEqual(parsed.gross_amount, Decimal("16525.55"))

    def test_parse_portclearance(self) -> None:
        parsed = parse_portclearance_text(PORT_SAMPLE, original_filename="pcs.pdf")
        self.assertEqual(parsed.invoice_number, "PCSI2601529")
        self.assertEqual(parsed.currency_code, "GBP")
        self.assertEqual(parsed.gross_amount, Decimal("52.00"))

    def test_parse_portclearance_with_vat(self) -> None:
        parsed = parse_portclearance_text(PORT_SAMPLE_WITH_VAT, original_filename="pcs-vat.pdf")
        self.assertEqual(parsed.invoice_number, "PCSI2603240")
        self.assertEqual(parsed.net_amount, Decimal("52.00"))
        self.assertEqual(parsed.vat_amount, Decimal("1.40"))
        self.assertEqual(parsed.gross_amount, Decimal("53.40"))


if __name__ == "__main__":
    unittest.main()
