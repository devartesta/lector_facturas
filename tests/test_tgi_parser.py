from __future__ import annotations

from decimal import Decimal
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from lector_facturas.parsers.tgi import parse_tgi_text


JAN_PRODUCTION_SAMPLE = """
Artesta,Inc
Today's Graphics Inc
Invoice
Invoice Date
Terms
Cust P.O. #
171197
1/31/26
Net 45 Days
Quantity Description Amount
$1,562.55Production January 1st-31st 20261
Artesta,Inc
"""

JAN_SHIPPING_SAMPLE = """
Artesta,Inc
Today's Graphics Inc
Invoice
Invoice Date
Terms
Cust P.O. #
171198
1/31/26
Net 10 Days
Quantity Description Amount
$2,888.86Shipping Charge's January 1st-31st 20261
Artesta,Inc
"""

FEB_SHIPPING_SAMPLE = """
Artesta,Inc
Today's Graphics Inc
Invoice
Invoice Date
Terms
Cust P.O. #
171420
2/28/26
Net 45 Days
Quantity Description Amount
$4,445.62February 1st-28th Shipping Charges1
Artesta,Inc
"""


class TgiParserTests(unittest.TestCase):
    def test_parse_january_production(self) -> None:
        invoice = parse_tgi_text(JAN_PRODUCTION_SAMPLE, original_filename="171197.pdf")
        self.assertEqual(invoice.invoice_number, "171197")
        self.assertEqual(invoice.invoice_date.isoformat(), "2026-01-31")
        self.assertEqual(invoice.division_invoice, "manufacturing")
        self.assertEqual(invoice.billing_period_start.isoformat(), "2026-01-01")
        self.assertEqual(invoice.billing_period_end.isoformat(), "2026-01-31")
        self.assertEqual(invoice.period_yyyymm, "202601")
        self.assertEqual(invoice.currency_code, "USD")
        self.assertEqual(invoice.vat_percent, Decimal("0.00"))
        self.assertEqual(invoice.net_amount, Decimal("1562.55"))
        self.assertEqual(invoice.gross_amount, Decimal("1562.55"))

    def test_parse_january_shipping(self) -> None:
        invoice = parse_tgi_text(JAN_SHIPPING_SAMPLE, original_filename="171198.pdf")
        self.assertEqual(invoice.invoice_number, "171198")
        self.assertEqual(invoice.division_invoice, "logistics")
        self.assertEqual(invoice.net_amount, Decimal("2888.86"))

    def test_parse_february_shipping(self) -> None:
        invoice = parse_tgi_text(FEB_SHIPPING_SAMPLE, original_filename="171420.pdf")
        self.assertEqual(invoice.invoice_number, "171420")
        self.assertEqual(invoice.invoice_date.isoformat(), "2026-02-28")
        self.assertEqual(invoice.division_invoice, "logistics")
        self.assertEqual(invoice.billing_period_start.isoformat(), "2026-02-01")
        self.assertEqual(invoice.billing_period_end.isoformat(), "2026-02-28")
        self.assertEqual(invoice.net_amount, Decimal("4445.62"))


if __name__ == "__main__":
    unittest.main()
