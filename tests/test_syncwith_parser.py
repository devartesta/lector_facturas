from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.syncwith import parse_syncwith_text


SYNCWITH_SAMPLE = dedent(
    """
    Invoice
    Invoice number TADGCDFS 0003
    Date of issue April 6, 2026
    SyncWith Inc
    hello@syncwith.com
    Bill to
    dev@artestastore.com
    $24.99 USD due April 6, 2026
    Description Qty Unit price Amount
    Business
    Apr 6 May 6, 2026
    1 $24.99 $24.99
    Subtotal $24.99
    Total $24.99
    Amount due $24.99 USD
    """
)


class SyncWithParserTests(unittest.TestCase):
    def test_parse_syncwith_invoice_to_previous_month(self) -> None:
        parsed = parse_syncwith_text(SYNCWITH_SAMPLE, original_filename="Invoice-TADGCDFS-0003.pdf")
        self.assertEqual(parsed.invoice_number, "TADGCDFS-0003")
        self.assertEqual(parsed.period_yyyymm, "202603")
        self.assertEqual(parsed.billing_period_start.isoformat(), "2026-03-01")
        self.assertEqual(parsed.billing_period_end.isoformat(), "2026-03-31")
        self.assertEqual(parsed.currency_code, "USD")
        self.assertEqual(parsed.net_amount, Decimal("24.99"))
        self.assertEqual(parsed.vat_amount, Decimal("0"))


if __name__ == "__main__":
    unittest.main()
