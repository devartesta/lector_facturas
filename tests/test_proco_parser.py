from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from lector_facturas.parsers.proco import parse_proco_text_and_summary


JAN_SAMPLE = """
31/01/2026
6032421
Accounts
Artesta Stores (UK) Ltd
Invoice
Precision Printing Co. Ltd
Ł964.48 1 01Print
Ł691.20 1 01Carriage
Ł563.85 1 01Postage
Ł3,092.21 1 01Direct Mailing
Ł697.50 1 01Storage
Ł7,211.09
Ł1201.85
Ł6,009.24
01 Ł6,009.24 Ł1,201.85 20.00
Subtotal VAT Total
"""

FEB_SAMPLE = """
28/02/2026
6033326
Accounts
Artesta Stores (UK) Ltd
Invoice
Precision Printing Co. Ltd
Ł1,235.84 1 01Print
Ł1,008.00 1 01Carriage
Ł662.30 1 01Postage
Ł2,737.96 1 01Direct Mailing
Ł620.00 1 01Storage
Ł7,516.92
Ł1252.82
Ł6,264.10
01 Ł6,264.10 Ł1,252.82 20.00
Subtotal VAT Total
"""

JAN_SUMMARY = {
    "storage_fee_total": Decimal("697.50"),
    "shipments_total": Decimal("1255.05"),
    "posters_total": Decimal("964.48"),
    "frames_total": Decimal("0.00"),
    "passpartout_total": Decimal("12.04"),
    "pick_pack_passpartout_total": Decimal("1.30"),
    "pick_pack_material_total": Decimal("3078.87"),
    "detail_total": Decimal("6009.24"),
    "manufacturing_total": Decimal("4754.19"),
    "period_start": date(2026, 1, 1),
    "period_end": date(2026, 1, 31),
}

FEB_SUMMARY = {
    "storage_fee_total": Decimal("620.00"),
    "shipments_total": Decimal("1670.30"),
    "posters_total": Decimal("1234.05"),
    "frames_total": Decimal("0.00"),
    "passpartout_total": Decimal("1.79"),
    "pick_pack_passpartout_total": Decimal("0.65"),
    "pick_pack_material_total": Decimal("2737.31"),
    "detail_total": Decimal("6264.10"),
    "manufacturing_total": Decimal("4593.80"),
    "period_start": date(2026, 2, 1),
    "period_end": date(2026, 2, 28),
}


class ProcoParserTests(unittest.TestCase):
    def test_parse_january_sample(self) -> None:
        invoice = parse_proco_text_and_summary(JAN_SAMPLE, detail_summary=JAN_SUMMARY, original_filename="SI6032421.pdf")

        self.assertEqual(invoice.supplier_code, "PROCO")
        self.assertEqual(invoice.invoice_number, "6032421")
        self.assertEqual(invoice.invoice_date.isoformat(), "2026-01-31")
        self.assertEqual(invoice.billing_period_start.isoformat(), "2026-01-01")
        self.assertEqual(invoice.billing_period_end.isoformat(), "2026-01-31")
        self.assertEqual(invoice.period_yyyymm, "202601")
        self.assertEqual(invoice.currency_code, "GBP")
        self.assertEqual(invoice.vat_percent, Decimal("20.00"))
        self.assertEqual(invoice.net_amount, Decimal("6009.24"))
        self.assertEqual(invoice.vat_amount, Decimal("1201.85"))
        self.assertEqual(invoice.gross_amount, Decimal("7211.09"))
        self.assertEqual(invoice.manufacturing_net_amount, Decimal("4754.19"))
        self.assertEqual(invoice.logistics_net_amount, Decimal("1255.05"))
        self.assertEqual(invoice.manufacturing_vat_amount, Decimal("950.84"))
        self.assertEqual(invoice.logistics_vat_amount, Decimal("251.01"))

    def test_parse_february_sample(self) -> None:
        invoice = parse_proco_text_and_summary(FEB_SAMPLE, detail_summary=FEB_SUMMARY, original_filename="IN6033326_001.pdf")

        self.assertEqual(invoice.invoice_number, "6033326")
        self.assertEqual(invoice.invoice_date.isoformat(), "2026-02-28")
        self.assertEqual(invoice.period_yyyymm, "202602")
        self.assertEqual(invoice.net_amount, Decimal("6264.10"))
        self.assertEqual(invoice.manufacturing_net_amount, Decimal("4593.80"))
        self.assertEqual(invoice.logistics_net_amount, Decimal("1670.30"))
        self.assertEqual(invoice.manufacturing_vat_amount, Decimal("918.76"))
        self.assertEqual(invoice.logistics_vat_amount, Decimal("334.06"))
        self.assertEqual(invoice.manufacturing_gross_amount, Decimal("5512.56"))
        self.assertEqual(invoice.logistics_gross_amount, Decimal("2004.36"))


if __name__ == "__main__":
    unittest.main()
