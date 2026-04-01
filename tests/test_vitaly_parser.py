from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.vitaly import parse_vitaly_text


VITALY_SAMPLE = dedent(
    """
    Base imponible sujeta a 21%
    369,00?
    Varios con IVA 21 %
    77,49?
    Total Factura
    446,49?
    GIRO
    04/03/2026
    IFC2603-18831
    04/03/2026
    ARTESTA STORE S.L.
    Fecha Factura:
    N.? Factura:
    VITALY HEALTH SERVICES, S.L.
    """
)


class VitalyParserTests(unittest.TestCase):
    def test_parse_vitaly_invoice(self) -> None:
        parsed = parse_vitaly_text(VITALY_SAMPLE, original_filename="FVFD_1_IFC2603-18831.pdf")
        self.assertEqual(parsed.supplier_code, "VITALY")
        self.assertEqual(parsed.invoice_number, "IFC2603-18831")
        self.assertEqual(parsed.period_yyyymm, "202603")
        self.assertEqual(parsed.billing_period_start.isoformat(), "2026-01-01")
        self.assertEqual(parsed.billing_period_end.isoformat(), "2026-12-31")
        self.assertEqual(parsed.net_amount, Decimal("369.00"))
        self.assertEqual(parsed.vat_amount, Decimal("77.49"))
        self.assertEqual(parsed.gross_amount, Decimal("446.49"))


if __name__ == "__main__":
    unittest.main()
