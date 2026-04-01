from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.adobe import parse_adobe_text


ADOBE_SAMPLE = dedent(
    """
    Duración del servicio: 28-ENE-2026 a 27-FEB-2026
    Información de facturación
    IEE2026001813920Número de factura
    28-ENE-2026Fecha de la factura
    EURDivisa
    Adobe Systems Software Ireland Ltd
    IMPORTE NETO (EUR) 33.49
    IMPUESTOS (VER LOS TIPOS) 0.00
    IVA
    TOTAL (EUR) 33.49
    """
)


class AdobeParserTests(unittest.TestCase):
    def test_parse_invoice(self) -> None:
        parsed = parse_adobe_text(ADOBE_SAMPLE, original_filename="IEE2026001813920.pdf")
        self.assertEqual(parsed.supplier_code, "ADOBE")
        self.assertEqual(parsed.invoice_number, "IEE2026001813920")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-01-28")
        self.assertEqual(parsed.billing_period_start.isoformat(), "2026-01-28")
        self.assertEqual(parsed.billing_period_end.isoformat(), "2026-02-27")
        self.assertEqual(parsed.period_yyyymm, "202601")
        self.assertEqual(parsed.net_amount, Decimal("33.49"))
        self.assertEqual(parsed.vat_amount, Decimal("0"))
        self.assertEqual(parsed.gross_amount, Decimal("33.49"))


if __name__ == "__main__":
    unittest.main()
