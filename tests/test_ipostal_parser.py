from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.ipostal import parse_ipostal_text


IPOSTAL_SAMPLE = dedent(
    """
    Factura #36427002
    Fecha de Transacción
    sáb mar 21, 2026 08:43 am
    Factura para
    Artesta Inc.
    Artesta Inc.
    18 Campus Blvd Suite 100
    Newtown Square, Pennsylvania 19073
    United States
    Identificaciones de Correo Propias
    M15552, M15553, M15735
    Pagadero a
    iPostal1
    10 West Road Newtown, PA 18940
    United States
    Producto
    1 x Storage
    Total
    Método de pago: Credit Card
    Estado: Paid
    Precio
    3,30 US$
    3,30 US$
    """
)


class IPostalParserTests(unittest.TestCase):
    def test_parse_ipostal_invoice(self) -> None:
        parsed = parse_ipostal_text(IPOSTAL_SAMPLE, original_filename="2026-03-21_36427002.pdf")
        self.assertEqual(parsed.supplier_code, "IPOSTAL")
        self.assertEqual(parsed.invoice_number, "36427002")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-03-21")
        self.assertEqual(parsed.period_yyyymm, "202603")
        self.assertEqual(parsed.currency_code, "USD")
        self.assertEqual(parsed.net_amount, Decimal("3.30"))
        self.assertEqual(parsed.vat_amount, Decimal("0"))
        self.assertEqual(parsed.gross_amount, Decimal("3.30"))


if __name__ == "__main__":
    unittest.main()
