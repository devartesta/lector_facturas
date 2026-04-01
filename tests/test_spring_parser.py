from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.spring import parse_spring_text


SPRING_SAMPLE = dedent(
    """
    TRACKED - UNDELIVERABLE
    2.200
    1
    3.00
    I
    SUPLEMENTO ENERGETICO
    0.30
    I
    TOTAL SUJETO A IVA
    3.30
    IVA 21%
    0.69
    TOTAL FACTURA EUR
    3.99
    INTERNATIONAL MAIL (SPAIN) S.L.
    admin.es@spring-gds.com
    FACTURA CONSOLIDADA
    ARTESTA STORE SL
    Factura Núm.
    E2600764
    No.Cuenta
    110002125
    Fecha Factura
    Vencimiento
    NIF
    27/01/26
    03/02/26
    ESB67503250
    """
)


class SpringParserTests(unittest.TestCase):
    def test_parse_spring_invoice(self) -> None:
        parsed = parse_spring_text(SPRING_SAMPLE, original_filename="E2600764.pdf")
        self.assertEqual(parsed.supplier_code, "SPRINGGDS")
        self.assertEqual(parsed.invoice_number, "E2600764")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-01-27")
        self.assertEqual(parsed.period_yyyymm, "202601")
        self.assertEqual(parsed.net_amount, Decimal("3.30"))
        self.assertEqual(parsed.vat_amount, Decimal("0.69"))
        self.assertEqual(parsed.gross_amount, Decimal("3.99"))
        self.assertEqual(parsed.vat_percent, Decimal("21"))
        self.assertEqual(parsed.sender_email, "admin.es@spring-gds.com")


if __name__ == "__main__":
    unittest.main()
