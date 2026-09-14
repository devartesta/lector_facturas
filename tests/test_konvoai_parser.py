from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.konvoai import parse_konvoai_text


SAMPLE = dedent(
    """
    Número de factura B5F7DF3C-6940
    Fecha de emisión 14 de enero de 2026
    Scale Subscription
    14 ene 2026 – 14 feb 2026
    Subtotal 339,00 €
    Total 339,00 €
    Importe adeudado 339,00 €
    """
)


class KonvoParserTests(unittest.TestCase):
    def test_parse(self) -> None:
        parsed = parse_konvoai_text(SAMPLE, original_filename="konvo.pdf")
        self.assertEqual(parsed.invoice_number, "B5F7DF3C-6940")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-01-14")
        self.assertEqual(parsed.billing_period_start.isoformat(), "2026-01-14")
        self.assertEqual(parsed.billing_period_end.isoformat(), "2026-02-14")
        self.assertEqual(parsed.net_amount, Decimal("339.00"))

    def test_net_amount_uses_final_amount_after_discount(self) -> None:
        sample = dedent(
            """
            Número de factura B5F7DF3C-9306
            Fecha de emisión 14 de agosto de 2026
            Subtotal 429,00 €
            CoArchitect - 3M 100% FREE (249,00 € de descuento)
            Total 180,00 €
            Importe adeudado 180,00 €
            """
        )

        parsed = parse_konvoai_text(sample, original_filename="konvo.pdf")

        self.assertEqual(parsed.gross_amount, Decimal("180.00"))
        self.assertEqual(parsed.net_amount, Decimal("180.00"))

    def test_parse_service_period_with_four_letter_september(self) -> None:
        sample = dedent(
            """
            Número de factura B5F7DF3C-9306
            Fecha de emisión 14 de agosto de 2026
            Scale Subscription
            14 ago 2026 - 14 sept 2026
            Subtotal 180,00 €
            Total 180,00 €
            Importe adeudado 180,00 €
            """
        )

        parsed = parse_konvoai_text(sample, original_filename="konvo.pdf")

        self.assertEqual(parsed.billing_period_start.isoformat(), "2026-08-14")
        self.assertEqual(parsed.billing_period_end.isoformat(), "2026-09-14")


if __name__ == "__main__":
    unittest.main()
