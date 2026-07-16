from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.simplycom import parse_simplycom_text


SAMPLE = dedent(
    """
    Factura 4727397
    Artesta Store S.L.
    Simply.com A/S
    Fecha 2026-07-01T11:36:43+00:00
    Número de factura. 4727397
    Moneda EUR
    Referencia artesta.dk
    1 año Registro de artesta.dk
    2026-07-01 - 2027-07-01 10,56 € 10,56 €
    1 stk Costes para selección de tarjeta 0,01 € 0,01 €
    I alt excl. IVA 0,54 €
    IVA 0 €
    I alt incl. IVA 0,54 €
    The sale is subject to reverse charge.
    """
)


class SimplyComParserTests(unittest.TestCase):
    def test_parse(self) -> None:
        parsed = parse_simplycom_text(SAMPLE, original_filename="simplycom-4727397.pdf")
        self.assertEqual(parsed.invoice_number, "4727397")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-07-01")
        self.assertEqual(parsed.billing_period_start.isoformat(), "2026-07-01")
        self.assertEqual(parsed.billing_period_end.isoformat(), "2027-07-01")
        self.assertEqual(parsed.period_yyyymm, "202607")
        self.assertEqual(parsed.net_amount, Decimal("0.54"))
        self.assertEqual(parsed.vat_amount, Decimal("0"))
        self.assertEqual(parsed.gross_amount, Decimal("0.54"))
        self.assertEqual(parsed.vat_percent, Decimal("0"))


if __name__ == "__main__":
    unittest.main()
