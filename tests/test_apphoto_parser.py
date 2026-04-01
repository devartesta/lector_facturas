from __future__ import annotations

from decimal import Decimal
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from lector_facturas.parsers.apphoto import parse_apphoto_text


INDUSTRIES_SAMPLE = """
A P PHOTO INDUSTRIES, S.L.
E-mail: apphoto@apphoto.es
ARTESTA STORE, S.L.
NUMERO FACTURA PAGINA CODIGO CLIENTE
1 - 2.675 ESB67503250 128/02/2026 Engracia8.173
D E S C R I P C I O N
Envio:PASSATGE SANT JAUME, 20De Fecha:23/02/26
Envio:PASSATGE SANT JAUME, 20De Fecha:23/02/26
BASE IMPONIBLETOTAL NETO TOTAL FACTURA%I.V.A.CUOTA I.V.A. EUR1.209,91 6.971,3721,005.761,465.761,46
"""

CANARIAS_SAMPLE = """
A P PHOTO CANARIAS, S.L.
E-mail: info@apphoto.net
ARTESTA STORE, S.L.
FECHA FACTURAESB67503250 127/02/202651.220 Mcabrera26 / 1 / 377
D E S C R I P C I O N
Envio:C/DOCTOR ALLART, 161 / 55.48502/02/26Albaran Numero:De Fecha:
Envio:C/DOCTOR ALLART, 161 / 55.55104/02/26Albaran Numero:De Fecha:
Envio:C/DOCTOR ALLART, 161 / 56.07024/02/26Albaran Numero:De Fecha:
CUOTA IGIC%IGICTOTAL NETO BASE IMPONIBLE TOTAL FACTURA7,00 EUR59,8459,84 64,034,19
"""


class ApphotoParserTests(unittest.TestCase):
    def test_parse_industries_sample(self) -> None:
        invoice = parse_apphoto_text(INDUSTRIES_SAMPLE, original_filename="2026-1-2675.PDF")

        self.assertEqual(invoice.supplier_code, "APPHOTOES")
        self.assertEqual(invoice.supplier_name, "APPHOTOES")
        self.assertEqual(invoice.issuer_company_name, "A P PHOTO INDUSTRIES, S.L.")
        self.assertEqual(invoice.invoice_number, "1-2675")
        self.assertEqual(invoice.invoice_date.isoformat(), "2026-02-28")
        self.assertEqual(invoice.billing_period_start.isoformat(), "2026-02-23")
        self.assertEqual(invoice.billing_period_end.isoformat(), "2026-02-23")
        self.assertEqual(invoice.period_yyyymm, "202602")
        self.assertEqual(invoice.vat_percent, Decimal("21.00"))
        self.assertEqual(invoice.net_amount, Decimal("5761.46"))
        self.assertEqual(invoice.vat_amount, Decimal("1209.91"))
        self.assertEqual(invoice.gross_amount, Decimal("6971.37"))

    def test_parse_canarias_sample(self) -> None:
        invoice = parse_apphoto_text(CANARIAS_SAMPLE, original_filename="fra.377.pdf")

        self.assertEqual(invoice.supplier_code, "APPHOTOCAN")
        self.assertEqual(invoice.supplier_name, "APPHOTOCAN")
        self.assertEqual(invoice.issuer_company_name, "A P PHOTO CANARIAS, S.L.")
        self.assertEqual(invoice.invoice_number, "26-1-377")
        self.assertEqual(invoice.invoice_date.isoformat(), "2026-02-27")
        self.assertEqual(invoice.billing_period_start.isoformat(), "2026-02-02")
        self.assertEqual(invoice.billing_period_end.isoformat(), "2026-02-24")
        self.assertEqual(invoice.period_yyyymm, "202602")
        self.assertEqual(invoice.vat_percent, Decimal("7.00"))
        self.assertEqual(invoice.net_amount, Decimal("59.84"))
        self.assertEqual(invoice.vat_amount, Decimal("4.19"))
        self.assertEqual(invoice.gross_amount, Decimal("64.03"))


if __name__ == "__main__":
    unittest.main()
