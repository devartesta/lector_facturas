from __future__ import annotations

import unittest

from lector_facturas.parsers.hannun import parse_hannun_text


OUTGOING_ORDERS = """Artesta Store, S.L.
04/02/2026
FACTURA
Número de factura: 2026-0008
Hannun SA
administracion@hannun.com
CONCEPTO UDS. BASE UD. BASE TOTAL % IVA IVA
COM/26-000032 1 78,98 € 78,98 € 21% 16,59 €
COM/26-000059 1 49,55 € 49,55 € 21% 10,41 €
Total Base Imponible: 1.419,70 €
Total IVA: 298,15 €
TOTAL: 1.717,85 €
"""

INCOMING_OFFICE = """-
Factura de venta:
Fecha de factura:
11/03/2026
HANNUN, S.A.
Artesta Store S.L.
VTA/26-010327FACTURA tienda@hannun.com
Cliente
Uso oficina BCN - Enero
Resumen
Base imponible
Importe IVA
Importe total
175,00
36,75
211,75
€
Total Base Imponible: 175,00 €
Total IVA: 36,75 €
TOTAL: 211,75 €
"""


class HannunParserTests(unittest.TestCase):
    def test_parse_outgoing_orders_invoice(self) -> None:
        parsed = parse_hannun_text(OUTGOING_ORDERS, original_filename="Factura_2026-0008.pdf")
        self.assertEqual(parsed.invoice_number, "2026-0008")
        self.assertEqual(parsed.issuer_company_name, "ARTESTA STORE, S.L.")
        self.assertEqual(parsed.billed_company_name, "HANNUN, S.A.")
        self.assertEqual(parsed.division_invoice, "orders")
        self.assertEqual(parsed.destination_path, "income/sales/marketplaces")
        self.assertEqual(parsed.period_yyyymm, "202602")

    def test_parse_incoming_office_invoice_with_forced_period(self) -> None:
        parsed = parse_hannun_text(INCOMING_OFFICE, original_filename="VTA26-010327.pdf", forced_period_yyyymm="202602")
        self.assertEqual(parsed.invoice_number, "VTA/26-010327")
        self.assertEqual(parsed.issuer_company_name, "HANNUN, S.A.")
        self.assertEqual(parsed.billed_company_name, "ARTESTA STORE, S.L.")
        self.assertEqual(parsed.division_invoice, "office")
        self.assertEqual(parsed.destination_path, "expenses/opex/administration")
        self.assertEqual(parsed.period_yyyymm, "202602")


if __name__ == "__main__":
    unittest.main()
