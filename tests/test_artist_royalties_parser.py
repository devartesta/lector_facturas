from __future__ import annotations

import unittest

from lector_facturas.parsers.artist_royalties import parse_artist_royalties_summary_text, parse_artist_royalty_text


PDF_TEXT = """Billing information
Artesta Store, S.L.
Tax ID: ESB67502350
Passatge Sant Jaume, 20
08035 Barcelona
Spain
Date: 1/1/2026
Credit note number: 00102.1.2026
Period: 1/1/2026 - 31/1/2026
INVOICE
Description
Amount
Unit price
Total
Gross commission for sold artworks
5
3,00 €
15,00 €
Errors, changes and returns (artworks)
0
3,00 €
0,00 €
Stationery
0
1,50 €
0,00 €
Errors, changes and returns (stationery)
0
1,50 €
0,00 €
Affiliate Plan
-
3,00 €
0,00 €
Total
15,00 €
Reduced Withholding Tax (10%)
10%
1,50 €
Net amount
13,50 €
INVOICE AMOUNT
13,50 €
Issuer
Tessier Ashpool AB
Agavägen 52
181 55 Lidingö
Sweden
Tax ID / VAT ID: SE556778112401
Email:  tomas@bondecco.com
Payment details
Payment method: Bank transfer
IBAN: LT78 3250 0096 2550 3926
SWIFT/BIC: REVOLT21
ID: SE556778112401
"""

SUMMARY_TEXT = """
📊 Importe total a facturar en January 2026:

🌍 TOTAL GENERAL
   • Posters:                    13.737,00 €
   • Stationery:                 0,00 €
   • Total bruto:                13.737,00 €
   • Impuestos:                  414,45 € (3.0%)
   • Total neto:                 13.322,55 €
   • A pagar por PayPal:         2.320,14 €
   • A pagar por transferencia:  8.761,41 €
   • A pagar a 1x:               2.241,00 €

🇬🇧 Reino Unido (UK)
   • Posters:                    1.272,00 €
   • Stationery:                 0,00 €
   • Total bruto:                1.272,00 €
   • Impuestos:                  39,21 € (3.1%)
   • Total neto:                 1.232,79 €
   • A pagar por PayPal:         289,02 €
   • A pagar por transferencia:  778,77 €
   • A pagar a 1x:               165,00 €

🇺🇸 Estados Unidos (US)
   • Posters:                    471,00 €
   • Stationery:                 0,00 €
   • Total bruto:                471,00 €
   • Impuestos:                  10,41 € (2.2%)
   • Total neto:                 460,59 €
   • A pagar por PayPal:         70,02 €
   • A pagar por transferencia:  318,57 €
   • A pagar a 1x:               72,00 €

🇪🇺 Resto de Europa
   • Posters:                    11.994,00 €
   • Stationery:                 0,00 €
   • Total bruto:                11.994,00 €
   • Impuestos:                  364,83 € (3.0%)
   • Total neto:                 11.629,17 €
   • A pagar por PayPal:         1.961,10 €
   • A pagar por transferencia:  7.664,07 €
   • A pagar a 1x:               2.004,00 €
"""


class ArtistRoyaltiesParserTests(unittest.TestCase):
    def test_parse_artist_royalty_text(self) -> None:
        parsed = parse_artist_royalty_text(PDF_TEXT, original_filename="00102.1.2026.pdf")
        self.assertEqual(parsed.invoice_number, "102")
        self.assertEqual(parsed.credit_note_number, "00102.1.2026")
        self.assertEqual(parsed.period_yyyymm, "202601")
        self.assertEqual(parsed.artist_name, "Tessier Ashpool AB")
        self.assertEqual(parsed.artist_country, "Sweden")
        self.assertEqual(parsed.artist_region_code, "eu")
        self.assertEqual(parsed.payment_method, "Bank transfer")
        self.assertEqual(str(parsed.gross_amount), "15.00")
        self.assertEqual(str(parsed.withholding_amount), "1.50")
        self.assertEqual(str(parsed.net_amount), "13.50")

    def test_parse_artist_royalties_summary_text(self) -> None:
        summaries = parse_artist_royalties_summary_text(SUMMARY_TEXT, source_filename="resumen.txt")
        self.assertEqual(len(summaries), 4)
        total = next(item for item in summaries if item.summary_scope == "total")
        eu = next(item for item in summaries if item.summary_scope == "eu")
        self.assertEqual(total.period_yyyymm, "202601")
        self.assertEqual(str(total.gross_amount), "13737.00")
        self.assertEqual(str(total.bank_transfer_amount), "8761.41")
        self.assertEqual(str(eu.net_amount), "11629.17")


if __name__ == "__main__":
    unittest.main()
