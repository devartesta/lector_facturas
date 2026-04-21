from __future__ import annotations

from decimal import Decimal
from textwrap import dedent
import unittest

from lector_facturas.parsers.ups import parse_ups_text


REGULAR_SAMPLE = dedent(
    """
    Sie erreichen uns per E-Mail: defcr@ups.com
    Übersicht
    Rechnungsdatum
    07.Januar 2026
    Kundennr.:
    Rechnungsnr.:
    A055C1
    326821409
    ARTESTA STORE  S.L.
    Diese Seite enthält eine Übersicht über Ihre
    Versandaktivitäten bis einschließlich
    03.Januar 2026.
    29.Dez 1ZA055C16801299424
    30.Dez 1ZA055C16805262205
    02.Jan 1ZA055C16899999999
    MwSt.-frei 204,42
    Fälliger Gesamtbetrag EUR 204,42
    Art.196 - Dir 2006/112/EC Steuerschuldnerschaft des
    Leistungsempfängers
    """
)

IMPORT_SAMPLE = dedent(
    """
    Wenn Sie Fragen zur Deutschen Importzollabfertigung
    oder dem Steuerbescheid haben, wenden Sie sich bitte
    an:
    importinfo@ups.com
    Bei allgemeinen Fragen zu dieser Rechnung, wenden Sie
    sich bitte an:
    rechnungswesen@ups.com
    United Parcel Service Deutschland S.à r.l. & Co. OHG
    Rechnungsdatum
    05.Januar 2026
    Kundennr.:
    Rechnungsnr.:
    A055C1
    837858124
    ARTESTA STORE  S.L.
    Diese Seite enthält eine Übersicht über Ihre
    Versandaktivitäten bis einschließlich
    03.Januar 2026.
    23.Dez 1ZA055C16889196451
    23.Dez 1ZA055C16889244809
    MwSt.-frei 57,25
    Fälliger Gesamtbetrag EUR 57,25
    Art.196 - Dir 2006/112/EC Steuerschuldnerschaft des
    Leistungsempfängers
    """
)


class UpsParserTests(unittest.TestCase):
    def test_parse_regular_ups_invoice(self) -> None:
        parsed = parse_ups_text(REGULAR_SAMPLE, original_filename="Invoice_326821409_010726.PDF")
        self.assertEqual(parsed.supplier_code, "UPS")
        self.assertEqual(parsed.invoice_number, "326821409")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-01-07")
        self.assertEqual(parsed.billing_period_start.isoformat(), "2025-12-29")
        self.assertEqual(parsed.billing_period_end.isoformat(), "2026-01-03")
        self.assertEqual(parsed.period_yyyymm, "202601")
        self.assertEqual(parsed.gross_amount, Decimal("204.42"))
        self.assertEqual(parsed.net_amount, Decimal("204.42"))
        self.assertEqual(parsed.vat_amount, Decimal("0.00"))
        self.assertEqual(parsed.sender_email, "defcr@ups.com")

    def test_parse_import_ups_invoice(self) -> None:
        parsed = parse_ups_text(IMPORT_SAMPLE, original_filename="Invoice_837858124_010526.PDF")
        self.assertEqual(parsed.invoice_number, "837858124")
        self.assertEqual(parsed.invoice_date.isoformat(), "2026-01-05")
        self.assertEqual(parsed.billing_period_start.isoformat(), "2025-12-23")
        self.assertEqual(parsed.billing_period_end.isoformat(), "2026-01-03")
        self.assertEqual(parsed.period_yyyymm, "202601")
        self.assertEqual(parsed.gross_amount, Decimal("57.25"))
        self.assertEqual(parsed.sender_email, "rechnungswesen@ups.com")


if __name__ == "__main__":
    unittest.main()
