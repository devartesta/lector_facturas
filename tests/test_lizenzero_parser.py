from __future__ import annotations

from decimal import Decimal

from lector_facturas.parsers.lizenzero import parse_lizenzero_text


LIZENZERO_SAMPLE = """
Artesta Store, S.L.
Andrea Guerrero Sulaibi
Passatge Sant Jaume, 20
08035 Barcelona
Spanien
kontakt@lizenzero.de
www.lizenzero.de
Ihre Kunden-Nr.: 90698
Ihre USt-IdNr.: ESB67503250
Ihre Bestellnr.: 859325
Datum: 03.03.2026
Leistungszeitraum: 01.01.2026 -
31.12.2026
Verpackungslizenz fuer Verkaufsverpackungen beim dualen
System Interzero Recycling Alliance
Rechnung Nr. 465344    |   Transaktions-Nr. 1468579538 bei Zahlung angeben
Pos. Art-Nr. Bezeichnung Vertragsnummer
1 LZV-2026 Automatische Vertragsverlaengerung
75222  100,88 €
Gesamtkosten Netto: 100,88 €
zzgl. 0 % MwSt.: 0,00 €
Gesamtkosten: 100,88 €
Hinweis: Der Empfaenger der Leistung schuldet die Steuer.
"""


def test_parse_lizenzero_text() -> None:
    parsed = parse_lizenzero_text(LIZENZERO_SAMPLE, original_filename="Rechnung.pdf")

    assert parsed.supplier_code == "LIZENZERO"
    assert parsed.issuer_company_name == "INTERZERO RECYCLING ALLIANCE GMBH"
    assert parsed.invoice_number == "465344"
    assert parsed.invoice_date.isoformat() == "2026-03-03"
    assert parsed.billing_period_start.isoformat() == "2026-01-01"
    assert parsed.billing_period_end.isoformat() == "2026-12-31"
    assert parsed.period_yyyymm == "202603"
    assert parsed.net_amount == Decimal("100.88")
    assert parsed.vat_amount == Decimal("0.00")
    assert parsed.gross_amount == Decimal("100.88")
    assert parsed.currency_code == "EUR"
