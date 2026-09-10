from datetime import date
from decimal import Decimal

from lector_facturas.invoice_ingestion import detect_parser_rule
from lector_facturas.parsers.caixarenting import parse_caixarenting_text


CAIXARENTING_INVOICE = """
ARTESTA STORE S.L.
info@caixarenting-auto.es
Periodo del 15/06/2026 al 30/06/2026
Factura N�: 2600305999
Fecha factura: 22/06/2026
TOTAL GENERAL (Euros) 186,67 39,20 225,87
Servicio de Renting
"""


def test_parse_caixarenting_invoice() -> None:
    parsed = parse_caixarenting_text(CAIXARENTING_INVOICE, original_filename="renting.pdf")
    assert parsed.supplier_code == "CAIXARENTING"
    assert parsed.invoice_number == "2600305999"
    assert parsed.invoice_date == date(2026, 6, 22)
    assert parsed.billing_period_start == date(2026, 6, 15)
    assert parsed.period_yyyymm == "202606"
    assert parsed.net_amount == Decimal("186.67")
    assert parsed.vat_amount == Decimal("39.20")
    assert parsed.gross_amount == Decimal("225.87")


def test_caixarenting_detection() -> None:
    rule = detect_parser_rule(
        filename="20260622FacturadeRentingdevehC3ADculos700010003140.pdf",
        sender_email="info@caixarenting-auto.es",
        subject="Factura de renting",
        pdf_text=CAIXARENTING_INVOICE,
    )
    assert rule is not None
    assert rule.supplier_code == "CAIXARENTING"
