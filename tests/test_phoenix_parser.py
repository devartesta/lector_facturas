from datetime import date
from decimal import Decimal
from textwrap import dedent

from lector_facturas.invoice_ingestion import detect_parser_rule
from lector_facturas.parsers.phoenix import parse_phoenix_text


PHOENIX_INVOICE = dedent(
    """
    Phoenix Maintenance, SL
    Factura
    Número # F260634
    Fecha 01/09/2026
    BASE IMPONIBLE 95,00
    IVA 21% 19,95
    TOTAL 114,95
    """
)


def test_parse_phoenix_invoice() -> None:
    parsed = parse_phoenix_text(PHOENIX_INVOICE, original_filename="F260634.pdf")
    assert parsed.supplier_code == "PHOENIX"
    assert parsed.invoice_number == "F260634"
    assert parsed.invoice_date == date(2026, 9, 1)
    assert parsed.period_yyyymm == "202609"
    assert parsed.net_amount == Decimal("95.00")
    assert parsed.vat_amount == Decimal("19.95")
    assert parsed.gross_amount == Decimal("114.95")


def test_phoenix_invoice_detection() -> None:
    rule = detect_parser_rule(
        filename="F260634.pdf",
        sender_email="info@equipdeservei.com",
        subject="Factura F260634",
        pdf_text=PHOENIX_INVOICE,
    )
    assert rule is not None
    assert rule.supplier_code == "PHOENIX"
