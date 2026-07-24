from datetime import date
from decimal import Decimal
from textwrap import dedent

from lector_facturas.parsers.notarios_monte_esquinza import parse_notarios_monte_esquinza_text


NOTARY_INVOICE = dedent(
    """
    NOTARIOS MONTE ESQUINZA 6 CB
    ARTESTA STORE, S.L.U.
    N? Factura: L 1033
    Fecha Factura: 17/03/2026
    Base Exenta IVA Base Imponible Impuestos
    3,60 EUR 304,36 EUR IVA (21,00%) 63,92 EUR
    304,36 EUR RETENCION (15,00%) -45,65 EUR
    IMPORTE TOTAL 326,23 EUR
    """
)


def test_parse_notary_invoice_with_vat_and_withholding() -> None:
    parsed = parse_notarios_monte_esquinza_text(NOTARY_INVOICE, original_filename="L1033-2026.pdf")
    assert parsed.supplier_code == "NOTARIOSMONTESQUINZA"
    assert parsed.invoice_number == "L1033"
    assert parsed.invoice_date == date(2026, 3, 17)
    assert parsed.period_yyyymm == "202603"
    assert parsed.exempt_amount == Decimal("3.60")
    assert parsed.taxable_amount == Decimal("304.36")
    assert parsed.net_amount == Decimal("307.96")
    assert parsed.vat_amount == Decimal("63.92")
    assert parsed.gross_amount == Decimal("371.88")
    assert parsed.withholding_amount == Decimal("45.65")
    assert parsed.payable_amount == Decimal("326.23")


def test_parse_notary_exempt_invoice() -> None:
    text = NOTARY_INVOICE.replace(
        "N? Factura: L 1033\nFecha Factura: 17/03/2026",
        "N? Factura: I-L 7956\nFecha: 21/04/2026",
    ).replace(
        "3,60 EUR 304,36 EUR IVA (21,00%) 63,92 EUR\n304,36 EUR RETENCION (15,00%) -45,65 EUR\nIMPORTE TOTAL 326,23 EUR",
        "6,37 EUR 0,00 EUR IVA (21,00%) 0,00 EUR\n0,00 EUR RETENCION (15,00%) -0,00 EUR\nIMPORTE TOTAL 6,37 EUR",
    )
    parsed = parse_notarios_monte_esquinza_text(text, original_filename="I-L7956-2026.pdf")
    assert parsed.invoice_number == "I-L7956"
    assert parsed.period_yyyymm == "202604"
    assert parsed.net_amount == Decimal("6.37")
    assert parsed.vat_amount == Decimal("0.00")
    assert parsed.payable_amount == Decimal("6.37")
