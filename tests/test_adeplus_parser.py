from __future__ import annotations

from decimal import Decimal

from lector_facturas.parsers.adeplus import parse_adeplus_text


ADEPLUS_SAMPLE = """
Cantidad
Concepto
Importe
Unitario
Subtotal
Tipo
IVA/IGIC
Cuota
IVA/IGIC
1
SERVICIOS INTEGRALES EN PROTECCIÓN DE DATOS
309,02€
309,02€
21
373,91€
PERIODO FACTURA: Febrero 2026 - Enero 2027
ID de la oferta: 240740
Observaciones
Base imponible sujeta a 21%
309,02€
Varios con IVA 21 %
64,89€
Base imponible exenta (*)
0,00€
Total Factura
373,91€
Adeplus Consultores, S.L.U.
GIRO
01/02/2026
ES172038666181600018****
IFC2602-00689
01/02/2026
ARTESTA STORE S.L.
Fecha Factura:
N.º Factura:
Fecha Vencimiento:
Forma Pago:
IBAN:
DATOS FACTURA
Av. de la Innovación, 2
06006 BADAJOZ
ADEPLUS CONSULTORES, S.L.U.
B06537609
151204
N.º Cliente:
"""


def test_parse_adeplus_text() -> None:
    parsed = parse_adeplus_text(ADEPLUS_SAMPLE, original_filename="Factura-5143539.pdf")

    assert parsed.supplier_code == "ADEPLUS"
    assert parsed.issuer_company_name == "ADEPLUS CONSULTORES, S.L.U."
    assert parsed.invoice_number == "IFC2602-00689"
    assert parsed.invoice_date.isoformat() == "2026-02-01"
    assert parsed.billing_period_start.isoformat() == "2026-02-01"
    assert parsed.billing_period_end.isoformat() == "2027-01-01"
    assert parsed.period_yyyymm == "202602"
    assert parsed.net_amount == Decimal("309.02")
    assert parsed.vat_amount == Decimal("64.89")
    assert parsed.gross_amount == Decimal("373.91")
    assert parsed.currency_code == "EUR"
