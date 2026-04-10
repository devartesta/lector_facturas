from __future__ import annotations

from decimal import Decimal

from lector_facturas.parsers.contasimple import parse_contasimple_text


CONTASIMPLE_SAMPLE = """
Número de factura: ES-2026-11348
Fecha: 10/03/2026
CEGID SMB, S.A.U
NIF: A08147811
Calle Rozabella, nº8 - Centro Europa Empresarial. Edificio
Roma
28290 Las Rozas de Madrid
Madrid, España
Cliente:
ARTESTA STORE, S.L.
NIF: B67503250
Passatge Sant Jaume, 20
08035 Barcelona
Barcelona, España
Concepto Uds. Base Ud. Base Total % IVA IVA
Plan Ultimate.
Periodo contratado desde el 09/03/2026 al 09/03/2027
1 191,40 € 191,40 € 21% 40,19 €
Periodo de prestación del servicio. Desde: 09/03/2026 Hasta: 09/03/2027
Base Imponible Tipo impuesto Impuesto
191,40 € IVA 21% 40,19 €
Totales
Total B.I.: 191,40 €
Total IVA: 40,19 €
TOTAL: 231,59 €
"""


def test_parse_contasimple_text() -> None:
    parsed = parse_contasimple_text(CONTASIMPLE_SAMPLE, original_filename="Factura_ES-2026-11348.pdf")

    assert parsed.supplier_code == "CONTASIMPLE"
    assert parsed.issuer_company_name == "CEGID SMB, S.A.U"
    assert parsed.invoice_number == "ES-2026-11348"
    assert parsed.invoice_date.isoformat() == "2026-03-10"
    assert parsed.billing_period_start.isoformat() == "2026-03-09"
    assert parsed.billing_period_end.isoformat() == "2027-03-09"
    assert parsed.period_yyyymm == "202603"
    assert parsed.net_amount == Decimal("191.40")
    assert parsed.vat_amount == Decimal("40.19")
    assert parsed.gross_amount == Decimal("231.59")
    assert parsed.currency_code == "EUR"
