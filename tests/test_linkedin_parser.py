from __future__ import annotations

from decimal import Decimal
from textwrap import dedent

from lector_facturas.parsers.linkedin import parse_linkedin_text


SAMPLE = dedent(
    """
    Invoice from LinkedIn Ireland Unlimited Company
    Effective Date
    5/12/2026
    Transaction ID
    P3016700053
    Invoice Number
    781241921881
    Purchaser Email
    raul@artestastore.com
    Amount
    €41.76
    Transaction Date
    5/12/2026
    Billing Frequency
    Threshold Billing
    Summary
    Item Description Rate Quantity Price
    1
    Job views
    Job Title: Especialista en Contabilidad y Finanzas — Barcelona
    From May 7, 2026 to May 12, 2026
    €41.76
    Subtotal :
    VAT : 0%
    Total :
    Payment :
    Balance :
    €41.76
    €0.00
    €41.76
    €41.76
    €0.00
    LinkedIn Ireland Unlimited Company, 5 Wilton Park, Dublin 2, Ireland
    VAT: IE9740425P
    """
)


def test_parse_linkedin_invoice() -> None:
    parsed = parse_linkedin_text(SAMPLE, original_filename="LNKD_INVOICE_781241921881.pdf")
    assert parsed.invoice_number == "781241921881"
    assert parsed.invoice_date.isoformat() == "2026-05-12"
    assert parsed.billing_period_start.isoformat() == "2026-05-07"
    assert parsed.billing_period_end.isoformat() == "2026-05-12"
    assert parsed.period_yyyymm == "202605"
    assert parsed.currency_code == "EUR"
    assert parsed.vat_percent == Decimal("0.00")
    assert parsed.net_amount == Decimal("41.76")
    assert parsed.vat_amount == Decimal("0.00")
    assert parsed.gross_amount == Decimal("41.76")
