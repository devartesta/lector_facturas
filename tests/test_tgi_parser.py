from __future__ import annotations

from decimal import Decimal

from lector_facturas.parsers.tgi import parse_tgi_text


JAN_PRODUCTION_SAMPLE = """
Artesta,Inc
Today's Graphics Inc
Invoice
Invoice Date
Terms
Cust P.O. #
171197
1/31/26
Net 45 Days
Quantity Description Amount
$1,562.55Production January 1st-31st 20261
Artesta,Inc
"""

JAN_SHIPPING_SAMPLE = """
Artesta,Inc
Today's Graphics Inc
Invoice
Invoice Date
Terms
Cust P.O. #
171198
1/31/26
Net 10 Days
Quantity Description Amount
$2,888.86Shipping Charge's January 1st-31st 20261
Artesta,Inc
"""

FEB_SHIPPING_SAMPLE = """
Artesta,Inc
Today's Graphics Inc
Invoice
Invoice Date
Terms
Cust P.O. #
171420
2/28/26
Net 45 Days
Quantity Description Amount
$4,445.62February 1st-28th Shipping Charges1
Artesta,Inc
"""

TGI_OCR_SAMPLE = """
Today's Graphics Inc 4848 Island Ave
Philadelphia, PA 19153
INVOICE
171663
Invoice #
Email: accounting@tginc.com
Invoice Date
3/31/26
Salesperson Rick Elfreth
Artesta,Inc
Adria Sebastia
10 West RD PMB 1055 Newtown, PA 18940
Terms
Contact Name Customer Job # Cust. P.O. #
Net 45 Days Adria Sebastia
TGI Job # 536112
Quantity Description Amount 1 Freight Charges (March 1-March 31st 2026) $4,826.94
Subtotal Sales Tax
$4,826.94 $0.00
Total Due $4,826.94
Customer Code : Invoice Number :
ARTESTA 171663
Invoice Date : 3/31/26
"""


def test_parse_january_production() -> None:
    invoice = parse_tgi_text(JAN_PRODUCTION_SAMPLE, original_filename="171197.pdf")
    assert invoice.invoice_number == "171197"
    assert invoice.invoice_date.isoformat() == "2026-01-31"
    assert invoice.division_invoice == "manufacturing"
    assert invoice.billing_period_start.isoformat() == "2026-01-01"
    assert invoice.billing_period_end.isoformat() == "2026-01-31"
    assert invoice.period_yyyymm == "202601"
    assert invoice.currency_code == "USD"
    assert invoice.vat_percent == Decimal("0.00")
    assert invoice.net_amount == Decimal("1562.55")
    assert invoice.gross_amount == Decimal("1562.55")


def test_parse_january_shipping() -> None:
    invoice = parse_tgi_text(JAN_SHIPPING_SAMPLE, original_filename="171198.pdf")
    assert invoice.invoice_number == "171198"
    assert invoice.division_invoice == "logistics"
    assert invoice.net_amount == Decimal("2888.86")


def test_parse_february_shipping() -> None:
    invoice = parse_tgi_text(FEB_SHIPPING_SAMPLE, original_filename="171420.pdf")
    assert invoice.invoice_number == "171420"
    assert invoice.invoice_date.isoformat() == "2026-02-28"
    assert invoice.division_invoice == "logistics"
    assert invoice.billing_period_start.isoformat() == "2026-02-01"
    assert invoice.billing_period_end.isoformat() == "2026-02-28"
    assert invoice.net_amount == Decimal("4445.62")


def test_parse_tgi_ocr_layout() -> None:
    parsed = parse_tgi_text(TGI_OCR_SAMPLE, original_filename="171663-Artesta.pdf")

    assert parsed.invoice_number == "171663"
    assert parsed.invoice_date.isoformat() == "2026-03-31"
    assert parsed.billing_period_start.isoformat() == "2026-03-01"
    assert parsed.billing_period_end.isoformat() == "2026-03-31"
    assert parsed.period_yyyymm == "202603"
    assert parsed.division_invoice == "logistics"
    assert parsed.gross_amount == Decimal("4826.94")
