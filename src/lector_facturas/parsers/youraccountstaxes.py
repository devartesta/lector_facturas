from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import calendar
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORES (UK) LTD"
ISSUER_COMPANY_NAME = "YOUR ACCOUNTS AND TAXES"
SUPPLIER_CODE = "YOURACCOUNTSTAXES"


@dataclass(frozen=True)
class YourAccountsAndTaxesInvoice:
    supplier_code: str
    supplier_name: str
    issuer_company_name: str
    billed_company_name: str
    invoice_number: str
    invoice_date: date
    billing_period_start: date
    billing_period_end: date
    period_yyyymm: str
    currency_code: str
    vat_percent: Decimal
    gross_amount: Decimal
    vat_amount: Decimal
    net_amount: Decimal
    original_filename: str
    sender_email: str
    parser_name: str = "youraccountstaxes"
    parser_confidence: Decimal = Decimal("0.9980")

    @property
    def extracted_raw(self) -> dict[str, object]:
        return {
            "issuer_company_name": self.issuer_company_name,
            "billed_company_name": self.billed_company_name,
            "billing_period_start": self.billing_period_start.isoformat(),
            "billing_period_end": self.billing_period_end.isoformat(),
            "period_yyyymm": self.period_yyyymm,
            "currency_code": self.currency_code,
            "vat_percent": format(self.vat_percent, "f"),
            "gross_amount": format(self.gross_amount, "f"),
            "vat_amount": format(self.vat_amount, "f"),
            "net_amount": format(self.net_amount, "f"),
            "sender_email": self.sender_email,
        }


def parse_youraccountstaxes_pdf(path: Path) -> YourAccountsAndTaxesInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_youraccountstaxes_text(text, original_filename=path.name)


def parse_youraccountstaxes_text(text: str, *, original_filename: str) -> YourAccountsAndTaxesInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_number = _extract(normalized, r"Invoice Number\s+([A-Z0-9-]+)")
    invoice_date = datetime.strptime(_extract(normalized, r"Invoice Date\s+([0-9]{1,2} [A-Za-z]{3} [0-9]{4})"), "%d %b %Y").date()
    year_match = re.search(r"year\s+ending\s+Dec\s+([0-9]{4})", normalized, flags=re.IGNORECASE)
    billing_year = int(year_match.group(1)) if year_match else invoice_date.year
    billing_period_start = date(billing_year, 1, 1)
    billing_period_end = date(billing_year, 12, 31)
    net_amount = _parse_decimal(_extract(normalized, r"Subtotal\s+([0-9,]+\.[0-9]{2})"))
    vat_amount = _parse_decimal(_extract(normalized, r"TOTAL\s+VAT\s+20%\s+([0-9,]+\.[0-9]{2})"))
    gross_amount = _parse_decimal(_extract(normalized, r"TOTAL GBP\s+([0-9,]+\.[0-9]{2})"))
    return YourAccountsAndTaxesInvoice(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_CODE,
        issuer_company_name=ISSUER_COMPANY_NAME,
        billed_company_name=COMPANY_NAME,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=billing_period_start,
        billing_period_end=billing_period_end,
        period_yyyymm=invoice_date.strftime("%Y%m"),
        currency_code="GBP",
        vat_percent=Decimal("20"),
        net_amount=net_amount,
        vat_amount=vat_amount,
        gross_amount=gross_amount,
        original_filename=original_filename,
        sender_email="",
    )


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract Your Accounts and Taxes field with pattern: {pattern}")
    return match.group(1).strip()


def _parse_decimal(raw: str) -> Decimal:
    return Decimal(raw.replace(",", ""))
