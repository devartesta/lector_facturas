from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA INC"
ISSUER_COMPANY_NAME = "INTUIT INC."
SUPPLIER_CODE = "QUICKBOOKS"

MONTHS_EN = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "sept": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


@dataclass(frozen=True)
class QuickBooksInvoice:
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
    parser_name: str = "quickbooks"
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


def parse_quickbooks_pdf(path: Path) -> QuickBooksInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages[:2])
    if not text.strip():
        raise ValueError("QuickBooks PDF requires OCR from ingestion workflow.")
    return parse_quickbooks_text(text, original_filename=path.name)


def parse_quickbooks_text(text: str, *, original_filename: str) -> QuickBooksInvoice:
    normalized = text.replace("\ufeff", "").replace("\r", "\n").replace("\xa0", " ")
    normalized = re.sub(r"[ \t]+", " ", normalized)
    normalized = re.sub(r"\n{2,}", "\n", normalized)

    invoice_number = _extract(normalized, r"Invoice number:\s*([0-9]+)")
    invoice_date = _parse_english_date(_extract(normalized, r"Date:\s*([A-Za-z]{3,9} [0-9]{1,2}, [0-9]{4})"))
    billing_period_start = _parse_english_date(
        _extract(normalized, r"Period for monthly fees:\s*([A-Za-z]{3,9} [0-9]{1,2}, [0-9]{4})")
    )
    billing_period_end = _parse_english_date(
        _extract(normalized, r"Period for monthly fees:\s*[A-Za-z]{3,9} [0-9]{1,2}, [0-9]{4}\s*-\s*([A-Za-z]{3,9} [0-9]{1,2}, [0-9]{4})")
    )
    net_amount = _parse_money(_extract(normalized, r"Total without tax:\s*\$([0-9,]+\.[0-9]{2})"))
    vat_amount = _parse_money(_extract(normalized, r"Total tax:\s*\$([0-9,]+\.[0-9]{2})"))
    gross_amount = _parse_money(_extract(normalized, r"Total invoice:\s*\$([0-9,]+\.[0-9]{2})"))
    vat_percent = _infer_vat_percent(net_amount=net_amount, vat_amount=vat_amount)

    return QuickBooksInvoice(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_CODE,
        issuer_company_name=ISSUER_COMPANY_NAME,
        billed_company_name=COMPANY_NAME,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=billing_period_start,
        billing_period_end=billing_period_end,
        period_yyyymm=billing_period_start.strftime("%Y%m"),
        currency_code="USD",
        vat_percent=vat_percent,
        gross_amount=gross_amount,
        vat_amount=vat_amount,
        net_amount=net_amount,
        original_filename=original_filename,
        sender_email="quickbooks@notification.intuit.com",
    )


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract QuickBooks field with pattern: {pattern}")
    return match.group(1).strip()


def _parse_money(raw: str) -> Decimal:
    return Decimal(raw.replace(",", ""))


def _parse_english_date(raw: str) -> date:
    month_name, day_year = raw.split(" ", 1)
    day = int(day_year.split(",")[0])
    year = int(day_year.split(",")[1].strip())
    return date(year, MONTHS_EN[month_name.lower()], day)


def _infer_vat_percent(*, net_amount: Decimal, vat_amount: Decimal) -> Decimal:
    if net_amount == 0:
        return Decimal("0")
    return (vat_amount / net_amount * Decimal("100")).quantize(Decimal("0.01"))
