from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "RAILWAY CORPORATION"
SUPPLIER_CODE = "RAILWAY"
MONTHS_EN = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}


@dataclass(frozen=True)
class RailwayInvoice:
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
    parser_name: str = "railway"
    parser_confidence: Decimal = Decimal("0.9970")

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
        }


def parse_railway_pdf(path: Path) -> RailwayInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_railway_text(text, original_filename=path.name)


def parse_railway_text(text: str, *, original_filename: str) -> RailwayInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "").replace("\x00", "-")
    invoice_number = _extract(normalized, r"Invoice number\s+([A-Z0-9-]+)")
    invoice_date = _parse_english_date(_extract(normalized, r"Date of issue\s+([A-Za-z]+ [0-9]{1,2}, [0-9]{4})"))
    ranges = re.findall(r"([A-Za-z]{3,9} [0-9]{1,2}, [0-9]{4})-([A-Za-z]{3,9} [0-9]{1,2}, [0-9]{4})", normalized)
    if ranges:
        starts = [_parse_english_date(start) for start, _ in ranges]
        ends = [_parse_english_date(end) for _, end in ranges]
        billing_period_start = min(starts)
        billing_period_end = max(ends)
    else:
        billing_period_start = date(invoice_date.year, invoice_date.month, 1)
        billing_period_end = invoice_date
    net_amount = _parse_money(_extract(normalized, r"Total excluding tax\s+\$([0-9,]+\.[0-9]{2})"))
    vat_amount = _parse_money(_extract(normalized, r"VAT - Spain\s*[- ]?\s*21%\s*on\s*\$[0-9,]+\.[0-9]{2}\s*[- ]?\s*\$([0-9,]+\.[0-9]{2})"))
    gross_amount = _parse_money(_extract(normalized, r"Amount due\s+\$([0-9,]+\.[0-9]{2})"))
    return RailwayInvoice(
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
        vat_percent=Decimal("21"),
        gross_amount=gross_amount,
        vat_amount=vat_amount,
        net_amount=net_amount,
        original_filename=original_filename,
        sender_email="billing@railway.com",
    )


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract Railway field with pattern: {pattern}")
    return match.group(1).strip()


def _parse_money(raw: str) -> Decimal:
    return Decimal(raw.replace(",", ""))


def _parse_english_date(raw: str) -> date:
    month_name, day_year = raw.split(" ", 1)
    day = int(day_year.split(",")[0])
    year = int(day_year.split(",")[1].strip())
    return date(year, MONTHS_EN[month_name.lower()], day)
