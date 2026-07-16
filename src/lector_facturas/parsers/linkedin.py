from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "LINKEDIN IRELAND UNLIMITED COMPANY"
SUPPLIER_CODE = "LINKEDIN"
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
}


@dataclass(frozen=True)
class LinkedInInvoice:
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
    parser_name: str = "linkedin"
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
            "service_type": "linkedin_job_views",
        }


def parse_linkedin_pdf(path: Path) -> LinkedInInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_linkedin_text(text, original_filename=path.name)


def parse_linkedin_text(text: str, *, original_filename: str) -> LinkedInInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_number = _extract(normalized, r"Invoice Number\s+([0-9]+)")
    invoice_date = _parse_short_us_date(_extract(normalized, r"Effective Date\s+([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})"))
    amount = _parse_money(_extract(normalized, r"Amount\s+€([0-9]+\.[0-9]{2})"))
    billed_company_name = COMPANY_NAME
    period_match = re.search(
        r"From\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+([0-9]{1,2}),\s+([0-9]{4})\s+to\s+"
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+([0-9]{1,2}),\s+([0-9]{4})",
        normalized,
        flags=re.IGNORECASE,
    )
    if period_match:
        billing_period_start = date(int(period_match.group(3)), MONTHS_EN[period_match.group(1).lower()], int(period_match.group(2)))
        billing_period_end = date(int(period_match.group(6)), MONTHS_EN[period_match.group(4).lower()], int(period_match.group(5)))
    else:
        billing_period_start = invoice_date
        billing_period_end = invoice_date
    return LinkedInInvoice(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_CODE,
        issuer_company_name=ISSUER_COMPANY_NAME,
        billed_company_name=billed_company_name,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=billing_period_start,
        billing_period_end=billing_period_end,
        period_yyyymm=billing_period_end.strftime("%Y%m"),
        currency_code="EUR",
        vat_percent=Decimal("0.00"),
        gross_amount=amount,
        vat_amount=Decimal("0.00"),
        net_amount=amount,
        original_filename=original_filename,
        sender_email="",
    )


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract LinkedIn field with pattern: {pattern}")
    return match.group(1).strip()


def _parse_short_us_date(raw: str) -> date:
    month, day, year = raw.split("/")
    return date(int(year), int(month), int(day))


def _parse_money(raw: str) -> Decimal:
    return Decimal(raw.replace(",", ""))
