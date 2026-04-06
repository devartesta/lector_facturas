from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "SYNCWITH INC"
SUPPLIER_CODE = "SYNCWITH"
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
class SyncWithInvoice:
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
    parser_name: str = "syncwith"
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
            "tax_label": "REVERSE_CHARGE",
        }


def parse_syncwith_pdf(path: Path) -> SyncWithInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_syncwith_text(text, original_filename=path.name)


def parse_syncwith_text(text: str, *, original_filename: str) -> SyncWithInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "").replace("\x00", " ")
    invoice_number = _extract(normalized, r"Invoice number\s+([A-Z0-9 -]+)").replace(" ", "-")
    invoice_date = _parse_english_date(_extract(normalized, r"Date of issue\s+([A-Za-z]+ [0-9]{1,2}, [0-9]{4})"))
    gross_amount = _parse_money(_extract(normalized, r"Amount due\s+\$([0-9,]+\.[0-9]{2})"))
    previous_month_start, previous_month_end = _previous_month_bounds(invoice_date)
    return SyncWithInvoice(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_CODE,
        issuer_company_name=ISSUER_COMPANY_NAME,
        billed_company_name=COMPANY_NAME,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=previous_month_start,
        billing_period_end=previous_month_end,
        period_yyyymm=previous_month_start.strftime("%Y%m"),
        currency_code="USD",
        vat_percent=Decimal("0"),
        gross_amount=gross_amount,
        vat_amount=Decimal("0"),
        net_amount=gross_amount,
        original_filename=original_filename,
        sender_email="hello@syncwith.com",
    )


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract SyncWith field with pattern: {pattern}")
    return match.group(1).strip()


def _parse_money(raw: str) -> Decimal:
    return Decimal(raw.replace(",", ""))


def _parse_english_date(raw: str) -> date:
    month_name, day_year = raw.split(" ", 1)
    day = int(day_year.split(",")[0])
    year = int(day_year.split(",")[1].strip())
    return date(year, MONTHS_EN[month_name.lower()], day)


def _previous_month_bounds(invoice_date: date) -> tuple[date, date]:
    previous_month_last_day = invoice_date.replace(day=1) - timedelta(days=1)
    previous_month_start = previous_month_last_day.replace(day=1)
    return previous_month_start, previous_month_last_day
