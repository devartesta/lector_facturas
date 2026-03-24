from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA INC"
ISSUER_COMPANY_NAME = "IPOSTAL1"
SUPPLIER_CODE = "IPOSTAL"

MONTHS_ES = {
    "ene": 1,
    "feb": 2,
    "mar": 3,
    "abr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "sep": 9,
    "sept": 9,
    "oct": 10,
    "nov": 11,
    "dic": 12,
}


@dataclass(frozen=True)
class IPostalInvoice:
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
    parser_name: str = "ipostal"
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
            "ocr_source": "google_drive_docs",
        }


def parse_ipostal_pdf(path: Path) -> IPostalInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages[:2])
    if not text.strip():
        raise ValueError("iPostal scanned PDF requires OCR from ingestion workflow.")
    return parse_ipostal_text(text, original_filename=path.name)


def parse_ipostal_text(text: str, *, original_filename: str) -> IPostalInvoice:
    normalized = text.replace("\ufeff", "").replace("\r", "\n").replace("\xa0", " ")
    normalized = re.sub(r"[ \t]+", " ", normalized)
    normalized = re.sub(r"\n{2,}", "\n", normalized)

    invoice_number = _extract(normalized, r"Factura\s*#\s*([0-9]+)")
    invoice_date = _extract_transaction_date(normalized)
    gross_amount = _extract_total_amount(normalized)

    return IPostalInvoice(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_CODE,
        issuer_company_name=ISSUER_COMPANY_NAME,
        billed_company_name=COMPANY_NAME,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=invoice_date,
        billing_period_end=invoice_date,
        period_yyyymm=invoice_date.strftime("%Y%m"),
        currency_code="USD",
        vat_percent=Decimal("0"),
        gross_amount=gross_amount,
        vat_amount=Decimal("0"),
        net_amount=gross_amount,
        original_filename=original_filename,
        sender_email="support@ipostal1.com",
    )


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract iPostal field with pattern: {pattern}")
    return match.group(1).strip()


def _extract_transaction_date(text: str) -> date:
    match = re.search(
        r"(?:lun|mar|mi[eé]|jue|vie|s[aá]b|dom)\s+([a-z]{3,4})\s+([0-9]{1,2}),\s*([0-9]{4})\s*([0-9]{1,2}:[0-9]{2})\s*(am|pm)",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        raise ValueError("Could not extract iPostal transaction date.")
    month = MONTHS_ES[match.group(1).lower()]
    day = int(match.group(2))
    year = int(match.group(3))
    hour_minute = match.group(4)
    meridiem = match.group(5).upper()
    return datetime.strptime(f"{year:04d}-{month:02d}-{day:02d} {hour_minute} {meridiem}", "%Y-%m-%d %I:%M %p").date()


def _parse_money(raw: str) -> Decimal:
    return Decimal(raw.replace(".", "").replace(",", "."))


def _extract_total_amount(text: str) -> Decimal:
    for pattern in (
        r"Total\s*([0-9]+,[0-9]{2})\s*US\$",
        r"Precio\s*([0-9]+,[0-9]{2})\s*US\$",
    ):
        match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return _parse_money(match.group(1))
    raise ValueError("Could not extract iPostal total amount.")
