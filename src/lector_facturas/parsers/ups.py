from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from collections import Counter
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "UNITED PARCEL SERVICE DEUTSCHLAND S.À R.L. & CO. OHG"
SUPPLIER_CODE = "UPS"
GERMAN_MONTHS = {
    "jan": 1,
    "feb": 2,
    "mär": 3,
    "maer": 3,
    "apr": 4,
    "mai": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "okt": 10,
    "nov": 11,
    "dez": 12,
}
GERMAN_FULL_MONTHS = {
    "januar": 1,
    "februar": 2,
    "märz": 3,
    "maerz": 3,
    "april": 4,
    "mai": 5,
    "juni": 6,
    "juli": 7,
    "august": 8,
    "september": 9,
    "oktober": 10,
    "november": 11,
    "dezember": 12,
}


@dataclass(frozen=True)
class UpsInvoice:
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
    parser_name: str = "ups"
    parser_confidence: Decimal = Decimal("0.9920")

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
            "reverse_charge": True,
        }


def parse_ups_pdf(path: Path) -> UpsInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_ups_text(text, original_filename=path.name)


def parse_ups_text(text: str, *, original_filename: str) -> UpsInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_number = _extract_invoice_number(normalized)
    invoice_date = _extract_invoice_date(normalized)
    billing_period_end = _extract_period_end(normalized)
    billing_period_start = _extract_period_start(normalized, invoice_date=invoice_date, billing_period_end=billing_period_end)
    gross_amount = _extract_total_amount(normalized)
    sender_email = _extract_sender_email(normalized)
    return UpsInvoice(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_CODE,
        issuer_company_name=ISSUER_COMPANY_NAME,
        billed_company_name=COMPANY_NAME,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=billing_period_start,
        billing_period_end=billing_period_end,
        period_yyyymm=invoice_date.strftime("%Y%m"),
        currency_code="EUR",
        vat_percent=Decimal("0.00"),
        net_amount=gross_amount,
        vat_amount=Decimal("0.00"),
        gross_amount=gross_amount,
        original_filename=original_filename,
        sender_email=sender_email,
    )


def _extract_invoice_number(text: str) -> str:
    match = re.search(
        r"Kundennr\.:\s*Rechnungsnr\.:\s*Seite:.*?\n[A-Z0-9]+\n(\d{9})\n",
        text,
        flags=re.DOTALL,
    )
    if not match:
        match = re.search(r"Rechnungsnr\.:(?:\s*\n[^\n]+){0,6}\s*\n(\d{9})\n", text)
    if not match:
        match = re.search(r"\nRechnungsnummer\s*\n(\d{9})\n", text)
    if not match:
        raise ValueError("Could not extract UPS invoice number.")
    return match.group(1)


def _extract_invoice_date(text: str) -> date:
    match = re.search(r"Rechnungsdatum\s*(\d{2}\.[A-Za-zÄÖÜäöü]+ \d{4})", text)
    if not match:
        raise ValueError("Could not extract UPS invoice date.")
    return _parse_german_full_date(match.group(1))


def _extract_period_end(text: str) -> date:
    match = re.search(r"bis einschließlich\s*(\d{2}\.[A-Za-zÄÖÜäöü]+ \d{4})", text)
    if not match:
        raise ValueError("Could not extract UPS billing period end.")
    return _parse_german_full_date(match.group(1))


def _extract_period_start(text: str, *, invoice_date: date, billing_period_end: date) -> date:
    matches = re.findall(r"\b(\d{1,2})\.(Jan|Feb|Mär|Apr|Mai|Jun|Jul|Aug|Sep|Okt|Nov|Dez)\b", text, flags=re.IGNORECASE)
    dates: list[date] = []
    for day_raw, month_raw in matches:
        month = GERMAN_MONTHS[_normalize_month_key(month_raw)]
        candidate = date(invoice_date.year, month, int(day_raw))
        if candidate > billing_period_end:
            candidate = date(invoice_date.year - 1, month, int(day_raw))
        dates.append(candidate)
    if dates:
        likely_dates = [candidate for candidate in dates if candidate >= billing_period_end - timedelta(days=21)]
        return min(likely_dates or dates)
    return billing_period_end


def _extract_total_amount(text: str) -> Decimal:
    match = re.search(r"Fälliger Gesamtbetrag\s*EUR\s*([\d.,]+)", text)
    if not match:
        raise ValueError("Could not extract UPS total amount.")
    return _parse_decimal(match.group(1))


def _extract_sender_email(text: str) -> str:
    for candidate in ("rechnungswesen@ups.com", "defcr@ups.com", "importinfo@ups.com"):
        if candidate in text.lower():
            return candidate
    return "rechnungswesen@ups.com"


def _parse_german_full_date(raw_value: str) -> date:
    day_raw, month_raw, year_raw = raw_value.replace(".", " ").split()
    month = GERMAN_FULL_MONTHS[_normalize_month_key(month_raw)]
    return date(int(year_raw), month, int(day_raw))


def _normalize_month_key(raw_value: str) -> str:
    return (
        raw_value.strip()
        .lower()
        .replace("ä", "ä")
        .replace("ö", "ö")
        .replace("ü", "ü")
    )


def _period_with_most_days(start: date, end: date) -> str:
    counts: Counter[str] = Counter()
    current = start
    while current <= end:
        counts[current.strftime("%Y%m")] += 1
        current += timedelta(days=1)
    return max(counts.items(), key=lambda item: (item[1], item[0]))[0]


def _parse_decimal(raw_value: str) -> Decimal:
    return Decimal(raw_value.replace(".", "").replace(",", "."))
