from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "ASESORIA FISCAL NODA Y ASOCIADOS, S.L."
SUPPLIER_CODE = "NODA"
SPANISH_MONTHS = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}


@dataclass(frozen=True)
class NodaInvoice:
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
    parser_name: str = "noda"
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
            "tax_label": "IGIC",
            "sender_email": self.sender_email,
        }


def parse_noda_pdf(path: Path) -> NodaInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_noda_text(text, original_filename=path.name)


def parse_noda_text(text: str, *, original_filename: str) -> NodaInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_number = _extract_invoice_number(normalized)
    invoice_date = _extract_invoice_date(normalized)
    billing_period_start, billing_period_end = _extract_billing_period(normalized)
    net_amount = _extract_amount(normalized, r"HONORARIOS\s+([0-9.,]+)\s*€")
    vat_percent = _extract_amount(normalized, r"\+\s*([0-9.,]+)\s*%\s*I\.G\.I\.C\.")
    vat_amount = _extract_amount(normalized, r"\+\s*[0-9.,]+\s*%\s*I\.G\.I\.C\.\s+([0-9.,]+)\s*€")
    gross_amount = _extract_amount(normalized, r"TOTAL\s+([0-9.,]+)\s*€")
    return NodaInvoice(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_CODE,
        issuer_company_name=ISSUER_COMPANY_NAME,
        billed_company_name=COMPANY_NAME,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=billing_period_start,
        billing_period_end=billing_period_end,
        period_yyyymm=billing_period_end.strftime("%Y%m"),
        currency_code="EUR",
        vat_percent=vat_percent,
        net_amount=net_amount,
        vat_amount=vat_amount,
        gross_amount=gross_amount,
        original_filename=original_filename,
        sender_email="",
    )


def _extract_invoice_number(text: str) -> str:
    match = re.search(r"F A C T U R A\s+N[º°]\s*([0-9 ]+/[0-9]+)", text)
    if not match:
        raise ValueError("Could not extract Noda invoice number.")
    return match.group(1).replace(" ", "")


def _extract_invoice_date(text: str) -> date:
    match = re.search(r"En Santa Cruz de Tenerife a,\s*([0-9]{1,2}) de ([A-Za-záéíóú]+) de 2\.0?26", text, flags=re.IGNORECASE)
    if not match:
        raise ValueError("Could not extract Noda invoice date.")
    day = int(match.group(1))
    month = SPANISH_MONTHS[match.group(2).strip().lower()]
    return date(2026, month, day)


def _extract_billing_period(text: str) -> tuple[date, date]:
    match = re.search(
        r"per[ií]odo trimestral de\s+([A-Za-záéíóú]+)\s+a\s+([A-Za-záéíóú]+)\s+de\s+2\.0?25",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        raise ValueError("Could not extract Noda billing period.")
    start_month = SPANISH_MONTHS[match.group(1).strip().lower()]
    end_month = SPANISH_MONTHS[match.group(2).strip().lower()]
    return date(2025, start_month, 1), _month_end(2025, end_month)


def _month_end(year: int, month: int) -> date:
    if month == 12:
        return date(year, 12, 31)
    next_month = date(year, month + 1, 1)
    return date.fromordinal(next_month.toordinal() - 1)


def _extract_amount(text: str, pattern: str) -> Decimal:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract Noda amount with pattern: {pattern}")
    return Decimal(match.group(1).replace(".", "").replace(",", "."))
