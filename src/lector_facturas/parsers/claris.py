from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import calendar
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "CLARÍS GESTIÓ I DOCUMENTACIÓ, S.L."
SUPPLIER_CODE = "CLARIS"
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
class ClarisInvoice:
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
    parser_name: str = "claris"
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


def parse_claris_pdf(path: Path) -> ClarisInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_claris_text(text, original_filename=path.name)


def parse_claris_text(text: str, *, original_filename: str) -> ClarisInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_number = _extract_invoice_number(normalized)
    invoice_date = _extract_invoice_date(normalized)
    billing_period_start, billing_period_end = _extract_billing_period(normalized, invoice_date)
    net_amount = _extract_amount(normalized, r"ASESORAMIENTO FISCAL CONTABLE\s+Correspondiente al mes de [a-záéíóú]+\s+([0-9.,]+)\s*€")
    vat_percent = _extract_amount(normalized, r"I\.V\.A\s+([0-9.,]+)\s*%")
    vat_amount = _extract_amount(normalized, r"I\.V\.A [0-9.,]+ % S/ [0-9.,]+ €\s+([0-9.,]+)\s*€")
    gross_amount = _extract_amount(normalized, r"TOTAL (?:HONORARIOS|A PAGAR)\s+([0-9.,]+)\s*€")
    return ClarisInvoice(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_CODE,
        issuer_company_name=ISSUER_COMPANY_NAME,
        billed_company_name=COMPANY_NAME,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=billing_period_start,
        billing_period_end=billing_period_end,
        period_yyyymm=billing_period_start.strftime("%Y%m"),
        currency_code="EUR",
        vat_percent=vat_percent,
        net_amount=net_amount,
        vat_amount=vat_amount,
        gross_amount=gross_amount,
        original_filename=original_filename,
        sender_email="",
    )


def _extract_invoice_number(text: str) -> str:
    match = re.search(r"Factura Nº:\s*([A-Z0-9/.-]+)", text)
    if not match:
        raise ValueError("Could not extract Claris invoice number.")
    return match.group(1)


def _extract_invoice_date(text: str) -> date:
    match = re.search(r"Fecha de expedición:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})", text)
    if not match:
        raise ValueError("Could not extract Claris invoice date.")
    return datetime.strptime(match.group(1), "%d/%m/%Y").date()


def _extract_billing_period(text: str, invoice_date: date) -> tuple[date, date]:
    match = re.search(r"Correspondiente al mes de ([a-záéíóú]+)", text, flags=re.IGNORECASE)
    if not match:
        month = invoice_date.month
        year = invoice_date.year
    else:
        month_name = match.group(1).strip().lower()
        month = SPANISH_MONTHS.get(month_name)
        if not month:
            raise ValueError(f"Unsupported Claris billing month: {month_name}")
        year = invoice_date.year
    start = date(year, month, 1)
    end = date(year, month, calendar.monthrange(year, month)[1])
    return start, end


def _extract_amount(text: str, pattern: str) -> Decimal:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract Claris amount with pattern: {pattern}")
    raw = match.group(1).strip()
    normalized = raw.replace(".", "").replace(",", ".")
    return Decimal(normalized)
