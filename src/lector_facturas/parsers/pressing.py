from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import calendar
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "PRESSING IMPRESSIÓ DIGITAL, S.A."
SUPPLIER_CODE = "PRESSING"

MONTHS_ES = {
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
class PressingInvoice:
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
    parser_name: str = "pressing"
    parser_confidence: Decimal = Decimal("0.9950")

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


def parse_pressing_pdf(path: Path) -> PressingInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_pressing_text(text, original_filename=path.name)


def parse_pressing_text(text: str, *, original_filename: str) -> PressingInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_number = _extract_invoice_number(normalized)
    invoice_date = _extract_invoice_date(normalized)
    billing_period_start, billing_period_end = _extract_period_range(normalized)
    vat_percent, net_amount, vat_amount, gross_amount = _extract_totals(normalized)
    return PressingInvoice(
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
    )


def _extract_invoice_number(text: str) -> str:
    match = re.search(r"Factura:\s*([A-Z]+/\d{4}/\d+|\d+)", text)
    if not match:
        raise ValueError("Could not extract PRESSING invoice number.")
    return match.group(1).strip()


def _extract_invoice_date(text: str) -> date:
    match = re.search(r"Fecha:\s*(\d{2}-\d{2}-\d{4})", text)
    if not match:
        raise ValueError("Could not extract PRESSING invoice date.")
    return datetime.strptime(match.group(1), "%d-%m-%Y").date()


def _extract_period_range(text: str) -> tuple[date, date]:
    match = re.search(r"durante el mes de\s+([A-Za-záéíóúñ]+)\s+(\d{4})", text, flags=re.IGNORECASE)
    if not match:
        raise ValueError("Could not extract PRESSING billing period.")
    month_name = match.group(1).lower()
    year = int(match.group(2))
    month = MONTHS_ES[month_name]
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last_day)


def _extract_totals(text: str) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    match = re.search(
        r"Importe % Dto Importe Dto B\. Imponible IVA T\. Impuesto Total Factura\s*([\d.,]+)€.*?([\d.,]+)€\s*([\d.,]+)%\s*([\d.,]+)€\s*([\d.,]+)€",
        text,
        flags=re.DOTALL,
    )
    if not match:
        raise ValueError("Could not extract PRESSING totals.")
    net_amount = _parse_decimal(match.group(2))
    vat_percent = _parse_decimal(match.group(3))
    vat_amount = _parse_decimal(match.group(4))
    gross_amount = _parse_decimal(match.group(5))
    return vat_percent, net_amount, vat_amount, gross_amount


def _parse_decimal(raw_value: str) -> Decimal:
    return Decimal(raw_value.replace(".", "").replace(",", "."))
