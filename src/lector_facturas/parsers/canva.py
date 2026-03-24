from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
import calendar
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "CANVA PTY. LTD."
SUPPLIER_CODE = "CANVA"
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
class CanvaInvoice:
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
    parser_name: str = "canva"
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
        }


def parse_canva_pdf(path: Path) -> CanvaInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_canva_text(text, original_filename=path.name)


def parse_canva_text(text: str, *, original_filename: str) -> CanvaInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_number = _extract(normalized, r"Nro\. de factura\s+([0-9-]+)")
    invoice_date = _parse_spanish_date(_extract(normalized, r"Fecha\s+de factura\s+([0-9]{1,2} de [A-Za-záéíóú]+ de [0-9]{4})"))
    gross_amount = _parse_decimal(_extract(normalized, r"Importe\s+total\s+([0-9.,]+)\s*€"))
    vat_amount = _parse_decimal(_extract(normalized, r"Impuestos\s+incluidos\s+([0-9.,]+)\s*€"))
    net_amount = gross_amount - vat_amount
    billing_period_start = date(invoice_date.year, invoice_date.month, 1)
    billing_period_end = date(invoice_date.year, invoice_date.month, calendar.monthrange(invoice_date.year, invoice_date.month)[1])
    return CanvaInvoice(
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
        vat_percent=Decimal("21"),
        gross_amount=gross_amount,
        vat_amount=vat_amount,
        net_amount=net_amount,
        original_filename=original_filename,
        sender_email="no-reply@canva.com",
    )


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract Canva field with pattern: {pattern}")
    return match.group(1).strip()


def _parse_spanish_date(raw: str) -> date:
    day_s, _, month_name, _, year_s = raw.split()
    return date(int(year_s), SPANISH_MONTHS[month_name.lower()], int(day_s))


def _parse_decimal(raw: str) -> Decimal:
    return Decimal(raw.replace(".", "").replace(",", "."))
