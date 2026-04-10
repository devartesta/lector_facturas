from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "ADEPLUS CONSULTORES, S.L.U."
SUPPLIER_CODE = "ADEPLUS"

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
class AdeplusInvoice:
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
    parser_name: str = "adeplus"
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


def parse_adeplus_pdf(path: Path) -> AdeplusInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages[:2])
    if not text.strip():
        raise ValueError("Adeplus PDF has no extractable text.")
    return parse_adeplus_text(text, original_filename=path.name)


def parse_adeplus_text(text: str, *, original_filename: str) -> AdeplusInvoice:
    normalized = text.replace("\ufeff", "").replace("\r", "\n").replace("\xa0", " ")
    normalized = re.sub(r"[ \t]+", " ", normalized)
    normalized = re.sub(r"\n{2,}", "\n", normalized)

    invoice_date = _parse_spanish_date(_extract(normalized, r"\b([0-9]{2}/[0-9]{2}/[0-9]{4})\b"))
    invoice_number = _extract(normalized, r"\n(IFC[0-9]{4}-[0-9]+)\s*\n[0-9]{2}/[0-9]{2}/[0-9]{4}")
    period_start, period_end = _extract_period(normalized, invoice_date)
    net_amount = _parse_euro(_extract(normalized, r"Base imponible sujeta a\s*21%\s*\n([0-9.,]+)€"))
    vat_amount = _parse_euro(_extract(normalized, r"Varios con IVA\s*21\s*%\s*\n([0-9.,]+)€"))
    gross_amount = _parse_euro(_extract(normalized, r"Total Factura\s*\n([0-9.,]+)€"))
    vat_percent = Decimal(_extract(normalized, r"Base imponible sujeta a\s*(\d+)%"))

    return AdeplusInvoice(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_CODE,
        issuer_company_name=ISSUER_COMPANY_NAME,
        billed_company_name=COMPANY_NAME,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=period_start,
        billing_period_end=period_end,
        period_yyyymm=invoice_date.strftime("%Y%m"),
        currency_code="EUR",
        vat_percent=vat_percent,
        gross_amount=gross_amount,
        vat_amount=vat_amount,
        net_amount=net_amount,
        original_filename=original_filename,
        sender_email="",
    )


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract Adeplus field with pattern: {pattern}")
    return match.group(1).strip()


def _extract_period(text: str, invoice_date: date) -> tuple[date, date]:
    match = re.search(
        r"PERIODO FACTURA:\s*([A-Za-zÁÉÍÓÚÜáéíóúüñÑ]+)\s+(\d{4})\s*-\s*([A-Za-zÁÉÍÓÚÜáéíóúüñÑ]+)\s+(\d{4})",
        text,
        flags=re.IGNORECASE,
    )
    if not match:
        return invoice_date, invoice_date
    start_month = MONTHS_ES[match.group(1).lower()]
    start_year = int(match.group(2))
    end_month = MONTHS_ES[match.group(3).lower()]
    end_year = int(match.group(4))
    return date(start_year, start_month, 1), date(end_year, end_month, 1)


def _parse_spanish_date(raw: str) -> date:
    return datetime.strptime(raw, "%d/%m/%Y").date()


def _parse_euro(raw: str) -> Decimal:
    return Decimal(raw.replace(".", "").replace(",", "."))
