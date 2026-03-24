from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "INTERNATIONAL MAIL (SPAIN), S.L."
SUPPLIER_CODE = "SPRINGGDS"


@dataclass(frozen=True)
class SpringInvoice:
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
    parser_name: str = "spring"
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


def parse_spring_pdf(path: Path) -> SpringInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_spring_text(text, original_filename=path.name)


def parse_spring_text(text: str, *, original_filename: str) -> SpringInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_number = _extract_invoice_number(normalized)
    invoice_date = _extract_invoice_date(normalized)
    net_amount = _extract_amount(normalized, r"TOTAL SUJETO A IVA\s+([0-9.,]+)")
    vat_percent = _extract_amount(normalized, r"IVA\s+([0-9.,]+)%")
    vat_amount = _extract_amount(normalized, r"IVA [0-9.,]+%\s+([0-9.,]+)")
    gross_amount = _extract_amount(normalized, r"TOTAL FACTURA EUR\s+([0-9.,]+)")
    billing_period_start = invoice_date
    billing_period_end = invoice_date
    return SpringInvoice(
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
        vat_percent=vat_percent,
        net_amount=net_amount,
        vat_amount=vat_amount,
        gross_amount=gross_amount,
        original_filename=original_filename,
        sender_email="admin.es@spring-gds.com",
    )


def _extract_invoice_number(text: str) -> str:
    match = re.search(r"Factura Núm\.\s*E?([0-9]{7})", text)
    if not match:
        match = re.search(r"Factura Núm\.\s*\n(E[0-9]{7})", text)
        if match:
            return match.group(1)
        raise ValueError("Could not extract Spring invoice number.")
    value = match.group(1)
    return value if value.startswith("E") else f"E{value}"


def _extract_invoice_date(text: str) -> date:
    match = re.search(r"Fecha Factura\s*Vencimiento\s*NIF\s*([0-9]{2}/[0-9]{2}/[0-9]{2})", text)
    if not match:
        match = re.search(r"Fecha Factura\s*\n([0-9]{2}/[0-9]{2}/[0-9]{2})", text)
    if not match:
        raise ValueError("Could not extract Spring invoice date.")
    return datetime.strptime(match.group(1), "%d/%m/%y").date()


def _extract_amount(text: str, pattern: str) -> Decimal:
    match = re.search(pattern, text)
    if not match:
        raise ValueError(f"Could not extract Spring amount with pattern: {pattern}")
    raw = match.group(1).strip()
    if "," in raw and "." in raw:
        normalized = raw.replace(".", "").replace(",", ".")
    elif "," in raw:
        normalized = raw.replace(",", ".")
    else:
        normalized = raw
    return Decimal(normalized)
