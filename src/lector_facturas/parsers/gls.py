from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import calendar
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "RED Y GESTION TERRASSA, S.L."
SUPPLIER_CODE = "GLS"


@dataclass(frozen=True)
class GlsInvoice:
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
    parser_name: str = "gls"
    parser_confidence: Decimal = Decimal("0.9900")

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


def parse_gls_ocr_text(text: str, *, original_filename: str) -> GlsInvoice:
    normalized = _normalize_ocr_text(text)
    invoice_number = _extract_invoice_number(normalized)
    invoice_date = _extract_invoice_date(normalized)
    net_amount, vat_amount, gross_amount = _extract_totals(normalized)
    sender_email = _extract_sender_email(normalized)
    period_start = date(invoice_date.year, invoice_date.month, 1)
    period_end = invoice_date
    return GlsInvoice(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_CODE,
        issuer_company_name=ISSUER_COMPANY_NAME,
        billed_company_name=COMPANY_NAME,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=period_start,
        billing_period_end=period_end,
        period_yyyymm=_period_with_most_days(period_start, period_end),
        currency_code="EUR",
        vat_percent=Decimal("21.00"),
        net_amount=net_amount,
        vat_amount=vat_amount,
        gross_amount=gross_amount,
        original_filename=original_filename,
        sender_email=sender_email,
    )


def parse_gls_pdf(pdf_path: Path) -> GlsInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(pdf_path)).pages[:2])
    if not text.strip():
        raise ValueError("GLS scanned PDF requires OCR from reception workflow.")
    return parse_gls_ocr_text(text, original_filename=pdf_path.name)


def _normalize_ocr_text(text: str) -> str:
    normalized = (
        text.replace("\ufeff", "")
        .replace("\r", "\n")
        .replace("?", "")
        .replace("“", '"')
        .replace("”", '"')
    )
    normalized = re.sub(r"[ \t]+", " ", normalized)
    normalized = re.sub(r"\n{2,}", "\n", normalized)
    return normalized


def _extract_invoice_number(text: str) -> str:
    match = re.search(r"N[ºo]\s*Factura:\s*(\d+)", text, flags=re.IGNORECASE)
    if not match:
        match = re.search(r"Factura:\s*(\d+)", text, flags=re.IGNORECASE)
    if not match:
        raise ValueError("Could not extract GLS invoice number.")
    return match.group(1)


def _extract_invoice_date(text: str) -> date:
    match = re.search(r"Fecha Fact:\s*(\d{2},\d{2},\d{2})", text, flags=re.IGNORECASE)
    if not match:
        raise ValueError("Could not extract GLS invoice date.")
    return datetime.strptime(match.group(1), "%d,%m,%y").date()


def _extract_totals(text: str) -> tuple[Decimal, Decimal, Decimal]:
    compact = text.replace("\n", " ")
    marker = re.search(r"EXENTO DE IVA:\s*SUBTOTAL:\s*21 ?% IVA:\s*TOTAL:", compact, flags=re.IGNORECASE)
    if not marker:
        raise ValueError("Could not extract GLS totals.")
    amounts = re.findall(r"[0-9.]+,[0-9]{2}", compact[marker.end(): marker.end() + 250])
    if len(amounts) < 3:
        raise ValueError("Could not extract GLS totals.")
    return (_parse_decimal(amounts[0]), _parse_decimal(amounts[1]), _parse_decimal(amounts[2]))


def _extract_sender_email(text: str) -> str:
    lowered = text.lower()
    if "info@rgtmensajeros.com" in lowered:
        return "info@rgtmensajeros.com"
    return "info@rgtmensajeros.com"


def _period_with_most_days(start: date, end: date) -> str:
    month_counts: dict[str, int] = {}
    current = start
    while current <= end:
        key = current.strftime("%Y%m")
        month_counts[key] = month_counts.get(key, 0) + 1
        current = date.fromordinal(current.toordinal() + 1)
    return max(month_counts.items(), key=lambda item: (item[1], item[0]))[0]


def _parse_decimal(raw_value: str) -> Decimal:
    return Decimal(raw_value.replace(".", "").replace(",", "."))
