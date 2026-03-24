from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "VITALY HEALTH SERVICES, S.L."
SUPPLIER_CODE = "VITALY"


@dataclass(frozen=True)
class VitalyInvoice:
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
    parser_name: str = "vitaly"
    parser_confidence: Decimal = Decimal("0.9850")

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
            "service_type": "occupational_health_prevention",
            "annual_service_assumption": True,
        }


def parse_vitaly_pdf(path: Path) -> VitalyInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_vitaly_text(text, original_filename=path.name)


def parse_vitaly_text(text: str, *, original_filename: str) -> VitalyInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_number = _extract(normalized, r"(IFC\d{4}-\d+)")
    invoice_date = _extract_invoice_date(normalized, invoice_number)
    net_amount = _parse_decimal(_extract_amount(normalized, "Base imponible sujeta a 21%"))
    vat_amount = _parse_decimal(_extract_amount(normalized, "Varios con IVA 21 %"))
    gross_amount = _parse_decimal(_extract_amount(normalized, "Total Factura"))

    # Business assumption confirmed by user direction: this is treated as an annual
    # prevention/occupational-health service that belongs to the 2026 admin budget.
    billing_period_start = date(invoice_date.year, 1, 1)
    billing_period_end = date(invoice_date.year, 12, 31)

    return VitalyInvoice(
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
        vat_percent=Decimal("21.0000"),
        gross_amount=gross_amount,
        vat_amount=vat_amount,
        net_amount=net_amount,
        original_filename=original_filename,
        sender_email="",
    )


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract Vitaly field with pattern: {pattern}")
    return match.group(1).strip()


def _extract_invoice_date(text: str, invoice_number: str) -> date:
    patterns = (
        rf"([0-9]{{2}}/[0-9]{{2}}/[0-9]{{4}})\s+{re.escape(invoice_number)}\s+[0-9]{{2}}/[0-9]{{2}}/[0-9]{{4}}\s+ARTESTA STORE",
        r"Fecha Factura:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})",
    )
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return datetime.strptime(match.group(1), "%d/%m/%Y").date()
    all_dates = re.findall(r"([0-9]{2}/[0-9]{2}/[0-9]{4})", text)
    if all_dates:
        return datetime.strptime(all_dates[0], "%d/%m/%Y").date()
    raise ValueError("Could not extract Vitaly invoice date.")


def _extract_amount(text: str, label: str) -> str:
    match = re.search(rf"{re.escape(label)}\s*([0-9][0-9.,]*)", text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract Vitaly amount for label: {label}")
    return match.group(1).strip()


def _parse_decimal(raw: str) -> Decimal:
    return Decimal(raw.replace(".", "").replace(",", "."))
