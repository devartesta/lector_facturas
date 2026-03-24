from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import calendar
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "SOCIEDAD EST. CORREOS Y TELEGRAFOS, S.A., S.M.E."


@dataclass(frozen=True)
class CorreosInvoice:
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
    parser_name: str = "correos"
    parser_confidence: Decimal = Decimal("0.9970")

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
            "tax_label": "IGIC" if self.supplier_code == "CORREOSCAN" else "IVA",
        }


def parse_correos_pdf(path: Path) -> CorreosInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_correos_text(text, original_filename=path.name)


def parse_correos_text(text: str, *, original_filename: str) -> CorreosInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_number = _extract_invoice_number(normalized)
    invoice_date = _extract_invoice_date(normalized)
    billing_period_start, billing_period_end = _extract_period_range(normalized, invoice_date=invoice_date)
    tax_label, vat_percent, net_amount, vat_amount, gross_amount = _extract_totals(normalized)
    supplier_code = "CORREOSCAN" if tax_label == "IGIC" else "CORREOS"
    return CorreosInvoice(
        supplier_code=supplier_code,
        supplier_name=supplier_code,
        issuer_company_name=ISSUER_COMPANY_NAME,
        billed_company_name=COMPANY_NAME,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=billing_period_start,
        billing_period_end=billing_period_end,
        period_yyyymm=_period_with_most_days(billing_period_start, billing_period_end),
        currency_code="EUR",
        vat_percent=vat_percent,
        net_amount=net_amount,
        vat_amount=vat_amount,
        gross_amount=gross_amount,
        original_filename=original_filename,
    )


def _extract_invoice_number(text: str) -> str:
    match = re.search(r"\b[0-9]{2}\.[0-9]{2}\.[0-9]{4}\s+([0-9]{10})\b", text)
    if not match:
        match = re.search(r"N[º°] FACTURA\s*\n([0-9]{10})", text, flags=re.IGNORECASE)
    if not match:
        raise ValueError("Could not extract Correos invoice number.")
    return match.group(1)


def _extract_invoice_date(text: str) -> date:
    match = re.search(r"\b([0-9]{2}\.[0-9]{2}\.[0-9]{4})\s+[0-9]{10}\b", text)
    if not match:
        match = re.search(r"FECHA\s*\n([0-9]{2}\.[0-9]{2}\.[0-9]{4})", text, flags=re.IGNORECASE)
    if not match:
        raise ValueError("Could not extract Correos invoice date.")
    return datetime.strptime(match.group(1), "%d.%m.%Y").date()


def _extract_period_range(text: str, *, invoice_date: date) -> tuple[date, date]:
    match = re.search(r"([0-9]{2}\.[0-9]{2}\.[0-9]{4})/([0-9]{2}\.[0-9]{2}\.[0-9]{4})", text)
    if match:
        return (
            datetime.strptime(match.group(1), "%d.%m.%Y").date(),
            datetime.strptime(match.group(2), "%d.%m.%Y").date(),
        )
    month_start = date(invoice_date.year, invoice_date.month, 1)
    month_end = date(invoice_date.year, invoice_date.month, calendar.monthrange(invoice_date.year, invoice_date.month)[1])
    return month_start, month_end


def _extract_totals(text: str) -> tuple[str, Decimal, Decimal, Decimal, Decimal]:
    match = re.search(
        r"Base imponible sujeta a impuesto \((IVA|IGIC)\)\s+([0-9.,]+).*?Tipo impositivo:\s*([0-9.,]+)\s*%.*?Cuota:\s*([0-9.,]+).*?Total factura en Euros\s+([0-9.,]+)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        raise ValueError("Could not extract Correos totals.")
    tax_label = match.group(1).upper()
    return (
        tax_label,
        _parse_decimal(match.group(3)),
        _parse_decimal(match.group(2)),
        _parse_decimal(match.group(4)),
        _parse_decimal(match.group(5)),
    )


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
