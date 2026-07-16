from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "SIMPLY.COM A/S"
SUPPLIER_CODE = "SIMPLYCOM"


@dataclass(frozen=True)
class SimplyComInvoice:
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
    parser_name: str = "simplycom"
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
        }


def parse_simplycom_pdf(path: Path) -> SimplyComInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_simplycom_text(text, original_filename=path.name)


def parse_simplycom_text(text: str, *, original_filename: str) -> SimplyComInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_number = _extract(normalized, r"(?:Factura|N[uú]mero de factura\.?)\s+([0-9]+)")
    invoice_date = _parse_iso_datetime(_extract(normalized, r"Fecha\s+([0-9]{4}-[0-9]{2}-[0-9]{2})T"))
    service_period_match = re.search(
        r"([0-9]{4}-[0-9]{2}-[0-9]{2})\s*-\s*([0-9]{4}-[0-9]{2}-[0-9]{2})",
        normalized,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if service_period_match:
        billing_period_start = _parse_iso_datetime(service_period_match.group(1))
        billing_period_end = _parse_iso_datetime(service_period_match.group(2))
    else:
        billing_period_start = invoice_date
        billing_period_end = invoice_date
    net_amount = _parse_decimal(_extract(normalized, r"(?:^|\n)I alt excl\. IVA\s+([0-9.,-]+)\s*€"))
    vat_amount = _parse_decimal(_extract(normalized, r"(?:^|\n)IVA\s+([0-9.,-]+)\s*€"))
    gross_amount = _parse_decimal(_extract(normalized, r"(?:^|\n)I alt incl\. IVA\s+([0-9.,-]+)\s*€"))
    vat_percent = Decimal("0") if vat_amount == Decimal("0") else Decimal("21")
    return SimplyComInvoice(
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
        gross_amount=gross_amount,
        vat_amount=vat_amount,
        net_amount=net_amount,
        original_filename=original_filename,
        sender_email="billing@simply.com",
    )


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract SimplyCom field with pattern: {pattern}")
    return match.group(1).strip()


def _parse_decimal(raw: str) -> Decimal:
    return Decimal(raw.replace(".", "").replace(",", "."))


def _parse_iso_datetime(raw: str) -> date:
    year_s, month_s, day_s = raw.split("-")
    return date(int(year_s), int(month_s), int(day_s))
