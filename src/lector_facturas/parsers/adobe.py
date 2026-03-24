from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "ADOBE SYSTEMS SOFTWARE IRELAND LTD"
SUPPLIER_CODE = "ADOBE"
SPANISH_MONTHS = {
    "ENE": 1,
    "FEB": 2,
    "MAR": 3,
    "ABR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AGO": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DIC": 12,
}


@dataclass(frozen=True)
class AdobeInvoice:
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
    parser_name: str = "adobe"
    parser_confidence: Decimal = Decimal("0.9990")

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
            "tax_label": "REVERSE_CHARGE",
            "sender_email": self.sender_email,
        }


def parse_adobe_pdf(path: Path) -> AdobeInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_adobe_text(text, original_filename=path.name)


def parse_adobe_text(text: str, *, original_filename: str) -> AdobeInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_number = _extract(normalized, r"([A-Z]{3}[0-9]{13})Número de factura")
    invoice_date = _parse_adobe_date(_extract(normalized, r"([0-9]{2}-[A-Z]{3}-[0-9]{4})Fecha de la factura"))
    period_raw = _extract(normalized, r"Duración del servicio:\s*([0-9]{2}-[A-Z]{3}-[0-9]{4})\s*a\s*([0-9]{2}-[A-Z]{3}-[0-9]{4})", group=0)
    period_match = re.search(r"([0-9]{2}-[A-Z]{3}-[0-9]{4})\s*a\s*([0-9]{2}-[A-Z]{3}-[0-9]{4})", period_raw)
    billing_period_start = _parse_adobe_date(period_match.group(1))
    billing_period_end = _parse_adobe_date(period_match.group(2))
    net_amount = _parse_decimal(_extract(normalized, r"IMPORTE NETO \(EUR\)\s+([0-9]+\.[0-9]{2})"))
    vat_amount = _parse_decimal(_extract(normalized, r"IMPUESTOS \(VER LOS TIPOS\)\s+([0-9]+\.[0-9]{2})"))
    gross_amount = _parse_decimal(_extract(normalized, r"TOTAL \(EUR\)\s+([0-9]+\.[0-9]{2})"))
    return AdobeInvoice(
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
        vat_percent=Decimal("0"),
        net_amount=net_amount,
        vat_amount=vat_amount,
        gross_amount=gross_amount,
        original_filename=original_filename,
        sender_email="",
    )


def _extract(text: str, pattern: str, *, group: int = 1) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract Adobe field with pattern: {pattern}")
    return match.group(group).strip()


def _parse_adobe_date(raw: str) -> date:
    day, month_code, year = raw.split("-")
    return date(int(year), SPANISH_MONTHS[month_code.upper()], int(day))


def _parse_decimal(raw: str) -> Decimal:
    return Decimal(raw.replace(",", ""))
