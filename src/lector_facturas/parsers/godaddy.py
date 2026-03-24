from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "GODADDY.COM, LLC"
SUPPLIER_CODE = "GODADDY"


@dataclass(frozen=True)
class GoDaddyInvoice:
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
    parser_name: str = "godaddy"
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


def parse_godaddy_pdf(path: Path) -> GoDaddyInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_godaddy_text(text, original_filename=path.name)


def parse_godaddy_text(text: str, *, original_filename: str) -> GoDaddyInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_number = _extract(normalized, r"№\s*([0-9]+)")
    invoice_date = _parse_short_date(_extract(normalized, r"FECHA:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})"))
    net_amount = _parse_decimal(_extract(normalized, r"Subtotal\s+([0-9.,]+)\s*€"))
    vat_amount = _parse_decimal(_extract(normalized, r"Impuestos\s+([0-9.,]+)\s*€"))
    gross_amount = _parse_decimal(_extract(normalized, r"Total \(EUR\)\s+([0-9.,]+)\s*€"))
    vat_percent = _parse_decimal(_extract(normalized, r"VAT \(([0-9.,]+)\s*%\)"))
    billing_period_start = invoice_date
    billing_period_end = invoice_date + timedelta(days=364)
    return GoDaddyInvoice(
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
        sender_email="",
    )


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract GoDaddy field with pattern: {pattern}")
    return match.group(1).strip()


def _parse_short_date(raw: str) -> date:
    day, month, year = raw.split("/")
    return date(int(year), int(month), int(day))


def _parse_decimal(raw: str) -> Decimal:
    return Decimal(raw.replace(".", "").replace(",", "."))
