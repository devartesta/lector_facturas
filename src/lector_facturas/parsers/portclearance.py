from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORES (UK) LTD"
ISSUER_COMPANY_NAME = "PORT CLEARANCE SERVICES LTD"
SUPPLIER_CODE = "PORTCLEARANCE"


@dataclass(frozen=True)
class PortClearanceInvoice:
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
    division_invoice: str = "logistics"
    parser_name: str = "portclearance"
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
            "division_invoice": self.division_invoice,
        }


def parse_portclearance_pdf(path: Path) -> PortClearanceInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_portclearance_text(text, original_filename=path.name)


def parse_portclearance_text(text: str, *, original_filename: str) -> PortClearanceInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_number = _extract(normalized, r"(?:INVOICE NO\s+|[0-9]{2}\.[0-9]{2}\.[0-9]{4}\s+[0-9A-Z]+\s+)(PCSI[A-Z0-9]+)")
    invoice_date = _parse_date(_extract(normalized, r"ARTESTA STORES LTD.*?\n([0-9]{2}\.[0-9]{2}\.[0-9]{4})\s+[0-9A-Z]+\s+[A-Z0-9]+"))
    net_amount = _parse_decimal(_extract(normalized, r"TOTAL\s+0\.00\s+([0-9.]+)\s+GBP"))
    return PortClearanceInvoice(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_CODE,
        issuer_company_name=ISSUER_COMPANY_NAME,
        billed_company_name=COMPANY_NAME,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=invoice_date,
        billing_period_end=invoice_date,
        period_yyyymm=invoice_date.strftime("%Y%m"),
        currency_code="GBP",
        vat_percent=Decimal("0"),
        gross_amount=net_amount,
        vat_amount=Decimal("0"),
        net_amount=net_amount,
        original_filename=original_filename,
        sender_email="accounts@pcsl.uk.com",
    )


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract Port Clearance field with pattern: {pattern}")
    return match.group(1).strip()


def _parse_date(raw: str) -> date:
    day, month, year = raw.split(".")
    return date(int(year), int(month), int(day))


def _parse_decimal(raw: str) -> Decimal:
    return Decimal(raw.replace(",", ""))
