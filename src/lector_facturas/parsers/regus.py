from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA INC"
ISSUER_COMPANY_NAME = "REGUS"
SUPPLIER_CODE = "REGUS"
MONTHS = {
    "January": 1,
    "February": 2,
    "March": 3,
    "April": 4,
    "May": 5,
    "June": 6,
    "July": 7,
    "August": 8,
    "September": 9,
    "October": 10,
    "November": 11,
    "December": 12,
}


@dataclass(frozen=True)
class RegusInvoice:
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
    parser_name: str = "regus"
    parser_confidence: Decimal = Decimal("0.9920")
    document_type: str = "invoice"

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


def parse_regus_pdf(path: Path) -> RegusInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    invoice_number = _extract(text, r"Invoice number:\s*([0-9-]+)")
    invoice_date = _parse_english_date(_extract(text, r"Invoice date:\s*([0-9]{2} [A-Za-z]+ [0-9]{4})"))
    period_start = _parse_short_english_date(_extract(text, r"Weekly Mail Forwarding\s+([0-9]{1,2} [A-Za-z]{3} [0-9]{4})"))
    period_end = _parse_short_english_date(_extract(text, r"Weekly Mail Forwarding\s+[0-9]{1,2} [A-Za-z]{3} [0-9]{4}\s+([0-9]{1,2} [A-Za-z]{3} [0-9]{4})"))
    charge_line = re.search(
        r"Weekly Mail Forwarding\s+[0-9]{1,2} [A-Za-z]{3} [0-9]{4}\s+[0-9]{1,2} [A-Za-z]{3} [0-9]{4}\s+\$ ([0-9.,]+)\s+\$ ([0-9.,]+)\s+\$ ([0-9.,]+)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not charge_line:
        raise ValueError("Could not extract REGUS charge line.")
    net_amount = _parse_decimal(charge_line.group(1))
    vat_amount = _parse_decimal(charge_line.group(2))
    gross_amount = _parse_decimal(charge_line.group(3))
    vat_percent = Decimal("0")
    if net_amount:
        vat_percent = (vat_amount / net_amount * Decimal("100")).quantize(Decimal("0.01"))
    return RegusInvoice(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_CODE,
        issuer_company_name=ISSUER_COMPANY_NAME,
        billed_company_name=COMPANY_NAME,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=period_start,
        billing_period_end=period_end,
        period_yyyymm=period_end.strftime("%Y%m"),
        currency_code="USD",
        vat_percent=vat_percent,
        gross_amount=gross_amount,
        vat_amount=vat_amount,
        net_amount=net_amount,
        original_filename=path.name,
        sender_email="",
    )


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if not match:
        raise ValueError(f"Could not extract REGUS field with pattern: {pattern}")
    return match.group(1).strip()


def _parse_decimal(raw: str) -> Decimal:
    return Decimal(raw.replace(",", ""))


def _parse_english_date(raw: str) -> date:
    day_s, month_name, year_s = raw.split()
    return date(int(year_s), MONTHS[month_name], int(day_s))


def _parse_short_english_date(raw: str) -> date:
    day_s, month_name, year_s = raw.split()
    short = {k[:3]: v for k, v in MONTHS.items()}
    return date(int(year_s), short[month_name], int(day_s))
