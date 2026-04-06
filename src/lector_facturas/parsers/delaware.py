from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import calendar
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA INC"
ISSUER_COMPANY_NAME = "DELAWARE CORPORATE HEADQUARTERS LLC"
SUPPLIER_CODE = "DELAWARE"


@dataclass(frozen=True)
class DelawareInvoice:
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
    parser_name: str = "delaware"
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


def parse_delaware_pdf(path: Path) -> DelawareInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_delaware_text(text, original_filename=path.name)


def parse_delaware_text(text: str, *, original_filename: str) -> DelawareInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_number = _extract(normalized, r"Invoice Number\s+([A-Z0-9-]+)")
    invoice_date = datetime.strptime(
        _extract(normalized, r"Invoice Date\s+([A-Za-z]+ [0-9]{2}, [0-9]{4})"),
        "%B %d, %Y",
    ).date()
    gross_amount = _parse_money(_extract(normalized, r"Total\s+\$([0-9,]+\.[0-9]{2})"))
    billing_period_start = date(invoice_date.year, invoice_date.month, 1)
    billing_period_end = date(invoice_date.year, invoice_date.month, calendar.monthrange(invoice_date.year, invoice_date.month)[1])
    return DelawareInvoice(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_CODE,
        issuer_company_name=ISSUER_COMPANY_NAME,
        billed_company_name=COMPANY_NAME,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=billing_period_start,
        billing_period_end=billing_period_end,
        period_yyyymm=invoice_date.strftime("%Y%m"),
        currency_code="USD",
        vat_percent=Decimal("0"),
        net_amount=gross_amount,
        vat_amount=Decimal("0"),
        gross_amount=gross_amount,
        original_filename=original_filename,
        sender_email="",
    )


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract Delaware field with pattern: {pattern}")
    return match.group(1).strip()


def _parse_money(raw: str) -> Decimal:
    return Decimal(raw.replace(",", ""))
