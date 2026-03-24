from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "GOOGLE CLOUD EMEA LIMITED"
SUPPLIER_CODE = "GOOGLEWORKSPACE"
SPANISH_MONTHS = {"ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6, "jul": 7, "ago": 8, "sep": 9, "oct": 10, "nov": 11, "dic": 12}


@dataclass(frozen=True)
class GoogleWorkspaceInvoice:
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
    parser_name: str = "googleworkspace"
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
            "tax_label": "REVERSE_CHARGE",
        }


def parse_googleworkspace_pdf(path: Path) -> GoogleWorkspaceInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_googleworkspace_text(text, original_filename=path.name)


def parse_googleworkspace_text(text: str, *, original_filename: str) -> GoogleWorkspaceInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_number = _extract(normalized, r"N(?:ú|Ãº)mero de factura:\s*([0-9]+)")
    invoice_date = _parse_date(_extract(normalized, r"\.{10,}([0-9]{1,2} [a-z]{3} [0-9]{4})"))
    period_match = re.search(r"Resumen de ([0-9]{1,2} [a-z]{3} [0-9]{4}) - ([0-9]{1,2} [a-z]{3} [0-9]{4})", normalized, flags=re.IGNORECASE)
    if not period_match:
        raise ValueError("Could not extract Google Workspace billing period.")
    billing_period_start = _parse_date(period_match.group(1))
    billing_period_end = _parse_date(period_match.group(2))
    totals_match = re.search(
        r"Total en EUR\s+([0-9.,]+)\s*(?:€|â‚¬)\s+([0-9.,]+)\s*(?:€|â‚¬)\s+([0-9.,]+)\s*(?:€|â‚¬)\s+Resumen de",
        normalized,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not totals_match:
        raise ValueError("Could not extract Google Workspace totals block.")
    net_amount = _parse_decimal(totals_match.group(1))
    vat_amount = _parse_decimal(totals_match.group(2))
    gross_amount = _parse_decimal(totals_match.group(3))
    return GoogleWorkspaceInvoice(
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
        gross_amount=gross_amount,
        vat_amount=vat_amount,
        net_amount=net_amount,
        original_filename=original_filename,
        sender_email="",
    )


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract Google Workspace field with pattern: {pattern}")
    return match.group(1).strip()


def _parse_date(raw: str) -> date:
    day_s, month_s, year_s = raw.split()
    return date(int(year_s), SPANISH_MONTHS[month_s.lower()], int(day_s))


def _parse_decimal(raw: str) -> Decimal:
    return Decimal(raw.replace(".", "").replace(",", "."))
