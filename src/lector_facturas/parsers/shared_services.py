from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


ISSUER_COMPANY_NAME = "ARTESTA STORE, S.L."
SUPPLIER_CODE = "SHAREDSERVICESSL"
SPANISH_MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


@dataclass(frozen=True)
class SharedServicesInvoice:
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
    line_items: list[dict[str, str]]
    parser_name: str = "shared_services"
    parser_confidence: Decimal = Decimal("0.9950")
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
            "line_items": self.line_items,
        }


def parse_shared_services_pdf(path: Path) -> SharedServicesInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_date = _parse_spanish_numeric_date(_extract(normalized, r"(\d{2}/\d{2}/\d{4})\s+FACTURA"))
    invoice_number = _extract(normalized, r"Número de factura:\s*([0-9-]+)")
    billed_company_name = _extract(
        normalized,
        r"Número de factura:\s*[0-9-]+\s+(.+?)\s+(?:85 Great Portland Street First Floor|18 Campus Blvd, Suite 100)",
    )
    line_items = _extract_line_items(normalized)
    billing_period_start, billing_period_end = _resolve_period(line_items, invoice_date)
    net_amount = _parse_eur(_extract(normalized, r"Total Base Imponible:\s*([0-9.,]+)\s*€"))
    vat_match = re.search(r"Total IVA:\s*([0-9.,]+)\s*€", normalized, flags=re.IGNORECASE)
    vat_amount = _parse_eur(vat_match.group(1)) if vat_match else Decimal("0")
    gross_amount = _parse_eur(_extract(normalized, r"TOTAL:\s*([0-9.,]+)\s*€"))
    return SharedServicesInvoice(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_CODE,
        issuer_company_name=ISSUER_COMPANY_NAME,
        billed_company_name=billed_company_name.upper(),
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=billing_period_start,
        billing_period_end=billing_period_end,
        period_yyyymm=billing_period_end.strftime("%Y%m"),
        currency_code="EUR",
        vat_percent=Decimal("0"),
        gross_amount=gross_amount,
        vat_amount=vat_amount,
        net_amount=net_amount,
        original_filename=path.name,
        sender_email="",
        line_items=line_items,
    )


def _extract_line_items(text: str) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    pattern = re.compile(
        r"Shared services ([a-z]+)(?: - ([a-z]+))?\s+1\s+([0-9.,]+)\s*€\s+[0-9.,]+\s*€\s+0%\s+[0-9.,]+\s*€",
        flags=re.IGNORECASE,
    )
    for match in pattern.finditer(text):
        concept = match.group(1).lower()
        month_name = (match.group(2) or "").lower()
        amount = _parse_eur(match.group(3))
        items.append(
            {
                "concept": concept,
                "month_name": month_name,
                "net_amount": format(amount, "f"),
            }
        )
    if not items:
        raise ValueError("Could not extract shared services line items.")
    return items


def _resolve_period(line_items: list[dict[str, str]], invoice_date: date) -> tuple[date, date]:
    month_name = next((item["month_name"] for item in line_items if item["month_name"]), "")
    if not month_name:
        return invoice_date, invoice_date
    month_number = SPANISH_MONTHS[month_name]
    year = invoice_date.year
    if month_number > invoice_date.month:
        year -= 1
    start = date(year, month_number, 1)
    if month_number == 12:
        end = date(year, 12, 31)
    else:
        from calendar import monthrange

        end = date(year, month_number, monthrange(year, month_number)[1])
    return start, end


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract shared services field with pattern: {pattern}")
    return " ".join(match.group(1).split())


def _parse_eur(raw: str) -> Decimal:
    return Decimal(raw.replace(".", "").replace(",", "."))


def _parse_spanish_numeric_date(raw: str) -> date:
    day_s, month_s, year_s = raw.split("/")
    return date(int(year_s), int(month_s), int(day_s))
