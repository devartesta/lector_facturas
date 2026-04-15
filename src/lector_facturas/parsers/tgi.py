from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import calendar
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA INC"
ISSUER_COMPANY_NAME = "TODAY'S GRAPHICS INC"
SUPPLIER_CODE = "TGI"
TWOPLACES = Decimal("0.01")


@dataclass(frozen=True)
class TgiInvoice:
    supplier_code: str
    supplier_name: str
    issuer_company_name: str
    billed_company_name: str
    invoice_number: str
    invoice_date: date
    billing_period_start: date
    billing_period_end: date
    period_yyyymm: str
    division_invoice: str
    currency_code: str
    vat_percent: Decimal
    gross_amount: Decimal
    vat_amount: Decimal
    net_amount: Decimal
    original_filename: str
    parser_name: str = "tgi"
    parser_confidence: Decimal = Decimal("0.9950")

    @property
    def extracted_raw(self) -> dict[str, object]:
        return {
            "issuer_company_name": self.issuer_company_name,
            "billed_company_name": self.billed_company_name,
            "billing_period_start": self.billing_period_start.isoformat(),
            "billing_period_end": self.billing_period_end.isoformat(),
            "period_yyyymm": self.period_yyyymm,
            "division_invoice": self.division_invoice,
            "currency_code": self.currency_code,
            "vat_percent": format(self.vat_percent, "f"),
            "gross_amount": format(self.gross_amount, "f"),
            "vat_amount": format(self.vat_amount, "f"),
            "net_amount": format(self.net_amount, "f"),
        }


def parse_tgi_pdf(path: Path) -> TgiInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_tgi_text(text, original_filename=path.name)


def parse_tgi_text(text: str, *, original_filename: str) -> TgiInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_number = _extract_invoice_number(normalized)
    invoice_date = _extract_invoice_date(normalized)
    description, amount = _extract_line_description_and_amount(normalized)
    billing_period_start, billing_period_end = _extract_period_range(description, invoice_date)
    division_invoice = _extract_division(description)
    net_amount = amount
    vat_amount = Decimal("0.00")
    gross_amount = amount
    return TgiInvoice(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_CODE,
        issuer_company_name=ISSUER_COMPANY_NAME,
        billed_company_name=COMPANY_NAME,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=billing_period_start,
        billing_period_end=billing_period_end,
        period_yyyymm=_period_with_most_days(billing_period_start, billing_period_end),
        division_invoice=division_invoice,
        currency_code="USD",
        vat_percent=Decimal("0.00"),
        net_amount=net_amount,
        vat_amount=vat_amount,
        gross_amount=gross_amount,
        original_filename=original_filename,
    )


def _extract_invoice_number(text: str) -> str:
    match = re.search(r"Invoice\s+Invoice Date.*?\n(\d{6})\n", text, flags=re.DOTALL)
    if match:
        return match.group(1)
    match = re.search(r"INVOICE\s+([0-9]{6})\s+Invoice\s+#", text, flags=re.IGNORECASE | re.DOTALL)
    if match:
        return match.group(1)
    match = re.search(r"Invoice\s+#\s*([0-9]{6})", text, flags=re.IGNORECASE)
    if match:
        return match.group(1)
    match = re.search(r"Invoice Number\s*:\s*([0-9]{6})", text, flags=re.IGNORECASE)
    if match:
        return match.group(1)
    raise ValueError("Could not extract TGI invoice number.")


def _extract_invoice_date(text: str) -> date:
    match = re.search(r"\n(\d{1,2}/\d{1,2}/\d{2})\nNet", text)
    if match:
        return datetime.strptime(match.group(1), "%m/%d/%y").date()
    match = re.search(r"Invoice Date\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2})", text, flags=re.IGNORECASE)
    if match:
        return datetime.strptime(match.group(1), "%m/%d/%y").date()
    match = re.search(r"Invoice Date\s*:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2})", text, flags=re.IGNORECASE)
    if match:
        return datetime.strptime(match.group(1), "%m/%d/%y").date()
    raise ValueError("Could not extract TGI invoice date.")


def _extract_line_description_and_amount(text: str) -> tuple[str, Decimal]:
    match = re.search(r"\$([\d,]+\.\d{2})([A-Za-z].*?)1\s+Artesta,Inc", text, flags=re.DOTALL)
    if match:
        amount = _parse_decimal(match.group(1))
        description = " ".join(match.group(2).split())
        return description, amount
    match = re.search(
        r"Quantity\s+Description\s+Amount\s+1\s+(.+?)\s+\$([\d,]+\.\d{2})\s+Subtotal",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if match:
        description = " ".join(match.group(1).split())
        amount = _parse_decimal(match.group(2))
        return description, amount
    raise ValueError("Could not extract TGI description and amount.")


def _extract_period_range(description: str, invoice_date: date) -> tuple[date, date]:
    match = re.search(
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})(?:st|nd|rd|th)?-(\d{1,2})(?:st|nd|rd|th)?\s+(\d{4})",
        description,
        flags=re.IGNORECASE,
    )
    if match:
        month = _month_number(match.group(1))
        year = int(match.group(4))
        return date(year, month, int(match.group(2))), date(year, month, int(match.group(3)))
    match = re.search(
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})(?:st|nd|rd|th)?-(\d{1,2})(?:st|nd|rd|th)?",
        description,
        flags=re.IGNORECASE,
    )
    if match:
        month = _month_number(match.group(1))
        year = invoice_date.year
        return date(year, month, int(match.group(2))), date(year, month, int(match.group(3)))
    month = invoice_date.month
    year = invoice_date.year
    return date(year, month, 1), date(year, month, calendar.monthrange(year, month)[1])


def _extract_division(description: str) -> str:
    lowered = description.lower()
    if "shipping" in lowered or "freight" in lowered:
        return "logistics"
    if "production" in lowered:
        return "manufacturing"
    raise ValueError("Could not infer TGI division from description.")


def _month_number(name: str) -> int:
    return {
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
    }[name.lower()]


def _period_with_most_days(start: date, end: date) -> str:
    counts: dict[str, int] = {}
    current = start
    while current <= end:
        key = current.strftime("%Y%m")
        counts[key] = counts.get(key, 0) + 1
        current = date.fromordinal(current.toordinal() + 1)
    return max(counts.items(), key=lambda item: (item[1], item[0]))[0]


def _parse_decimal(raw_value: str) -> Decimal:
    return Decimal(raw_value.replace(",", "")).quantize(TWOPLACES, rounding=ROUND_HALF_UP)
