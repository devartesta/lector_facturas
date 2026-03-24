from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "SHOPIFY INTERNATIONAL LIMITED"
SUPPLIER_CODE = "SHOPIFY"
MONTHS_EN = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


@dataclass(frozen=True)
class ShopifyInvoice:
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
    parser_name: str = "shopify"
    parser_confidence: Decimal = Decimal("0.9960")

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


def parse_shopify_pdf(path: Path) -> ShopifyInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_shopify_text(text, original_filename=path.name)


def parse_shopify_text(text: str, *, original_filename: str) -> ShopifyInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "").replace("\ufb01", "fi")
    invoice_number = _extract(normalized, r"Bill #([0-9]+)")
    invoice_date = _parse_short_english_date(_extract(normalized, r"Paid on ([A-Z][a-z]{2} [0-9]{1,2}, [0-9]{4})"))
    gross_amount = _parse_decimal(_extract(normalized, r"TOTAL DUE\s+\$([0-9,]+\.[0-9]{2}) USD"))
    vat_amount = _parse_decimal(_extract(normalized, r"VAT 0\.0%\*\s+\$([0-9,]+\.[0-9]{2}) USD"))
    net_amount = _parse_decimal(_extract(normalized, r"Subtotal\s+\$([0-9,]+\.[0-9]{2}) USD"))
    periods = re.findall(r"([0-9]{4}-[0-9]{2}-[0-9]{2})\s*-\s*([0-9]{4}-[0-9]{2}-[0-9]{2})", normalized)
    if periods:
        starts = [date.fromisoformat(start) for start, _ in periods]
        ends = [date.fromisoformat(end) for _, end in periods]
        billing_period_start = min(starts)
        billing_period_end = max(ends)
    else:
        billing_period_start = invoice_date
        billing_period_end = invoice_date
    return ShopifyInvoice(
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
        gross_amount=gross_amount,
        vat_amount=vat_amount,
        net_amount=net_amount,
        original_filename=original_filename,
        sender_email="billing@shopify.com",
    )


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract Shopify field with pattern: {pattern}")
    return match.group(1).strip()


def _parse_short_english_date(raw: str) -> date:
    month_name, day_year = raw.split(" ", 1)
    day = int(day_year.split(",")[0])
    year = int(day_year.split(",")[1].strip())
    return date(year, MONTHS_EN[month_name.lower()], day)


def _parse_decimal(raw: str) -> Decimal:
    return Decimal(raw.replace(",", ""))
