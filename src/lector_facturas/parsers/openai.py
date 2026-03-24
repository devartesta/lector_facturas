from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "OPENAI OPCO, LLC"
SUPPLIER_CODE = "OPENAI"
MONTHS_EN = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "jun": 6, "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}


@dataclass(frozen=True)
class OpenAIInvoice:
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
    division_invoice: str = ""
    document_type: str = "invoice"
    parser_name: str = "openai"
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
            "division_invoice": self.division_invoice,
            "document_type": self.document_type,
        }


def parse_openai_pdf(path: Path) -> OpenAIInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_openai_text(text, original_filename=path.name)


def parse_openai_text(text: str, *, original_filename: str) -> OpenAIInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "").replace("\x00", "")
    invoice_number = _extract(normalized, r"Invoice number\s+([A-Z0-9-]+)")
    is_receipt = "Receipt number" in normalized or normalized.lstrip().startswith("Receipt")
    date_label = "Date paid" if is_receipt else "Date of issue"
    invoice_date = _parse_english_date(_extract(normalized, rf"{date_label}\s+([A-Za-z]+ [0-9]{{1,2}}, [0-9]{{4}})"))
    if "OpenAI Ireland Limited" in normalized:
        issuer_company_name = "OPENAI IRELAND LIMITED"
        currency_symbol = "€"
        currency_code = "EUR"
        division_invoice = "chatgpt_plus"
    else:
        issuer_company_name = ISSUER_COMPANY_NAME
        currency_symbol = "$"
        currency_code = "USD"
        division_invoice = "receipt" if is_receipt else "api_usage"
    net_amount = _parse_money(_extract(normalized, rf"Total excluding tax\s+\{currency_symbol}([0-9,]+\.[0-9]{{2}})"))
    vat_amount = _parse_money(_extract(normalized, rf"VAT - Spain\s+[0-9]{{1,2}}% on \{currency_symbol}[0-9,]+\.[0-9]{{2}}\s+\{currency_symbol}([0-9,]+\.[0-9]{{2}})"))
    vat_percent = Decimal(_extract(normalized, r"VAT - Spain\s+([0-9]{1,2})%"))
    total_label = "Amount paid" if is_receipt else "Amount due"
    gross_amount = _parse_money(_extract(normalized, rf"{total_label}\s+\{currency_symbol}([0-9,]+\.[0-9]{{2}})"))
    period_match = re.search(r"([A-Za-z]{3,9} [0-9]{1,2})\s*[–-]\s*([A-Za-z]{3,9} [0-9]{1,2}, [0-9]{4})", normalized)
    if period_match:
        billing_period_end = _parse_english_date(period_match.group(2))
        billing_period_start = _parse_english_date(f"{period_match.group(1)}, {billing_period_end.year}")
        if billing_period_start > billing_period_end:
            billing_period_start = date(billing_period_end.year - 1, billing_period_start.month, billing_period_start.day)
    else:
        billing_period_start = date(invoice_date.year, invoice_date.month, 1)
        billing_period_end = invoice_date
    return OpenAIInvoice(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_CODE,
        issuer_company_name=issuer_company_name,
        billed_company_name=COMPANY_NAME,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=billing_period_start,
        billing_period_end=billing_period_end,
        period_yyyymm=billing_period_start.strftime("%Y%m"),
        currency_code=currency_code,
        vat_percent=vat_percent,
        gross_amount=gross_amount,
        vat_amount=vat_amount,
        net_amount=net_amount,
        original_filename=original_filename,
        sender_email="ar@openai.com",
        division_invoice=division_invoice,
        document_type="receipt" if is_receipt else "invoice",
    )


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract OpenAI field with pattern: {pattern}")
    return match.group(1).strip()


def _parse_money(raw: str) -> Decimal:
    return Decimal(raw.replace(",", ""))


def _parse_english_date(raw: str) -> date:
    month_name, day_year = raw.split(" ", 1)
    day = int(day_year.split(",")[0])
    year = int(day_year.split(",")[1].strip())
    return date(year, MONTHS_EN[month_name.lower()], day)
