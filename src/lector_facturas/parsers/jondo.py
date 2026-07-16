from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORES (UK) LTD"
US_COMPANY_NAME = "ARTESTA INC"
ISSUER_COMPANY_NAME = "JONDO UK"
SUPPLIER_CODE = "JONDO"


@dataclass(frozen=True)
class JondoInvoice:
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
    parser_name: str = "jondo"
    parser_confidence: Decimal = Decimal("0.9900")
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


def parse_jondo_pdf(path: Path) -> JondoInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return _parse_jondo_text(text, original_filename=path.name)


def _parse_jondo_text(text: str, *, original_filename: str) -> JondoInvoice:
    invoice_number = _invoice_number_from_filename(original_filename) or _extract(text, r"PO Number:\s*([A-Z0-9-]+)")
    invoice_date = _parse_iso_date(_extract(text, r"Date:\s*([0-9]{4}-[0-9]{2}-[0-9]{2})"))
    product_subtotal = _parse_decimal(_extract(text, r"\bSubtotal\s+USD \$\s*([0-9.,]+)"))
    shipping_amount = _extract_optional_decimal(text, r"\bShipping\s+USD \$\s*([0-9.,]+)") or Decimal("0")
    net_amount = product_subtotal + shipping_amount
    gross_amount = _parse_decimal(_extract(text, r"\bTotal\s+USD \$\s*([0-9.,]+)"))
    billed_company_name = _detect_billed_company(text)
    vat_amount = _extract_optional_decimal(text, r"GB VAT\s+USD \$\s*([0-9.,]+)")
    if vat_amount is None:
        vat_amount = gross_amount - net_amount
    if billed_company_name == US_COMPANY_NAME:
        # US sales tax is not reclaimed, so it should remain part of the expense base.
        net_amount = gross_amount
    return JondoInvoice(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_CODE,
        issuer_company_name=_detect_issuer_company(text),
        billed_company_name=billed_company_name,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=invoice_date,
        billing_period_end=invoice_date,
        period_yyyymm=invoice_date.strftime("%Y%m"),
        currency_code="USD",
        vat_percent=Decimal("20"),
        gross_amount=gross_amount,
        vat_amount=vat_amount,
        net_amount=net_amount,
        original_filename=original_filename,
        sender_email="",
    )


def _detect_billed_company(text: str) -> str:
    upper = text.upper()
    if "ARTESTA,INC" in upper or "ARTESTA INC" in upper:
        return US_COMPANY_NAME
    return COMPANY_NAME


def _invoice_number_from_filename(original_filename: str) -> str:
    stem = Path(original_filename).stem
    match = re.fullmatch(r"(\d+-AS-\d+)", stem, flags=re.IGNORECASE)
    return match.group(1).upper() if match else ""


def _detect_issuer_company(text: str) -> str:
    if "SENSARIA" in text.upper():
        return "JONDO US"
    return ISSUER_COMPANY_NAME


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract JONDO field with pattern: {pattern}")
    return match.group(1).strip()


def _parse_decimal(raw: str) -> Decimal:
    return Decimal(raw.replace(",", ""))


def _extract_optional_decimal(text: str, pattern: str) -> Decimal | None:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    return _parse_decimal(match.group(1).strip())


def _parse_iso_date(raw: str) -> date:
    year_s, month_s, day_s = raw.split("-")
    return date(int(year_s), int(month_s), int(day_s))
