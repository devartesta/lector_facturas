from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "ART LINK BALTIC OU"
SUPPLIER_CODE = "ARTLINK"


@dataclass(frozen=True)
class ArtlinkInvoice:
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
    parser_name: str = "artlink"
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
            "division_invoice": self.division_invoice,
        }


def parse_artlink_pdf(path: Path) -> ArtlinkInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_artlink_text(text, original_filename=path.name)


def parse_artlink_text(text: str, *, original_filename: str) -> ArtlinkInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_number = _extract(normalized, r"INVOICE\s+([0-9]+)")
    invoice_date = _parse_date(_extract(normalized, r"Date:\s+([0-9]{2}-[0-9]{2}-[0-9]{2})"))
    gross_amount = _parse_decimal(_extract(normalized, r"T O T A L EUR\s+([0-9.,]+)"))
    return ArtlinkInvoice(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_CODE,
        issuer_company_name=ISSUER_COMPANY_NAME,
        billed_company_name=COMPANY_NAME,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=invoice_date,
        billing_period_end=invoice_date,
        period_yyyymm=invoice_date.strftime("%Y%m"),
        currency_code="EUR",
        vat_percent=Decimal("0"),
        gross_amount=gross_amount,
        vat_amount=Decimal("0"),
        net_amount=gross_amount,
        original_filename=original_filename,
        sender_email="",
    )


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract Artlink field with pattern: {pattern}")
    return match.group(1).strip()


def _parse_date(raw: str) -> date:
    day, month, year = raw.split("-")
    return date(2000 + int(year), int(month), int(day))


def _parse_decimal(raw: str) -> Decimal:
    return Decimal(raw.replace(".", "").replace(",", "."))
