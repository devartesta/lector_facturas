from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "INTERZERO RECYCLING ALLIANCE GMBH"
SUPPLIER_CODE = "LIZENZERO"


@dataclass(frozen=True)
class LizenzeroInvoice:
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
    parser_name: str = "lizenzero"
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


def parse_lizenzero_pdf(path: Path) -> LizenzeroInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    if not text.strip():
        raise ValueError("Lizenzero PDF has no extractable text.")
    return parse_lizenzero_text(text, original_filename=path.name)


def parse_lizenzero_text(text: str, *, original_filename: str) -> LizenzeroInvoice:
    normalized = text.replace("\ufeff", "").replace("\xa0", " ").replace("\r", "\n")
    normalized = re.sub(r"[ \t]+", " ", normalized)
    normalized = re.sub(r"\n{2,}", "\n", normalized)

    invoice_number = _extract(normalized, r"Rechnung Nr\.\s*([0-9]+)")
    invoice_date = _parse_german_date(_extract(normalized, r"Datum:\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4})"))
    period_start = _parse_german_date(
        _extract(normalized, r"Leistungszeitraum:\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4})")
    )
    period_end = _parse_german_date(
        _extract(normalized, r"Leistungszeitraum:.*?-\s*([0-9]{2}\.[0-9]{2}\.[0-9]{4})")
    )
    net_amount = _parse_euro(_extract(normalized, r"Gesamtkosten Netto:\s*([0-9.,]+)\s*€"))
    vat_match = re.search(r"zzgl\.\s*([0-9]+)\s*%\s*MwSt\.:\s*([0-9.,]+)\s*€", normalized, flags=re.IGNORECASE)
    if not vat_match:
        raise ValueError("Could not extract Lizenzero VAT row.")
    vat_percent = Decimal(vat_match.group(1))
    vat_amount = _parse_euro(vat_match.group(2))
    gross_amount = _parse_euro(_extract(normalized, r"Gesamtkosten:\s*([0-9.,]+)\s*€"))

    return LizenzeroInvoice(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_CODE,
        issuer_company_name=ISSUER_COMPANY_NAME,
        billed_company_name=COMPANY_NAME,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=period_start,
        billing_period_end=period_end,
        period_yyyymm=invoice_date.strftime("%Y%m"),
        currency_code="EUR",
        vat_percent=vat_percent,
        gross_amount=gross_amount,
        vat_amount=vat_amount,
        net_amount=net_amount,
        original_filename=original_filename,
        sender_email="kontakt@lizenzero.de",
    )


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract Lizenzero field with pattern: {pattern}")
    return match.group(1).strip()


def _parse_german_date(raw: str) -> date:
    return datetime.strptime(raw, "%d.%m.%Y").date()


def _parse_euro(raw: str) -> Decimal:
    return Decimal(raw.replace(".", "").replace(",", "."))
