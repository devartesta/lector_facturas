from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "MICROSOFT IBERICA S.R.L."
SUPPLIER_CODE = "MICROSOFT"


@dataclass(frozen=True)
class MicrosoftInvoice:
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
    parser_name: str = "microsoft"
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
        }


def parse_microsoft_pdf(path: Path) -> MicrosoftInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_microsoft_text(text, original_filename=path.name)


def parse_microsoft_text(text: str, *, original_filename: str) -> MicrosoftInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_number = _extract(
        normalized,
        r"(?:N(?:Ãº|ú|u|º|°|o)?mero de facturaci(?:Ã³|ó|o)n\s+([A-Z0-9]+))|\b(G\d{9})\b",
    )
    invoice_date = datetime.strptime(
        _extract(normalized, r"Fecha del documento\s+([0-9]{2}/[0-9]{2}/[0-9]{4})"),
        "%d/%m/%Y",
    ).date()
    period_match = re.search(r"([0-9]{2}/[0-9]{2}/[0-9]{4})-\s*([0-9]{2}/[0-9]{2}/[0-9]{4})", normalized)
    if not period_match:
        raise ValueError("Could not extract Microsoft billing period.")
    billing_period_start = datetime.strptime(period_match.group(1), "%d/%m/%Y").date()
    billing_period_end = datetime.strptime(period_match.group(2), "%d/%m/%Y").date()
    net_amount = _parse_decimal(_extract(normalized, r"Total \(sin incluir impuestos\)\s+([0-9.,]+)"))
    vat_amount = _parse_decimal(_extract(normalized, r"Importe de impuestos\s+([0-9.,]+)"))
    gross_amount = _parse_decimal(_extract(normalized, r"Total con impuestos incluidos(?: EUR)?\s+([0-9.,]+)"))
    vat_percent = _parse_decimal(
        _extract(
            normalized,
            r"Ventas nacionales con tasa est(?:Ã¡|á|a)ndar\s+([0-9.,]+)%",
        )
    )
    return MicrosoftInvoice(
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
        vat_percent=vat_percent,
        gross_amount=gross_amount,
        vat_amount=vat_amount,
        net_amount=net_amount,
        original_filename=original_filename,
        sender_email="",
    )


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract Microsoft field with pattern: {pattern}")
    groups = [group.strip() for group in match.groups() if group]
    if not groups:
        raise ValueError(f"Could not extract Microsoft field with pattern: {pattern}")
    return groups[0]


def _parse_decimal(raw: str) -> Decimal:
    if "," in raw:
        return Decimal(raw.replace(".", "").replace(",", "."))
    return Decimal(raw)
