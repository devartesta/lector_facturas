from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "XFERA MÓVILES, S.A.U."
SUPPLIER_CODE = "MASMOVIL"


@dataclass(frozen=True)
class MasMovilInvoice:
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
    parser_name: str = "masmovil"
    parser_confidence: Decimal = Decimal("0.9970")

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


def parse_masmovil_pdf(path: Path) -> MasMovilInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_masmovil_text(text, original_filename=path.name)


def parse_masmovil_text(text: str, *, original_filename: str) -> MasMovilInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    header_match = re.search(
        r"TOTAL A PAGAR\s+([A-Z0-9]+)\s+([0-9]{2}/[0-9]{2}/[0-9]{4})\s+([0-9]{2}/[0-9]{2}/[0-9]{4})",
        normalized,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not header_match:
        raise ValueError("Could not extract MasMovil header block.")
    invoice_number = header_match.group(1).strip()
    billing_period_start = _parse_date(header_match.group(2))
    invoice_date = _parse_date(header_match.group(3))
    billing_period_end = _parse_date(_extract(normalized, r"al\s+([0-9]{2}/[0-9]{2}/[0-9]{4})"))
    gross_amount = _parse_decimal(_extract(normalized, r"TOTAL A PAGAR\s+([0-9.,]+)€"))
    net_amount = _parse_decimal(_extract(normalized, r"\(21%\)\s*([0-9.,]+)€\s*Base imponible"))
    vat_amount = _parse_decimal(_extract(normalized, r"IVA 21%\s+([0-9.,]+)€"))
    return MasMovilInvoice(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_CODE,
        issuer_company_name=ISSUER_COMPANY_NAME,
        billed_company_name=COMPANY_NAME,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=billing_period_start,
        billing_period_end=billing_period_end,
        period_yyyymm=billing_period_end.strftime("%Y%m"),
        currency_code="EUR",
        vat_percent=Decimal("21"),
        gross_amount=gross_amount,
        vat_amount=vat_amount,
        net_amount=net_amount,
        original_filename=original_filename,
        sender_email="",
    )


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract MasMovil field with pattern: {pattern}")
    return match.group(1).strip()


def _parse_date(raw: str) -> date:
    day, month, year = raw.split("/")
    return date(int(year), int(month), int(day))


def _parse_decimal(raw: str) -> Decimal:
    return Decimal(raw.replace(".", "").replace(",", "."))
