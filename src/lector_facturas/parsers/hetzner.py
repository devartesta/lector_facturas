from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
import calendar
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "HETZNER ONLINE GMBH"
SUPPLIER_CODE = "HETZNER"


@dataclass(frozen=True)
class HetznerInvoice:
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
    parser_name: str = "hetzner"
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


def parse_hetzner_pdf(path: Path) -> HetznerInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_hetzner_text(text, original_filename=path.name)


def parse_hetzner_text(text: str, *, original_filename: str) -> HetznerInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_number = _extract(normalized, r"Invoice no\.\:\s*([0-9]+)")
    invoice_date = _parse_date(_extract(normalized, r"Invoice date:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})"))
    period_code = _extract(normalized, r"Storage\s+\(([0-9]{2}/[0-9]{4})\)")
    month = int(period_code.split("/")[0])
    year = int(period_code.split("/")[1])
    billing_period_start = date(year, month, 1)
    billing_period_end = date(year, month, calendar.monthrange(year, month)[1])
    total_rows = re.findall(r"Total €\s*([0-9.]+)\s*€\s*([0-9.]+)\s*€\s*([0-9.]+)", normalized)
    if not total_rows:
        raise ValueError("Could not extract Hetzner totals.")
    net_amount = _parse_decimal(total_rows[0][0])
    vat_amount = _parse_decimal(total_rows[0][1])
    gross_amount = _parse_decimal(total_rows[0][2])
    vat_percent = _parse_decimal(_extract(normalized, r"SJ\s*([0-9]+)\s*%"))
    return HetznerInvoice(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_CODE,
        issuer_company_name=ISSUER_COMPANY_NAME,
        billed_company_name=COMPANY_NAME,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=billing_period_start,
        billing_period_end=billing_period_end,
        period_yyyymm=f"{year:04d}{month:02d}",
        currency_code="EUR",
        vat_percent=vat_percent,
        gross_amount=gross_amount,
        vat_amount=vat_amount,
        net_amount=net_amount,
        original_filename=original_filename,
        sender_email="info@hetzner.com",
    )


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract Hetzner field with pattern: {pattern}")
    return match.group(1).strip()


def _parse_date(raw: str) -> date:
    day, month, year = raw.split("/")
    return date(int(year), int(month), int(day))


def _parse_decimal(raw: str) -> Decimal:
    return Decimal(raw.replace(",", ""))
