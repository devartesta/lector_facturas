from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "ARVAL SERVICE LEASE, SAU."
SUPPLIER_CODE = "CAIXARENTING"


@dataclass(frozen=True)
class CaixaRentingInvoice:
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
    parser_name: str = "caixarenting"
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


def parse_caixarenting_pdf(path: Path) -> CaixaRentingInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    if not text.strip():
        raise ValueError("CaixaRenting PDF has no extractable text.")
    return parse_caixarenting_text(text, original_filename=path.name)


def parse_caixarenting_text(text: str, *, original_filename: str) -> CaixaRentingInvoice:
    normalized = text.replace("\ufeff", "").replace("\xa0", " ")
    normalized = re.sub(r"[ \t]+", " ", normalized)
    normalized = re.sub(r"\n{2,}", "\n", normalized)

    invoice_number = _extract(normalized, r"Factura\s+N[^\d]{0,8}(\d{10})")
    invoice_date = _parse_date(_extract(normalized, r"Fecha factura:\s*(\d{2}/\d{2}/\d{4})"))
    period_match = re.search(
        r"Periodo del\s+(\d{2}/\d{2}/\d{4})\s+al\s+(\d{2}/\d{2}/\d{4})",
        normalized,
        flags=re.IGNORECASE,
    )
    if not period_match:
        raise ValueError("Could not extract CaixaRenting service period.")
    billing_period_start = _parse_date(period_match.group(1))
    billing_period_end = _parse_date(period_match.group(2))

    summary = _extract(normalized, r"TOTAL GENERAL \(Euros\)\s+[\d.,]+\s+[\d.,]+\s+[\d.,]+", group=0)
    amounts = re.search(r"([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)$", summary)
    if not amounts:
        raise ValueError("Could not extract CaixaRenting totals.")
    net_amount = _parse_euro(amounts.group(1))
    vat_amount = _parse_euro(amounts.group(2))
    gross_amount = _parse_euro(amounts.group(3))

    return CaixaRentingInvoice(
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
        vat_percent=Decimal("21"),
        gross_amount=gross_amount,
        vat_amount=vat_amount,
        net_amount=net_amount,
        original_filename=original_filename,
        sender_email="info@caixarenting-auto.es",
    )


def _extract(text: str, pattern: str, *, group: int = 1) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract CaixaRenting field with pattern: {pattern}")
    return match.group(group).strip()


def _parse_date(raw: str) -> date:
    return datetime.strptime(raw, "%d/%m/%Y").date()


def _parse_euro(raw: str) -> Decimal:
    return Decimal(raw.replace(".", "").replace(",", "."))
