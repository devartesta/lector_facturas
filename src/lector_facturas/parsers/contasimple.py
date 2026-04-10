from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "CEGID SMB, S.A.U"
SUPPLIER_CODE = "CONTASIMPLE"


@dataclass(frozen=True)
class ContasimpleInvoice:
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
    parser_name: str = "contasimple"
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


def parse_contasimple_pdf(path: Path) -> ContasimpleInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages[:2])
    if not text.strip():
        raise ValueError("Contasimple PDF has no extractable text.")
    return parse_contasimple_text(text, original_filename=path.name)


def parse_contasimple_text(text: str, *, original_filename: str) -> ContasimpleInvoice:
    normalized = text.replace("\ufeff", "").replace("\r", "\n").replace("\xa0", " ")
    normalized = re.sub(r"[ \t]+", " ", normalized)
    normalized = re.sub(r"\n{2,}", "\n", normalized)

    invoice_number = _extract(normalized, r"N[úu]mero de factura:\s*([A-Z]{2}-\d{4}-\d+)")
    invoice_date = _parse_spanish_date(_extract(normalized, r"Fecha:\s*(\d{2}/\d{2}/\d{4})"))
    billing_period_start = _parse_spanish_date(
        _extract(normalized, r"Periodo de prestaci[óo]n del servicio\.\s*Desde:\s*(\d{2}/\d{2}/\d{4})")
    )
    billing_period_end = _parse_spanish_date(
        _extract(normalized, r"Periodo de prestaci[óo]n del servicio\..*?Hasta:\s*(\d{2}/\d{2}/\d{4})")
    )
    net_amount = _parse_euro(_extract(normalized, r"Total B\.I\.:\s*([\d.,]+)\s*€"))
    vat_amount = _parse_euro(_extract(normalized, r"Total IVA:\s*([\d.,]+)\s*€"))
    gross_amount = _parse_euro(_extract(normalized, r"TOTAL:\s*([\d.,]+)\s*€"))
    vat_percent = Decimal(_extract(normalized, r"IVA\s*(\d+)%"))

    return ContasimpleInvoice(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_CODE,
        issuer_company_name=ISSUER_COMPANY_NAME,
        billed_company_name=COMPANY_NAME,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=billing_period_start,
        billing_period_end=billing_period_end,
        period_yyyymm=invoice_date.strftime("%Y%m"),
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
        raise ValueError(f"Could not extract Contasimple field with pattern: {pattern}")
    return match.group(1).strip()


def _parse_spanish_date(raw: str) -> date:
    return datetime.strptime(raw, "%d/%m/%Y").date()


def _parse_euro(raw: str) -> Decimal:
    return Decimal(raw.replace(".", "").replace(",", "."))
