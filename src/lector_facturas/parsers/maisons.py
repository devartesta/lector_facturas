from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


SUPPLIER_CODE = "MAISONS"
SUPPLIER_NAME = "MAISONS DU MONDE"
ISSUER_NAME = "Maisons du Monde France SAS"
BILLED_COMPANY = "ARTESTA STORE, S.L."


@dataclass(frozen=True)
class MaisonsInvoice:
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
    division_invoice: str = "marketplace"
    parser_name: str = "maisons"
    parser_confidence: Decimal = Decimal("0.9950")

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


def parse_maisons_pdf(path: Path) -> MaisonsInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_maisons_text(text, original_filename=path.name)


def parse_maisons_text(text: str, *, original_filename: str) -> MaisonsInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_number = _extract(normalized, r"Facture n[°º]\s*([0-9]+)")
    invoice_date = _parse_date(_extract(normalized, r"Date d['’]émission\s*:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})"))
    net_amount = _parse_decimal(_extract(normalized, r"Total HT\s+EUR\s+([0-9.,]+)"))
    vat_amount = _parse_decimal(_extract(normalized, r"Total Taxes\s+EUR\s+([0-9.,]+)"))
    gross_amount = _parse_decimal(_extract(normalized, r"Total TTC\s+EUR\s+([0-9.,]+)"))
    period_yyyymm = invoice_date.strftime("%Y%m")
    return MaisonsInvoice(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_NAME,
        issuer_company_name=ISSUER_NAME,
        billed_company_name=BILLED_COMPANY,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=date(invoice_date.year, invoice_date.month, 1),
        billing_period_end=invoice_date,
        period_yyyymm=period_yyyymm,
        currency_code="EUR",
        vat_percent=Decimal("0.00"),
        gross_amount=gross_amount,
        vat_amount=vat_amount,
        net_amount=net_amount,
        original_filename=original_filename,
        sender_email="no-reply-mirakl-sender@maisonsdumonde.com",
    )


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract Maisons field with pattern: {pattern}")
    return match.group(1).strip()


def _parse_decimal(raw: str) -> Decimal:
    return Decimal(raw.replace(".", "").replace(",", ".").strip())


def _parse_date(raw: str) -> date:
    return datetime.strptime(raw, "%d/%m/%Y").date()
