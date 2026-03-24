from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."


@dataclass(frozen=True)
class ArtestaIncomeInvoice:
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
    parser_name: str = "artesta_income"
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


def parse_qhands_pdf(path: Path) -> ArtestaIncomeInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_qhands_text(text, original_filename=path.name)


def parse_qhands_text(text: str, *, original_filename: str) -> ArtestaIncomeInvoice:
    return _parse_spanish_artesta_invoice(
        text,
        original_filename=original_filename,
        supplier_code="QHANDS",
        billed_company_name="QHANDS DESIGN SL.",
        division_invoice="renting_cnc",
        period_override="202602",
    )


def parse_rappel_pdf(path: Path) -> ArtestaIncomeInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_rappel_text(text, original_filename=path.name)


def parse_rappel_text(text: str, *, original_filename: str) -> ArtestaIncomeInvoice:
    return _parse_spanish_artesta_invoice(
        text,
        original_filename=original_filename,
        supplier_code="LIVITUM",
        billed_company_name="HOME DESIGN LABS S.L.",
        division_invoice="rappels",
        period_override="202601",
    )


def _parse_spanish_artesta_invoice(
    text: str,
    *,
    original_filename: str,
    supplier_code: str,
    billed_company_name: str,
    division_invoice: str,
    period_override: str | None = None,
) -> ArtestaIncomeInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_date = _parse_spanish_numeric_date(_extract(normalized, r"\n([0-9]{2}/[0-9]{2}/[0-9]{4})\nFACTURA"))
    invoice_number = _extract(normalized, r"N(?:ú|Ãº)mero de factura:\s*([A-Z0-9_-]+)")
    net_amount = _parse_decimal(_extract(normalized, r"Total Base Imponible:\s*([\-0-9.,]+)\s*"))
    vat_amount = _parse_decimal(_extract(normalized, r"Total IVA:\s*([\-0-9.,]+)\s*"))
    gross_amount = _parse_decimal(_extract(normalized, r"TOTAL:\s*([\-0-9.,]+)\s*"))
    vat_percent = Decimal(_extract(normalized, r"([0-9]{1,2})%"))
    period_yyyymm = period_override or invoice_date.strftime("%Y%m")
    billing_period_start = date(int(period_yyyymm[:4]), int(period_yyyymm[4:]), 1)
    billing_period_end = invoice_date
    return ArtestaIncomeInvoice(
        supplier_code=supplier_code,
        supplier_name=supplier_code,
        issuer_company_name=COMPANY_NAME,
        billed_company_name=billed_company_name,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=billing_period_start,
        billing_period_end=billing_period_end,
        period_yyyymm=period_yyyymm,
        currency_code="EUR",
        vat_percent=vat_percent,
        gross_amount=gross_amount,
        vat_amount=vat_amount,
        net_amount=net_amount,
        original_filename=original_filename,
        sender_email="info@artestastore.com",
        division_invoice=division_invoice,
    )


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract Artesta income field with pattern: {pattern}")
    return match.group(1).strip()


def _parse_decimal(raw: str) -> Decimal:
    return Decimal(raw.replace(".", "").replace(",", ".").replace("€", "").strip())


def _parse_spanish_numeric_date(raw: str) -> date:
    day, month, year = raw.split("/")
    return date(int(year), int(month), int(day))
