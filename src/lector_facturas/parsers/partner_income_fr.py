from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."


@dataclass(frozen=True)
class PartnerIncomeInvoice:
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
    parser_name: str = "partner_income_fr"
    parser_confidence: Decimal = Decimal("0.9940")

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


def parse_toasty_pdf(path: Path) -> PartnerIncomeInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_toasty_text(text, original_filename=path.name)


def parse_toasty_text(text: str, *, original_filename: str) -> PartnerIncomeInvoice:
    return _parse_partner_text(
        text,
        original_filename=original_filename,
        supplier_code="TOASTY",
        billed_company_name="TOASTY SAS",
        sender_email="simon@toasty.family",
        division_invoice="b2b_partner",
    )


def parse_choose_pdf(path: Path) -> PartnerIncomeInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_choose_text(text, original_filename=path.name)


def parse_choose_text(text: str, *, original_filename: str) -> PartnerIncomeInvoice:
    return _parse_partner_text(
        text,
        original_filename=original_filename,
        supplier_code="CHOOSE",
        billed_company_name="CHOOSE SAS",
        sender_email="facture@appchoose.io",
        division_invoice="campaign",
    )


def _parse_partner_text(
    text: str,
    *,
    original_filename: str,
    supplier_code: str,
    billed_company_name: str,
    sender_email: str,
    division_invoice: str,
) -> PartnerIncomeInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_number = _extract(normalized, r"COMMANDE NO\s+([A-Z0-9-]+)")
    invoice_date = _parse_date(_extract(normalized, r"DATE DE COMMANDE\s+([0-9]{4}/[0-9]{2}/[0-9]{2})"))
    gross_amount = _parse_eur(_extract(normalized, r"TOTAL TTC:\s*€\s*([0-9,]+\.[0-9]{2})"))
    billing_period_start = invoice_date
    billing_period_end = invoice_date
    return PartnerIncomeInvoice(
        supplier_code=supplier_code,
        supplier_name=supplier_code,
        issuer_company_name=COMPANY_NAME,
        billed_company_name=billed_company_name,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=billing_period_start,
        billing_period_end=billing_period_end,
        period_yyyymm=invoice_date.strftime("%Y%m"),
        currency_code="EUR",
        vat_percent=Decimal("0"),
        gross_amount=gross_amount,
        vat_amount=Decimal("0"),
        net_amount=gross_amount,
        original_filename=original_filename,
        sender_email=sender_email,
        division_invoice=division_invoice,
    )


def _extract(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract partner income field with pattern: {pattern}")
    return match.group(1).strip()


def _parse_date(raw: str) -> date:
    year, month, day = raw.split("/")
    return date(int(year), int(month), int(day))


def _parse_eur(raw: str) -> Decimal:
    return Decimal(raw.replace(",", ""))
