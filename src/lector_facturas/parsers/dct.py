from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from collections import Counter
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "DCT GMBH"
SUPPLIER_CODE = "DCT"


@dataclass(frozen=True)
class DctInvoice:
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
    parser_name: str = "dct"
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
        }


def parse_dct_pdf(path: Path) -> DctInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_dct_text(text, original_filename=path.name)


def parse_dct_text(text: str, *, original_filename: str) -> DctInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_number = _extract_invoice_number(normalized)
    invoice_date = _extract_invoice_date(normalized)
    billing_period_start, billing_period_end = _extract_period_range(normalized, invoice_date=invoice_date)
    vat_percent, net_amount, vat_amount, gross_amount = _extract_totals(normalized)
    return DctInvoice(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_CODE,
        issuer_company_name=ISSUER_COMPANY_NAME,
        billed_company_name=COMPANY_NAME,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=billing_period_start,
        billing_period_end=billing_period_end,
        period_yyyymm=_period_with_most_days(billing_period_start, billing_period_end),
        currency_code="EUR",
        vat_percent=vat_percent,
        net_amount=net_amount,
        vat_amount=vat_amount,
        gross_amount=gross_amount,
        original_filename=original_filename,
    )


def _extract_invoice_number(text: str) -> str:
    match = re.search(r"SAMMELRECHNUNG NR\.\s*([0-9]{2}-[0-9]{4})", text)
    if not match:
        raise ValueError("Could not extract DCT invoice number.")
    return match.group(1)


def _extract_invoice_date(text: str) -> date:
    match = re.search(r"Rechnungsdatum:\s*(\d{2}\.\d{2}\.\d{4})", text)
    if not match:
        raise ValueError("Could not extract DCT invoice date.")
    return datetime.strptime(match.group(1), "%d.%m.%Y").date()


def _extract_period_range(text: str, *, invoice_date: date) -> tuple[date, date]:
    match = re.search(r"(\d{2}\.\d{2}\.\d{4})\s*-\s*(\d{2}\.\d{2}\.\d{4})", text)
    if not match:
        return invoice_date, invoice_date
    return (
        datetime.strptime(match.group(1), "%d.%m.%Y").date(),
        datetime.strptime(match.group(2), "%d.%m.%Y").date(),
    )


def _extract_totals(text: str) -> tuple[Decimal, Decimal, Decimal, Decimal]:
    match = re.search(
        r"Nettobetrag:\s*([\d.,]+)\s*EUR\s+(\d+)\s*%\s*MwSt\s*([\d.,]+)\s*EUR\s+Gesamtbetrag:\s*([\d.,]+)\s*EUR",
        text,
    )
    if not match:
        raise ValueError("Could not extract DCT totals.")
    return (
        Decimal(match.group(2)),
        _parse_decimal(match.group(1)),
        _parse_decimal(match.group(3)),
        _parse_decimal(match.group(4)),
    )


def _period_with_most_days(start: date, end: date) -> str:
    counts: Counter[str] = Counter()
    current = start
    while current <= end:
        counts[current.strftime("%Y%m")] += 1
        current += timedelta(days=1)
    return max(counts.items(), key=lambda item: (item[1], item[0]))[0]


def _parse_decimal(raw_value: str) -> Decimal:
    return Decimal(raw_value.replace(".", "").replace(",", "."))
