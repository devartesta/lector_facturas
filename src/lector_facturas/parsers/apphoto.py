from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
INDUSTRIES_NAME = "A P PHOTO INDUSTRIES, S.L."
CANARIAS_NAME = "A P PHOTO CANARIAS, S.L."

INDUSTRIES_TOTALS_MARKER = "BASE IMPONIBLETOTAL NETO TOTAL FACTURA%I.V.A.CUOTA I.V.A. EUR"
CANARIAS_TOTALS_MARKER = "CUOTA IGIC%IGICTOTAL NETO BASE IMPONIBLE TOTAL FACTURA"


@dataclass(frozen=True)
class ApphotoInvoice:
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
    vat_percent: Decimal | None
    gross_amount: Decimal | None
    vat_amount: Decimal | None
    net_amount: Decimal | None
    original_filename: str
    sender_emails: tuple[str, ...]
    parser_name: str = "apphoto"
    parser_confidence: Decimal = Decimal("0.9900")

    @property
    def extracted_raw(self) -> dict[str, object]:
        return {
            "issuer_company_name": self.issuer_company_name,
            "billed_company_name": self.billed_company_name,
            "sender_emails": list(self.sender_emails),
            "billing_period_start": self.billing_period_start.isoformat(),
            "billing_period_end": self.billing_period_end.isoformat(),
            "period_yyyymm": self.period_yyyymm,
            "currency_code": self.currency_code,
            "vat_percent": _decimal_to_str(self.vat_percent),
            "gross_amount": _decimal_to_str(self.gross_amount),
            "vat_amount": _decimal_to_str(self.vat_amount),
            "net_amount": _decimal_to_str(self.net_amount),
        }


def parse_apphoto_pdf(path: Path) -> ApphotoInvoice:
    text = extract_pdf_text(path)
    return parse_apphoto_text(text, original_filename=path.name)


def extract_pdf_text(path: Path) -> str:
    return "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)


def parse_apphoto_text(text: str, *, original_filename: str) -> ApphotoInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    issuer_company_name, supplier_code = _detect_issuer(normalized)
    sender_emails = tuple(sorted(set(re.findall(r"[\w.+-]+@[\w.-]+\.\w+", normalized, flags=re.IGNORECASE))))
    invoice_date = _extract_invoice_date(normalized)
    invoice_number = _extract_invoice_number(normalized, supplier_code=supplier_code)
    line_dates = _extract_line_dates(normalized, supplier_code=supplier_code)
    if line_dates:
        billing_period_start = min(line_dates)
        billing_period_end = max(line_dates)
        period_yyyymm = _period_with_most_dates(line_dates)
    else:
        billing_period_start = invoice_date
        billing_period_end = invoice_date
        period_yyyymm = invoice_date.strftime("%Y%m")
    vat_percent, gross_amount, vat_amount, net_amount = _extract_totals(normalized, supplier_code=supplier_code)
    return ApphotoInvoice(
        supplier_code=supplier_code,
        supplier_name=supplier_code,
        issuer_company_name=issuer_company_name,
        billed_company_name=COMPANY_NAME,
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
        sender_emails=sender_emails,
    )


def _detect_issuer(text: str) -> tuple[str, str]:
    if CANARIAS_NAME in text:
        return CANARIAS_NAME, "APPHOTOCAN"
    return INDUSTRIES_NAME, "APPHOTOES"


def _extract_invoice_date(text: str) -> date:
    header = text[:2500]
    match = re.search(r"(\d{2}/\d{2}/\d{4})", header)
    if not match:
        raise ValueError("Could not extract invoice date from APPHOTO invoice.")
    return datetime.strptime(match.group(1), "%d/%m/%Y").date()


def _extract_invoice_number(text: str, *, supplier_code: str) -> str:
    header = text.split("D E S C", 1)[0][:2500]
    if supplier_code == "APPHOTOCAN":
        matches = re.findall(r"\d{2}\s*/\s*\d+\s*/\s*[\d.]+", header)
    else:
        matches = re.findall(r"\d+\s*-\s*[\d.]+", header)
    if not matches:
        raise ValueError("Could not extract invoice number from APPHOTO invoice.")
    return _normalise_invoice_number(matches[-1])


def _extract_line_dates(text: str, *, supplier_code: str) -> list[date]:
    patterns = [r"De Fecha:(\d{2}/\d{2}/\d{2})"]
    if supplier_code == "APPHOTOCAN":
        patterns.insert(0, r"(\d{2}/\d{2}/\d{2})Albaran Numero:De Fecha:")
    dates: list[date] = []
    for pattern in patterns:
        for raw in re.findall(pattern, text):
            try:
                dates.append(datetime.strptime(raw, "%d/%m/%y").date())
            except ValueError:
                continue
    return dates


def _period_with_most_dates(line_dates: list[date]) -> str:
    counts = Counter(item.strftime("%Y%m") for item in line_dates)
    return max(counts.items(), key=lambda item: (item[1], item[0]))[0]


def _extract_totals(text: str, *, supplier_code: str) -> tuple[Decimal | None, Decimal | None, Decimal | None, Decimal | None]:
    if supplier_code == "APPHOTOCAN":
        marker = CANARIAS_TOTALS_MARKER
        segment = _segment_after(text, marker, 120)
        numbers = re.findall(r"\d[\d.]*,\d{2}", segment)
        if len(numbers) >= 5:
            vat_percent = _parse_decimal(numbers[0])
            net_amount = _parse_decimal(numbers[2])
            gross_amount = _parse_decimal(numbers[3])
            vat_amount = _parse_decimal(numbers[4])
            return vat_percent, gross_amount, vat_amount, net_amount
    else:
        marker = INDUSTRIES_TOTALS_MARKER
        segment = _segment_after(text, marker, 120)
        numbers = re.findall(r"\d[\d.]*,\d{2}", segment)
        if len(numbers) >= 5:
            vat_amount = _parse_decimal(numbers[0])
            gross_amount = _parse_decimal(numbers[1])
            vat_percent = _parse_decimal(numbers[2])
            net_amount = _parse_decimal(numbers[4])
            return vat_percent, gross_amount, vat_amount, net_amount
    return None, None, None, None


def _segment_after(text: str, marker: str, tail_length: int) -> str:
    index = text.find(marker)
    if index == -1:
        return ""
    return text[index + len(marker): index + len(marker) + tail_length]


def _normalise_invoice_number(raw_value: str) -> str:
    clean = raw_value.replace(".", "").replace("/", "-")
    clean = re.sub(r"\s+", "", clean)
    clean = re.sub(r"-+", "-", clean)
    return clean.strip("-")


def _parse_decimal(raw_value: str) -> Decimal:
    return Decimal(raw_value.replace(".", "").replace(",", "."))


def _decimal_to_str(value: Decimal | None) -> str | None:
    return None if value is None else format(value, "f")
