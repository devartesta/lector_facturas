from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import re

from pypdf import PdfReader


TWOPLACES = Decimal("0.01")
EU_COUNTRIES = {
    "Austria",
    "Belgium",
    "Bulgaria",
    "Croatia",
    "Cyprus",
    "Czech Republic",
    "Czechia",
    "Denmark",
    "Estonia",
    "Finland",
    "France",
    "Germany",
    "Greece",
    "Hungary",
    "Ireland",
    "Italy",
    "Latvia",
    "Lithuania",
    "Luxembourg",
    "Malta",
    "Netherlands",
    "Poland",
    "Portugal",
    "Romania",
    "Slovakia",
    "Slovenia",
    "Spain",
    "Sweden",
}


@dataclass(frozen=True)
class ArtistRoyaltyDocument:
    company_code: str
    supplier_code: str
    supplier_name: str
    invoice_number: str
    credit_note_number: str
    invoice_date: date
    billing_period_start: date
    billing_period_end: date
    period_yyyymm: str
    artist_name: str
    artist_tax_id: str
    artist_email: str
    artist_country: str
    artist_region_code: str
    payment_method: str
    gross_amount: Decimal
    withholding_percent: Decimal
    withholding_amount: Decimal
    net_amount: Decimal
    currency_code: str
    original_filename: str
    parser_name: str = "artist_royalties_pdf"
    parser_confidence: Decimal = Decimal("0.9950")

    @property
    def extracted_raw(self) -> dict[str, object]:
        return {
            "company_code": self.company_code,
            "supplier_code": self.supplier_code,
            "supplier_name": self.supplier_name,
            "invoice_number": self.invoice_number,
            "credit_note_number": self.credit_note_number,
            "invoice_date": self.invoice_date.isoformat(),
            "billing_period_start": self.billing_period_start.isoformat(),
            "billing_period_end": self.billing_period_end.isoformat(),
            "period_yyyymm": self.period_yyyymm,
            "artist_name": self.artist_name,
            "artist_tax_id": self.artist_tax_id,
            "artist_email": self.artist_email,
            "artist_country": self.artist_country,
            "artist_region_code": self.artist_region_code,
            "payment_method": self.payment_method,
            "gross_amount": format(self.gross_amount, "f"),
            "withholding_percent": format(self.withholding_percent, "f"),
            "withholding_amount": format(self.withholding_amount, "f"),
            "net_amount": format(self.net_amount, "f"),
            "currency_code": self.currency_code,
        }


@dataclass(frozen=True)
class ArtistRoyaltyMonthlySummary:
    company_code: str
    supplier_code: str
    summary_scope: str
    period_yyyymm: str
    posters_amount: Decimal
    stationery_amount: Decimal
    gross_amount: Decimal
    withholding_amount: Decimal
    withholding_percent: Decimal
    net_amount: Decimal
    paypal_amount: Decimal
    bank_transfer_amount: Decimal
    one_x_amount: Decimal
    source_filename: str

    @property
    def extracted_raw(self) -> dict[str, object]:
        return {
            "company_code": self.company_code,
            "supplier_code": self.supplier_code,
            "summary_scope": self.summary_scope,
            "period_yyyymm": self.period_yyyymm,
            "posters_amount": format(self.posters_amount, "f"),
            "stationery_amount": format(self.stationery_amount, "f"),
            "gross_amount": format(self.gross_amount, "f"),
            "withholding_amount": format(self.withholding_amount, "f"),
            "withholding_percent": format(self.withholding_percent, "f"),
            "net_amount": format(self.net_amount, "f"),
            "paypal_amount": format(self.paypal_amount, "f"),
            "bank_transfer_amount": format(self.bank_transfer_amount, "f"),
            "one_x_amount": format(self.one_x_amount, "f"),
            "source_filename": self.source_filename,
        }


def parse_artist_royalty_pdf(path: Path) -> ArtistRoyaltyDocument:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_artist_royalty_text(text, original_filename=path.name)


def parse_artist_royalty_text(text: str, *, original_filename: str) -> ArtistRoyaltyDocument:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_date = _extract_date(normalized, "Date")
    period_start, period_end = _extract_period_range(normalized)
    credit_note_number = _require_match(normalized, r"Credit note number:\s*([0-9]+\.[0-9]+\.[0-9]{4})", "credit note number")
    invoice_number = str(int(credit_note_number.split(".", 1)[0]))
    gross_amount = _parse_decimal(_require_match(normalized, r"Total\s+([0-9\.,]+ €)", "gross amount"))
    withholding_percent = _parse_percent(_require_match(normalized, r"Reduced Withholding Tax \(([0-9]+%)\)", "withholding percent"))
    withholding_amount = _parse_decimal(_require_match(normalized, r"Reduced Withholding Tax \([^)]+\)\s+[0-9]+%\s+([0-9\.,]+ €)", "withholding amount"))
    net_amount = _parse_decimal(_require_match(normalized, r"Net amount\s+([0-9\.,]+ €)", "net amount"))
    artist_block = _extract_artist_block(normalized)
    artist_name = artist_block[0]
    artist_country = artist_block[-1]
    artist_tax_id = _optional_match(normalized, r"Tax ID / VAT ID:\s*([^\n]+)").strip()
    artist_email = _require_match(normalized, r"Email:\s*([^\s]+@[^\s]+)", "artist email").strip()
    payment_method = _require_match(normalized, r"Payment method:\s*([^\n]+)", "payment method").strip()
    return ArtistRoyaltyDocument(
        company_code="SL",
        supplier_code="ROYALTIES",
        supplier_name="ARTIST ROYALTIES",
        invoice_number=invoice_number,
        credit_note_number=credit_note_number,
        invoice_date=invoice_date,
        billing_period_start=period_start,
        billing_period_end=period_end,
        period_yyyymm=period_end.strftime("%Y%m"),
        artist_name=artist_name,
        artist_tax_id=artist_tax_id,
        artist_email=artist_email,
        artist_country=artist_country,
        artist_region_code=_country_to_region_code(artist_country),
        payment_method=payment_method,
        gross_amount=gross_amount,
        withholding_percent=withholding_percent,
        withholding_amount=withholding_amount,
        net_amount=net_amount,
        currency_code="EUR",
        original_filename=original_filename,
    )


def parse_artist_royalties_summary_text(text: str, *, source_filename: str) -> list[ArtistRoyaltyMonthlySummary]:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    period_yyyymm = _extract_period_yyyymm(normalized)
    sections = [
        ("total", "🌍 TOTAL GENERAL"),
        ("uk", "🇬🇧 Reino Unido (UK)"),
        ("us", "🇺🇸 Estados Unidos (US)"),
        ("eu", "🇪🇺 Resto de Europa"),
    ]
    summaries: list[ArtistRoyaltyMonthlySummary] = []
    for scope, marker in sections:
        section_text = _extract_summary_section(normalized, marker)
        posters_amount = _parse_decimal(_require_match(section_text, r"Posters:\s+([0-9\.,]+ €)", f"{scope} posters"))
        stationery_amount = _parse_decimal(_require_match(section_text, r"Stationery:\s+([0-9\.,]+ €)", f"{scope} stationery"))
        gross_amount = _parse_decimal(_require_match(section_text, r"Total bruto:\s+([0-9\.,]+ €)", f"{scope} gross"))
        withholding_amount = _parse_decimal(_require_match(section_text, r"Impuestos:\s+([0-9\.,]+ €)", f"{scope} withholding"))
        withholding_percent = _parse_percent(_require_match(section_text, r"Impuestos:\s+[0-9\.,]+ € \(([0-9\.]+%)\)", f"{scope} withholding percent"))
        net_amount = _parse_decimal(_require_match(section_text, r"Total neto:\s+([0-9\.,]+ €)", f"{scope} net"))
        paypal_amount = _parse_decimal(_require_match(section_text, r"A pagar por PayPal:\s+([0-9\.,]+ €)", f"{scope} paypal"))
        bank_transfer_amount = _parse_decimal(_require_match(section_text, r"A pagar por transferencia:\s+([0-9\.,]+ €)", f"{scope} transfer"))
        one_x_amount = _parse_decimal(_require_match(section_text, r"A pagar a 1x:\s+([0-9\.,]+ €)", f"{scope} 1x"))
        summaries.append(
            ArtistRoyaltyMonthlySummary(
                company_code="SL",
                supplier_code="ROYALTIES",
                summary_scope=scope,
                period_yyyymm=period_yyyymm,
                posters_amount=posters_amount,
                stationery_amount=stationery_amount,
                gross_amount=gross_amount,
                withholding_amount=withholding_amount,
                withholding_percent=withholding_percent,
                net_amount=net_amount,
                paypal_amount=paypal_amount,
                bank_transfer_amount=bank_transfer_amount,
                one_x_amount=one_x_amount,
                source_filename=source_filename,
            )
        )
    return summaries


def _extract_artist_block(text: str) -> list[str]:
    match = re.search(r"Issuer\s+(.+?)\s+(?:Tax ID / VAT ID:|Email:)", text, flags=re.DOTALL)
    if not match:
        raise ValueError("Could not extract artist block.")
    lines = [line.strip() for line in match.group(1).splitlines() if line.strip()]
    if len(lines) < 2:
        raise ValueError("Artist block is too short.")
    return lines


def _extract_date(text: str, label: str) -> date:
    value = _require_match(text, rf"{label}:\s*([0-9]{{1,2}}/[0-9]{{1,2}}/[0-9]{{4}})", label)
    return datetime.strptime(value, "%d/%m/%Y").date()


def _extract_period_range(text: str) -> tuple[date, date]:
    match = re.search(r"Period:\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})\s*-\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{4})", text)
    if not match:
        raise ValueError("Could not extract billing period.")
    return (
        datetime.strptime(match.group(1), "%d/%m/%Y").date(),
        datetime.strptime(match.group(2), "%d/%m/%Y").date(),
    )


def _extract_period_yyyymm(text: str) -> str:
    match = re.search(r"Importe total a facturar en ([A-Za-z]+) ([0-9]{4})", text)
    if not match:
        raise ValueError("Could not extract summary period.")
    month = {
        "January": "01",
        "February": "02",
        "March": "03",
        "April": "04",
        "May": "05",
        "June": "06",
        "July": "07",
        "August": "08",
        "September": "09",
        "October": "10",
        "November": "11",
        "December": "12",
    }[match.group(1)]
    return f"{match.group(2)}{month}"


def _extract_summary_section(text: str, marker: str) -> str:
    start = text.find(marker)
    if start == -1:
        raise ValueError(f"Could not find summary section {marker!r}.")
    next_markers = ["🇬🇧 Reino Unido (UK)", "🇺🇸 Estados Unidos (US)", "🇪🇺 Resto de Europa"]
    if marker == "🌍 TOTAL GENERAL":
        next_candidates = [text.find(candidate, start + 1) for candidate in next_markers if text.find(candidate, start + 1) != -1]
        end = min(next_candidates) if next_candidates else len(text)
        return text[start:end]
    for candidate in next_markers:
        if candidate == marker:
            continue
        index = text.find(candidate, start + 1)
        if index != -1:
            return text[start:index]
    return text[start:]


def _country_to_region_code(country: str) -> str:
    normalized = country.strip()
    if normalized in {"United Kingdom", "UK"}:
        return "uk"
    if normalized in {"the United Kingdom"}:
        return "uk"
    if normalized in {"United States", "United States of America", "USA"}:
        return "us"
    if normalized in EU_COUNTRIES:
        return "eu"
    return "other"


def _require_match(text: str, pattern: str, label: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract {label}.")
    return match.group(1)


def _optional_match(text: str, pattern: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return ""
    return match.group(1)


def _parse_decimal(raw: str) -> Decimal:
    return Decimal(raw.replace("€", "").replace(".", "").replace(",", ".").strip()).quantize(TWOPLACES, rounding=ROUND_HALF_UP)


def _parse_percent(raw: str) -> Decimal:
    return Decimal(raw.replace("%", "").replace(",", ".").strip()).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
