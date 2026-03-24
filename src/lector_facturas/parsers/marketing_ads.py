from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import calendar
import re
import unicodedata

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
TWOPLACES = Decimal("0.01")
GOOGLE_ADS_CODE = "GOOGLEADS"
META_ADS_CODE = "METAADS"

ES_MONTHS = {
    "ene": 1,
    "feb": 2,
    "mar": 3,
    "abr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dic": 12,
}

EN_MONTHS_SHORT = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


@dataclass(frozen=True)
class MarketingInvoiceDivision:
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
    division_invoice: str
    original_filename: str
    sender_email: str
    parser_name: str
    parser_confidence: Decimal
    extracted_raw: dict[str, object]


def read_pdf_text(path: Path) -> str:
    return "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)


def parse_google_ads_pdf(path: Path) -> list[MarketingInvoiceDivision]:
    return parse_google_ads_text(read_pdf_text(path), original_filename=path.name)


def parse_meta_ads_pdf(path: Path) -> list[MarketingInvoiceDivision]:
    return parse_meta_ads_text(read_pdf_text(path), original_filename=path.name)


def parse_google_ads_text(text: str, *, original_filename: str) -> list[MarketingInvoiceDivision]:
    normalized = _normalize_text(text)
    invoice_number = _require_match(normalized, r"Numero de factura:\s*([0-9]{10})", "Google Ads invoice number")
    invoice_date = _parse_es_date(
        _require_match(normalized, r"Numero de factura:\s*[0-9]{10}.*?([0-9]{1,2}\s+[a-z]{3}\s+[0-9]{4})", "Google Ads invoice date")
    )
    period_match = re.search(
        r"Resumen de\s+([0-9]{1,2}\s+[a-z]{3}\s+[0-9]{4})\s+-\s+([0-9]{1,2}\s+[a-z]{3}\s+[0-9]{4})",
        normalized,
        flags=re.IGNORECASE,
    )
    if not period_match:
        raise ValueError("Could not extract Google Ads billing period.")
    billing_period_start = _parse_es_date(period_match.group(1))
    billing_period_end = _parse_es_date(period_match.group(2))
    total_amount = _extract_google_total_amount(normalized)
    fee_map = _extract_google_fee_map(normalized)
    line_totals = _extract_google_line_totals(normalized)
    line_sum = _quantize(sum(line_totals.values(), Decimal("0.00")))
    fee_sum = _quantize(sum(fee_map.values(), Decimal("0.00")))
    residual = _quantize(total_amount - line_sum - fee_sum)
    if residual != Decimal("0.00"):
        line_totals["eu"] = _quantize(line_totals["eu"] + residual)
    division_totals = {
        "uk": _quantize(line_totals["uk"] + fee_map["uk"]),
        "us": _quantize(line_totals["us"]),
        "eu": _quantize(line_totals["eu"] + fee_map["eu"]),
    }
    return _build_division_rows(
        supplier_code=GOOGLE_ADS_CODE,
        issuer_company_name="GOOGLE IRELAND LIMITED",
        billed_company_name=COMPANY_NAME,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=billing_period_start,
        billing_period_end=billing_period_end,
        currency_code="EUR",
        original_filename=original_filename,
        sender_email="collections@google.com",
        parser_name="google_ads",
        parser_confidence=Decimal("0.9960"),
        division_totals=division_totals,
        extracted_raw={
            "line_totals": _stringify_decimal_map(line_totals),
            "fee_totals": _stringify_decimal_map(fee_map),
            "total_amount": format(total_amount, "f"),
            "residual_to_eu": format(residual, "f"),
        },
    )


def parse_meta_ads_text(text: str, *, original_filename: str) -> list[MarketingInvoiceDivision]:
    normalized = _normalize_text(text)
    invoice_number = _require_match(normalized, r"Factura Numero:\s*([0-9]{9})", "Meta Ads invoice number")
    invoice_date = _parse_en_date(
        _require_match(normalized, r"Fecha de Factura:\s*([0-9]{2}-[A-Za-z]{3}-[0-9]{4})", "Meta Ads invoice date")
    )
    period_raw = _require_match(normalized, r"Periodo de facturacion:([A-Za-z]{3}-[0-9]{2})", "Meta Ads billing period")
    billing_period_start, billing_period_end = _parse_meta_period(period_raw)
    total_amount = _parse_decimal_en(
        _require_match(normalized, r"Total Factura:\s*([0-9,]+\.\d{2})", "Meta Ads total amount")
    )
    division_totals = _extract_meta_line_totals(normalized)
    division_sum = _quantize(sum(division_totals.values(), Decimal("0.00")))
    residual = _quantize(total_amount - division_sum)
    if residual != Decimal("0.00"):
        division_totals["eu"] = _quantize(division_totals["eu"] + residual)
    return _build_division_rows(
        supplier_code=META_ADS_CODE,
        issuer_company_name="META PLATFORMS IRELAND LIMITED",
        billed_company_name=COMPANY_NAME,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=billing_period_start,
        billing_period_end=billing_period_end,
        currency_code="EUR",
        original_filename=original_filename,
        sender_email="ar@meta.com",
        parser_name="meta_ads",
        parser_confidence=Decimal("0.9960"),
        division_totals=division_totals,
        extracted_raw={
            "total_amount": format(total_amount, "f"),
            "residual_to_eu": format(residual, "f"),
        },
    )


def _build_division_rows(
    *,
    supplier_code: str,
    issuer_company_name: str,
    billed_company_name: str,
    invoice_number: str,
    invoice_date: date,
    billing_period_start: date,
    billing_period_end: date,
    currency_code: str,
    original_filename: str,
    sender_email: str,
    parser_name: str,
    parser_confidence: Decimal,
    division_totals: dict[str, Decimal],
    extracted_raw: dict[str, object],
) -> list[MarketingInvoiceDivision]:
    period_yyyymm = _period_with_most_days(billing_period_start, billing_period_end)
    rows: list[MarketingInvoiceDivision] = []
    for division in ("uk", "us", "eu"):
        amount = _quantize(division_totals.get(division, Decimal("0.00")))
        if amount == Decimal("0.00"):
            continue
        rows.append(
            MarketingInvoiceDivision(
                supplier_code=supplier_code,
                supplier_name=supplier_code,
                issuer_company_name=issuer_company_name,
                billed_company_name=billed_company_name,
                invoice_number=invoice_number,
                invoice_date=invoice_date,
                billing_period_start=billing_period_start,
                billing_period_end=billing_period_end,
                period_yyyymm=period_yyyymm,
                currency_code=currency_code,
                vat_percent=Decimal("0.00"),
                gross_amount=amount,
                vat_amount=Decimal("0.00"),
                net_amount=amount,
                division_invoice=division,
                original_filename=original_filename,
                sender_email=sender_email,
                parser_name=parser_name,
                parser_confidence=parser_confidence,
                extracted_raw={
                    **extracted_raw,
                    "division_invoice": division,
                    "division_amount": format(amount, "f"),
                    "billing_period_start": billing_period_start.isoformat(),
                    "billing_period_end": billing_period_end.isoformat(),
                    "period_yyyymm": period_yyyymm,
                },
            )
        )
    return rows


def _extract_google_fee_map(text: str) -> dict[str, Decimal]:
    mapping = {
        "eu": [
            r"Impuesto sobre servicios digitales de Austria \*\s*([0-9.]+,\d{2})",
            r"Coste de operaciones normativo de Turquia \*\s*([0-9.]+,\d{2})",
            r"Coste de operaciones normativo de Espana \*\s*([0-9.]+,\d{2})",
            r"Coste de operaciones normativo de Francia \*\s*([0-9.]+,\d{2})",
            r"Coste de operaciones normativo de Italia \*\s*([0-9.]+,\d{2})",
        ],
        "uk": [
            r"Impuesto sobre servicios digitales del Reino Unido \*\s*([0-9.]+,\d{2})",
        ],
    }
    result = {"uk": Decimal("0.00"), "us": Decimal("0.00"), "eu": Decimal("0.00")}
    for division, patterns in mapping.items():
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                result[division] = _quantize(result[division] + _parse_decimal_es(match.group(1)))
    return result


def _extract_google_total_amount(text: str) -> Decimal:
    if "Fecha de vencimiento:" not in text:
        raise ValueError("Could not extract Google Ads total amount.")
    prefix = text.split("Fecha de vencimiento:", 1)[0]
    amounts = re.findall(r"[0-9.]+,\d{2}", prefix)
    if not amounts:
        raise ValueError("Could not extract Google Ads total amount.")
    return _parse_decimal_es(amounts[-1])


def _extract_google_line_totals(text: str) -> dict[str, Decimal]:
    result = {"uk": Decimal("0.00"), "us": Decimal("0.00"), "eu": Decimal("0.00")}
    for line in text.splitlines():
        clean = " ".join(line.split())
        match = re.match(r"(.+?)\s+\d[\d.]*\s+(Clics|Impresiones)\s+(-?[0-9.]+,\d{2})$", clean)
        if not match:
            continue
        description = match.group(1)
        amount = _parse_decimal_es(match.group(3))
        division = _classify_campaign_region(description)
        result[division] = _quantize(result[division] + amount)
    return result


def _extract_meta_line_totals(text: str) -> dict[str, Decimal]:
    result = {"uk": Decimal("0.00"), "us": Decimal("0.00"), "eu": Decimal("0.00")}
    for line in text.splitlines():
        clean = " ".join(line.split())
        match = re.match(r"\d+\s+(.+?)\s+([0-9,]+\.\d{2})$", clean)
        if not match:
            continue
        description = match.group(1)
        if "campaign" not in description.lower() and "GLOBAL" not in description:
            continue
        amount = _parse_decimal_en(match.group(2))
        division = _classify_campaign_region(description)
        result[division] = _quantize(result[division] + amount)
    return result


def _classify_campaign_region(description: str) -> str:
    normalized = f" {description.upper()} "
    if " UK " in normalized or "- UK " in normalized:
        return "uk"
    if " US " in normalized or "- US " in normalized:
        return "us"
    return "eu"


def _parse_meta_period(raw: str) -> tuple[date, date]:
    month_text, year_text = raw.split("-")
    month = EN_MONTHS_SHORT[month_text.lower()]
    year = 2000 + int(year_text)
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last_day)


def _period_with_most_days(start: date, end: date) -> str:
    counts: dict[str, int] = {}
    current = start
    while current <= end:
        key = current.strftime("%Y%m")
        counts[key] = counts.get(key, 0) + 1
        current = date.fromordinal(current.toordinal() + 1)
    return max(counts.items(), key=lambda item: (item[1], item[0]))[0]


def _normalize_text(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text.replace("\xa0", " ").replace("\r", ""))
    return "".join(character for character in normalized if not unicodedata.combining(character))


def _parse_es_date(raw: str) -> date:
    day_text, month_text, year_text = raw.strip().split()
    return date(int(year_text), ES_MONTHS[month_text.lower()], int(day_text))


def _parse_en_date(raw: str) -> date:
    return datetime.strptime(raw, "%d-%b-%Y").date()


def _require_match(text: str, pattern: str, label: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract {label}.")
    return match.group(1)


def _parse_decimal_es(raw: str) -> Decimal:
    return _quantize(Decimal(raw.replace(".", "").replace(",", ".")))


def _parse_decimal_en(raw: str) -> Decimal:
    return _quantize(Decimal(raw.replace(",", "")))


def _quantize(value: Decimal) -> Decimal:
    return value.quantize(TWOPLACES, rounding=ROUND_HALF_UP)


def _stringify_decimal_map(values: dict[str, Decimal]) -> dict[str, str]:
    return {key: format(value, "f") for key, value in values.items()}
