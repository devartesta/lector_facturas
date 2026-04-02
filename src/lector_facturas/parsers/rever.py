from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
COMPANY_NAME_LTD = "ARTESTA STORES (UK) LTD"
ISSUER_COMPANY_NAME = "HERMONT RTU, INC. SUCURSAL EN ESPANA"
SUPPLIER_CODE = "REVER"
SPANISH_MONTHS = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}
SHORT_SPANISH_MONTHS = {
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


@dataclass(frozen=True)
class ReverDocument:
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
    document_type: str
    division_invoice: str = ""
    parser_name: str = "rever"
    parser_confidence: Decimal = Decimal("0.9960")

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
            "document_type": self.document_type,
        }


def parse_rever_pdf(path: Path) -> ReverDocument:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    lowered = path.name.lower()
    if "suppliednote" in lowered or "suppli" in lowered:
        return parse_rever_supplied_note_text(text, original_filename=path.name)
    return parse_rever_invoice_text(text, original_filename=path.name)


def parse_rever_invoice_text(text: str, *, original_filename: str) -> ReverDocument:
    normalized = _normalize_text(text)
    invoice_number = _extract(
        normalized,
        r"(?:Numero de factura|Numero de factura|Invoice number)\s+([A-Z0-9\x00/-]+)",
    ).replace("\x00", "-")
    invoice_date = _parse_spanish_date(
        _extract(normalized, r"Fecha de emision\s+([0-9]{1,2} de [a-z]+ de [0-9]{4})")
    )
    period_match = re.search(
        r"REVER.?\s+Suscripcion Mensual\s+([0-9]{1,2} [a-z]{3} [0-9]{4})\s*[-–]\s*([0-9]{1,2} [a-z]{3} [0-9]{4})",
        normalized,
        flags=re.IGNORECASE,
    )
    if period_match:
        billing_period_start = _parse_short_spanish_date(period_match.group(1))
        billing_period_end = _parse_short_spanish_date(period_match.group(2))
    else:
        billing_period_start = invoice_date
        billing_period_end = invoice_date

    is_suplidos = "suplidos" in normalized.lower() or "supplied expenses" in normalized.lower()

    if is_suplidos:
        net_amount = _parse_decimal(_extract(normalized, r"Total sin impuestos\s+([0-9.,]+)\s*€"))
        vat_amount = Decimal("0")
        gross_amount = net_amount
        vat_percent = Decimal("0")
        # Suplidos invoices are issued in the following month; period = month before invoice date
        from datetime import timedelta
        first_of_invoice_month = invoice_date.replace(day=1)
        last_of_prev_month = first_of_invoice_month - timedelta(days=1)
        period_yyyymm = last_of_prev_month.strftime("%Y%m")
        if billing_period_start == invoice_date:  # no period parsed — use previous month
            billing_period_start = last_of_prev_month.replace(day=1)
            billing_period_end = last_of_prev_month
    elif "Cliente exento de impuestos" in normalized:
        net_amount = _parse_decimal(_extract(normalized, r"Subtotal\s+([0-9.,]+)\s*€"))
        vat_amount = Decimal("0")
        gross_amount = _parse_decimal(_extract(normalized, r"Total\s+([0-9.,]+)\s*€"))
        vat_percent = Decimal("0")
        period_yyyymm = invoice_date.strftime("%Y%m")
    else:
        net_amount = _parse_decimal(_extract(normalized, r"Total sin impuestos\s+([0-9.,]+)\s*€"))
        vat_line = _extract_line(normalized, "IVA")
        vat_amount = _parse_decimal(re.findall(r"([0-9.,]+)\s*€", vat_line)[-1])
        gross_line = _extract_total_line(normalized)
        gross_amount = _parse_decimal(re.findall(r"([0-9.,]+)\s*€", gross_line)[-1])
        vat_percent = Decimal("21")
        period_yyyymm = invoice_date.strftime("%Y%m")

    return ReverDocument(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_CODE,
        issuer_company_name=ISSUER_COMPANY_NAME,
        billed_company_name=_extract_billed_company_name(normalized),
        invoice_number=invoice_number.replace("/", "-"),
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
        sender_email="invoice@itsrever.com",
        document_type="invoice",
        division_invoice="suplidos" if is_suplidos else "",
    )


def parse_rever_supplied_note_text(text: str, *, original_filename: str) -> ReverDocument:
    normalized = _normalize_text(text)
    period_match = re.search(
        r"Billing Period:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})\s*-\s*([0-9]{2}/[0-9]{2}/[0-9]{4})",
        normalized,
        flags=re.IGNORECASE,
    )
    if not period_match:
        raise ValueError("Could not extract REVER supplied note billing period.")
    billing_period_start = _parse_slash_date(period_match.group(1))
    billing_period_end = _parse_slash_date(period_match.group(2))
    period_yyyymm = billing_period_end.strftime("%Y%m")

    amounts = re.search(
        r"Supplied Expenses\s+[0-9]+\s+€([0-9.,]+)\s+€0,00\s+€([0-9.,]+)",
        normalized,
        flags=re.IGNORECASE,
    )
    if not amounts:
        raise ValueError("Could not extract REVER supplied note amounts.")
    net_amount = _parse_decimal(amounts.group(1))
    gross_amount = _parse_decimal(amounts.group(2))

    return ReverDocument(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_CODE,
        issuer_company_name=ISSUER_COMPANY_NAME,
        billed_company_name=COMPANY_NAME,
        invoice_number=f"SUPPLIED-{period_yyyymm}",
        invoice_date=billing_period_end,
        billing_period_start=billing_period_start,
        billing_period_end=billing_period_end,
        period_yyyymm=period_yyyymm,
        currency_code="EUR",
        vat_percent=Decimal("0"),
        gross_amount=gross_amount,
        vat_amount=Decimal("0"),
        net_amount=net_amount,
        original_filename=original_filename,
        sender_email="invoice@itsrever.com",
        document_type="supplied_note",
    )


def _extract(text: str, pattern: str, *, group: int = 1) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract REVER field with pattern: {pattern}")
    return match.group(group).strip()


def _extract_line(text: str, prefix: str) -> str:
    for line in text.splitlines():
        if prefix in line:
            return line.strip()
    raise ValueError(f"Could not extract REVER line with prefix: {prefix}")


def _extract_total_line(text: str) -> str:
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("Total sin"):
            continue
        if stripped.startswith("Total ") or stripped == "Total":
            return stripped
    raise ValueError("Could not extract REVER total line.")


def _extract_billed_company_name(text: str) -> str:
    if "Artesta UK" in text or "GB VAT" in text:
        return COMPANY_NAME_LTD
    return COMPANY_NAME


def _parse_decimal(raw: str) -> Decimal:
    cleaned = raw.replace(" ", "").replace("\u2009", "").replace(".", "").replace(",", ".")
    return Decimal(cleaned)


def _normalize_text(text: str) -> str:
    normalized = (
        text.replace("\xa0", " ")
        .replace("\u2009", "")
        .replace("\r", "")
        .replace("\x00", "-")
        .replace("ÃƒÂ³", "ó")
        .replace("ÃƒÂ±", "ñ")
        .replace("ÃƒÂ¡", "á")
        .replace("ÃƒÂ©", "é")
        .replace("ÃƒÃ­", "í")
        .replace("ÃƒÂº", "ú")
        .replace("Ã¢â€šÂ¬", "€")
        .replace("Ã¢â‚¬â€œ", "–")
    )
    return (
        normalized.replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("ñ", "n")
    )


def _parse_spanish_date(raw: str) -> date:
    day_s, _, month_name, _, year_s = raw.split()
    return date(int(year_s), SPANISH_MONTHS[month_name.lower()], int(day_s))


def _parse_short_spanish_date(raw: str) -> date:
    day_s, month_s, year_s = raw.split()
    return date(int(year_s), SHORT_SPANISH_MONTHS[month_s.lower()], int(day_s))


def _parse_slash_date(raw: str) -> date:
    day_s, month_s, year_s = raw.split("/")
    return date(int(year_s), int(month_s), int(day_s))
