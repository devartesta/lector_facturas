from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "NOTARIOS MONTE ESQUINZA 6 CB"
SUPPLIER_CODE = "NOTARIOSMONTESQUINZA"


@dataclass(frozen=True)
class NotariosMonteEsquinzaInvoice:
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
    exempt_amount: Decimal
    taxable_amount: Decimal
    withholding_percent: Decimal
    withholding_amount: Decimal
    payable_amount: Decimal
    parser_name: str = "notarios_monte_esquinza"
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
            "exempt_amount": format(self.exempt_amount, "f"),
            "taxable_amount": format(self.taxable_amount, "f"),
            "withholding_percent": format(self.withholding_percent, "f"),
            "withholding_amount": format(self.withholding_amount, "f"),
            "payable_amount": format(self.payable_amount, "f"),
        }


def parse_notarios_monte_esquinza_pdf(path: Path) -> NotariosMonteEsquinzaInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_notarios_monte_esquinza_text(text, original_filename=path.name)


def parse_notarios_monte_esquinza_text(text: str, *, original_filename: str) -> NotariosMonteEsquinzaInvoice:
    normalized = _decode_font_encoding(text)
    normalized = normalized.replace("\xa0", " ").replace("\r", "")
    normalized = re.sub(r"[ \t]+", " ", normalized)
    normalized = re.sub(r"\n{2,}", "\n", normalized)

    invoice_number = _extract_invoice_number(normalized, original_filename=original_filename)
    invoice_date = _extract_invoice_date(normalized)
    exempt_amount, taxable_amount, vat_percent, vat_amount, withholding_percent, withholding_amount, payable_amount = _extract_totals(normalized)

    # PYG expense is the cost excluding recoverable VAT. Exempt suplidos are
    # retained here because these invoices are classified directly in admin.
    net_amount = exempt_amount + taxable_amount
    gross_amount = net_amount + vat_amount
    return NotariosMonteEsquinzaInvoice(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_CODE,
        issuer_company_name=ISSUER_COMPANY_NAME,
        billed_company_name=COMPANY_NAME,
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        billing_period_start=invoice_date,
        billing_period_end=invoice_date,
        period_yyyymm=invoice_date.strftime("%Y%m"),
        currency_code="EUR",
        vat_percent=vat_percent,
        gross_amount=gross_amount,
        vat_amount=vat_amount,
        net_amount=net_amount,
        original_filename=original_filename,
        sender_email="notarios@menotarios.com",
        exempt_amount=exempt_amount,
        taxable_amount=taxable_amount,
        withholding_percent=withholding_percent,
        withholding_amount=withholding_amount,
        payable_amount=payable_amount,
    )


def _decode_font_encoding(text: str) -> str:
    # These PDFs encode digits as control characters in the extracted text.
    replacements = {chr(0x13 + digit): str(digit) for digit in range(10)}
    replacements.update({"\x0f": ",", "\x0e": ".", "\x03": " "})
    for source, target in replacements.items():
        text = text.replace(source, target)
    return text


def _extract_invoice_number(text: str, *, original_filename: str) -> str:
    match = re.search(r"Factura\s*[:\x1d]?\s*((?:[A-Z]-?)*[A-Z])\s*([0-9]+)", text, flags=re.IGNORECASE)
    if not match:
        match = re.search(r"\b(I-?L|L)[-_ ]?([0-9]+)", original_filename, flags=re.IGNORECASE)
    if not match:
        raise ValueError("Could not extract Notarios Monte Esquinza invoice number.")
    return f"{match.group(1).upper().replace(' ', '')}{match.group(2)}"


def _extract_invoice_date(text: str) -> date:
    match = re.search(r"Fecha(?:\s+Factura)?\s*:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})", text, flags=re.IGNORECASE)
    if not match:
        raise ValueError("Could not extract Notarios Monte Esquinza invoice date.")
    return datetime.strptime(match.group(1), "%d/%m/%Y").date()


def _extract_totals(text: str) -> tuple[Decimal, Decimal, Decimal, Decimal, Decimal, Decimal, Decimal]:
    money_suffix = r"(?:[A-Z]{3}|[?€])?"
    pattern = re.compile(
        r"Base\s+Exenta\s+IVA\s+Base\s+Imponible\s+Impuestos\s+"
        rf"([0-9.,]+)\s*{money_suffix}\s+([0-9.,]+)\s*{money_suffix}\s*"
        rf"IVA\s*\(\s*([0-9.,]+)\s*%\s*\)\s+([0-9.,]+)\s*{money_suffix}\s*"
        rf"([0-9.,]+)\s*{money_suffix}\s*RETENCION\s*\(\s*([0-9.,]+)\s*%\s*\)\s*-\s*([0-9.,]+)\s*{money_suffix}\s*"
        r"IMPORTE[^\n]*?([0-9.,]+)",
        flags=re.IGNORECASE | re.DOTALL,
    )
    match = pattern.search(text)
    if not match:
        summary_start = text.find("Base Exenta")
        if summary_start < 0:
            raise ValueError("Could not extract Notarios Monte Esquinza totals.")
        summary = text[summary_start:]
        summary = summary.split("FIRMA", 1)[0]
        percentages = list(re.finditer(r"([0-9]+,[0-9]+)\s*%", summary))
        if len(percentages) < 2:
            raise ValueError("Could not extract Notarios Monte Esquinza totals.")
        first_percent = percentages[0]
        second_percent = percentages[1]
        before_vat = re.findall(r"-?[0-9]+,[0-9]{2}", summary[: first_percent.start()])
        vat_after = re.search(r"-?[0-9]+,[0-9]{2}", summary[first_percent.end() :])
        withholding_after = re.search(r"-?[0-9]+,[0-9]{2}", summary[second_percent.end() :])
        payable_match = re.search(r"IMPORTE.*?(-?[0-9]+,[0-9]{2})", summary, flags=re.IGNORECASE | re.DOTALL)
        if not before_vat or len(before_vat) < 2 or not vat_after or not withholding_after or not payable_match:
            raise ValueError("Could not extract Notarios Monte Esquinza totals.")
        return (
            _parse_euro(before_vat[0]),
            _parse_euro(before_vat[1]),
            _parse_euro(first_percent.group(1)),
            _parse_euro(vat_after.group(0)),
            _parse_euro(second_percent.group(1)),
            abs(_parse_euro(withholding_after.group(0))),
            _parse_euro(payable_match.group(1)),
        )
    exempt_amount = _parse_euro(match.group(1))
    taxable_amount = _parse_euro(match.group(2))
    vat_percent = _parse_euro(match.group(3))
    vat_amount = _parse_euro(match.group(4))
    withholding_percent = _parse_euro(match.group(6))
    withholding_amount = _parse_euro(match.group(7))
    payable_amount = _parse_euro(match.group(8))
    return (
        exempt_amount,
        taxable_amount,
        vat_percent,
        vat_amount,
        withholding_percent,
        withholding_amount,
        payable_amount,
    )


def _parse_euro(raw: str) -> Decimal:
    return Decimal(raw.replace(".", "").replace(",", "."))
