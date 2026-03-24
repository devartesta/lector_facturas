from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import re

from pypdf import PdfReader


TWOPLACES = Decimal("0.01")


@dataclass(frozen=True)
class HannunInvoice:
    invoice_number: str
    invoice_date: date
    issuer_company_name: str
    billed_company_name: str
    supplier_code: str
    supplier_name: str
    destination_path: str
    division_invoice: str
    billing_period_start: date | None
    billing_period_end: date | None
    period_yyyymm: str
    vat_percent: Decimal
    gross_amount: Decimal
    vat_amount: Decimal
    net_amount: Decimal
    currency_code: str
    original_filename: str
    sender_email: str
    source_subject: str
    parser_name: str = "hannun_invoice"
    parser_confidence: Decimal = Decimal("0.9950")

    @property
    def document_type(self) -> str:
        return "invoice"

    @property
    def extracted_raw(self) -> dict[str, object]:
        return {
            "invoice_number": self.invoice_number,
            "invoice_date": self.invoice_date.isoformat(),
            "issuer_company_name": self.issuer_company_name,
            "billed_company_name": self.billed_company_name,
            "supplier_code": self.supplier_code,
            "supplier_name": self.supplier_name,
            "destination_path": self.destination_path,
            "division_invoice": self.division_invoice,
            "billing_period_start": self.billing_period_start.isoformat() if self.billing_period_start else None,
            "billing_period_end": self.billing_period_end.isoformat() if self.billing_period_end else None,
            "period_yyyymm": self.period_yyyymm,
            "vat_percent": format(self.vat_percent, "f"),
            "gross_amount": format(self.gross_amount, "f"),
            "vat_amount": format(self.vat_amount, "f"),
            "net_amount": format(self.net_amount, "f"),
            "currency_code": self.currency_code,
            "sender_email": self.sender_email,
            "source_subject": self.source_subject,
        }


def parse_hannun_pdf(path: Path, *, forced_period_yyyymm: str | None = None) -> HannunInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_hannun_text(text, original_filename=path.name, forced_period_yyyymm=forced_period_yyyymm)


def parse_hannun_text(text: str, *, original_filename: str, forced_period_yyyymm: str | None = None) -> HannunInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_number = _extract_invoice_number(normalized)
    invoice_date = _extract_invoice_date(normalized)
    lower = normalized.lower()
    if "factura de venta:" in lower:
        net_amount, vat_amount, gross_amount = _extract_summary_block_amounts(normalized)
    else:
        net_amount = _extract_money(normalized, "Total Base Imponible")
        vat_amount = _extract_money(normalized, "Total IVA")
        gross_amount = _extract_money(normalized, "TOTAL")
    vat_percent = _extract_vat_percent(normalized)

    if "factura de venta:" in lower:
        issuer_company_name = "HANNUN, S.A."
        billed_company_name = "ARTESTA STORE, S.L."
        supplier_code = "HANNUN"
        supplier_name = "HANNUN"
        sender_email = "tienda@hannun.com"
        source_subject = original_filename
        if "uso oficina bcn" in lower:
            division_invoice = "office"
        elif "servicios andrea" in lower:
            division_invoice = "services"
        else:
            division_invoice = "administration"
        destination_path = "expenses/opex/administration"
    else:
        issuer_company_name = "ARTESTA STORE, S.L."
        billed_company_name = "HANNUN, S.A."
        supplier_code = "HANNUN"
        supplier_name = "HANNUN"
        sender_email = "administracion@hannun.com"
        source_subject = original_filename
        if "renting cnc" in lower:
            division_invoice = "renting_cnc"
            destination_path = "income/shared-services"
        elif "shared services staff" in lower:
            division_invoice = "staff"
            destination_path = "income/shared-services"
        elif "servicios profesionales" in lower:
            division_invoice = "services"
            destination_path = "income/shared-services"
        elif "com/" in lower:
            division_invoice = "orders"
            destination_path = "income/sales/marketplaces"
        else:
            division_invoice = "other"
            destination_path = "income/other_income/other"

    billing_period_start, billing_period_end = _infer_period(normalized, invoice_date, division_invoice)
    period_yyyymm = forced_period_yyyymm or (billing_period_end.strftime("%Y%m") if billing_period_end else invoice_date.strftime("%Y%m"))

    return HannunInvoice(
        invoice_number=invoice_number,
        invoice_date=invoice_date,
        issuer_company_name=issuer_company_name,
        billed_company_name=billed_company_name,
        supplier_code=supplier_code,
        supplier_name=supplier_name,
        destination_path=destination_path,
        division_invoice=division_invoice,
        billing_period_start=billing_period_start,
        billing_period_end=billing_period_end,
        period_yyyymm=period_yyyymm,
        vat_percent=vat_percent,
        gross_amount=gross_amount,
        vat_amount=vat_amount,
        net_amount=net_amount,
        currency_code="EUR",
        original_filename=original_filename,
        sender_email=sender_email,
        source_subject=original_filename,
    )


def _extract_invoice_number(text: str) -> str:
    match = re.search(r"Número de factura:\s*([A-Z0-9/\-]+)", text, flags=re.IGNORECASE)
    if not match:
        match = re.search(r"VTA/\d{2}-\d{6}|COM/\d{2}-\d{6}|[A-Z]{0,2}\d{4}-\d{4}", text)
    if not match:
        raise ValueError("Could not extract invoice number.")
    return match.group(1) if match.lastindex else match.group(0)


def _extract_invoice_date(text: str) -> date:
    for pattern in (r"Fecha de factura:\s*([0-9]{2}/[0-9]{2}/[0-9]{4})", r"\n([0-9]{2}/[0-9]{2}/[0-9]{4})\nFACTURA"):
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return datetime.strptime(match.group(1), "%d/%m/%Y").date()
    raise ValueError("Could not extract invoice date.")


def _extract_money(text: str, label: str) -> Decimal:
    match = re.search(rf"{re.escape(label)}:\s*([0-9\.\,]+)\s*€", text, flags=re.IGNORECASE)
    if not match:
        raise ValueError(f"Could not extract {label}.")
    return _parse_decimal_es(match.group(1))


def _extract_vat_percent(text: str) -> Decimal:
    match = re.search(r"IVA\s+([0-9]{1,2}[\,\.]?[0-9]{0,2})%?", text, flags=re.IGNORECASE)
    if not match:
        return Decimal("0.0000")
    return Decimal(match.group(1).replace(",", ".")).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def _extract_summary_block_amounts(text: str) -> tuple[Decimal, Decimal, Decimal]:
    match = re.search(
        r"Resumen\s+Base imponible\s+Importe IVA\s+Importe total\s+([0-9\.\,]+)\s+([0-9\.\,]+)\s+([0-9\.\,]+)\s+€",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        raise ValueError("Could not extract summary block amounts.")
    return (
        _parse_decimal_es(match.group(1)),
        _parse_decimal_es(match.group(2)),
        _parse_decimal_es(match.group(3)),
    )


def _infer_period(text: str, invoice_date: date, division_invoice: str) -> tuple[date | None, date | None]:
    lower = text.lower()
    month_map = {
        "enero": 1,
        "febrero": 2,
        "january": 1,
        "february": 2,
    }
    for month_name, month_number in month_map.items():
        if month_name in lower:
            year = invoice_date.year if month_number <= invoice_date.month else invoice_date.year - 1
            start = date(year, month_number, 1)
            if month_number == 12:
                end = date(year, 12, 31)
            else:
                next_month = date(year + (1 if month_number == 12 else 0), 1 if month_number == 12 else month_number + 1, 1)
                end = next_month.fromordinal(next_month.toordinal() - 1)
            return start, end
    return invoice_date.replace(day=1), invoice_date


def _parse_decimal_es(raw: str) -> Decimal:
    return Decimal(raw.replace(".", "").replace(",", ".")).quantize(TWOPLACES, rounding=ROUND_HALF_UP)
