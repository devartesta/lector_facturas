from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
import calendar
import re

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA STORE, S.L."
ISSUER_COMPANY_NAME = "SOCIEDAD EST. CORREOS Y TELEGRAFOS, S.A., S.M.E."


@dataclass(frozen=True)
class CorreosTaxBreakdown:
    tax_label: str
    vat_percent: Decimal
    net_amount: Decimal
    vat_amount: Decimal

    @property
    def gross_amount(self) -> Decimal:
        return self.net_amount + self.vat_amount


@dataclass(frozen=True)
class CorreosInvoice:
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
    parser_name: str = "correos"
    parser_confidence: Decimal = Decimal("0.9970")
    tax_breakdowns: tuple[CorreosTaxBreakdown, ...] = ()

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
            "tax_label": "IGIC" if self.supplier_code == "CORREOSCAN" else "IVA",
            "tax_breakdowns": [
                {
                    "tax_label": item.tax_label,
                    "vat_percent": format(item.vat_percent, "f"),
                    "net_amount": format(item.net_amount, "f"),
                    "vat_amount": format(item.vat_amount, "f"),
                    "gross_amount": format(item.gross_amount, "f"),
                }
                for item in self.tax_breakdowns
            ],
        }


def parse_correos_pdf(path: Path) -> CorreosInvoice:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_correos_text(text, original_filename=path.name)


def parse_correos_text(text: str, *, original_filename: str) -> CorreosInvoice:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    invoice_number = _extract_invoice_number(normalized)
    invoice_date = _extract_invoice_date(normalized)
    billing_period_start, billing_period_end = _extract_period_range(normalized, invoice_date=invoice_date)
    tax_label, vat_percent, net_amount, vat_amount, gross_amount, tax_breakdowns = _extract_totals(normalized)
    supplier_code = "CORREOSCAN" if tax_label == "IGIC" else "CORREOS"
    return CorreosInvoice(
        supplier_code=supplier_code,
        supplier_name=supplier_code,
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
        tax_breakdowns=tax_breakdowns,
    )


def _extract_invoice_number(text: str) -> str:
    match = re.search(r"\b[0-9]{2}\.[0-9]{2}\.[0-9]{4}\s+([0-9]{10})\b", text)
    if not match:
        match = re.search(r"N[º°] FACTURA\s*\n([0-9]{10})", text, flags=re.IGNORECASE)
    if not match:
        raise ValueError("Could not extract Correos invoice number.")
    return match.group(1)


def _extract_invoice_date(text: str) -> date:
    match = re.search(r"\b([0-9]{2}\.[0-9]{2}\.[0-9]{4})\s+[0-9]{10}\b", text)
    if not match:
        match = re.search(r"FECHA\s*\n([0-9]{2}\.[0-9]{2}\.[0-9]{4})", text, flags=re.IGNORECASE)
    if not match:
        raise ValueError("Could not extract Correos invoice date.")
    return datetime.strptime(match.group(1), "%d.%m.%Y").date()


def _extract_period_range(text: str, *, invoice_date: date) -> tuple[date, date]:
    match = re.search(r"([0-9]{2}\.[0-9]{2}\.[0-9]{4})/([0-9]{2}\.[0-9]{2}\.[0-9]{4})", text)
    if match:
        return (
            datetime.strptime(match.group(1), "%d.%m.%Y").date(),
            datetime.strptime(match.group(2), "%d.%m.%Y").date(),
        )
    month_start = date(invoice_date.year, invoice_date.month, 1)
    month_end = date(invoice_date.year, invoice_date.month, calendar.monthrange(invoice_date.year, invoice_date.month)[1])
    return month_start, month_end


def _extract_totals(text: str) -> tuple[str, Decimal, Decimal, Decimal, Decimal, tuple[CorreosTaxBreakdown, ...]]:
    tax_breakdowns = _extract_tax_breakdowns(text)
    net_total_match = re.search(r"Total importe neto antes de impuesto\s+([0-9.,]+)", text, flags=re.IGNORECASE)
    vat_total_match = re.search(r"Total impuesto\s+([0-9.,]+)", text, flags=re.IGNORECASE)
    gross_match = re.search(r"Total factura en Euros\s+([0-9.,]+)", text, flags=re.IGNORECASE)
    if not tax_breakdowns or not gross_match:
        raise ValueError("Could not extract Correos totals.")
    tax_labels = {item.tax_label for item in tax_breakdowns}
    tax_label = "IGIC" if "IGIC" in tax_labels else next(iter(tax_labels))
    taxable_breakdowns = [item for item in tax_breakdowns if item.vat_percent > 0]
    vat_percent = taxable_breakdowns[0].vat_percent if taxable_breakdowns else tax_breakdowns[0].vat_percent
    declared_net_amount = _parse_decimal(net_total_match.group(1)) if net_total_match else None
    declared_vat_amount = _parse_decimal(vat_total_match.group(1)) if vat_total_match else None
    return (
        tax_label,
        vat_percent,
        declared_net_amount if declared_net_amount is not None else sum((item.net_amount for item in tax_breakdowns), Decimal("0.00")),
        declared_vat_amount if declared_vat_amount is not None else sum((item.vat_amount for item in tax_breakdowns), Decimal("0.00")),
        _parse_decimal(gross_match.group(1)),
        tuple(tax_breakdowns),
    )


def _extract_tax_breakdowns(text: str) -> list[CorreosTaxBreakdown]:
    breakdowns: list[CorreosTaxBreakdown] = []
    pattern = re.compile(
        r"Base imponible sujeta a impuesto \((IVA|IGIC)\)\s+([0-9.,]+)"
        r".*?Tipo impositivo:\s*(?:(IVA|IGIC)\s+repercutido\s*)?(EXENTO|[0-9.,]+)\s*%?"
        r".*?Cuota:\s*([0-9.,]+)",
        flags=re.IGNORECASE | re.DOTALL,
    )
    summary_text = text.split("Totales", 1)[0]
    for match in pattern.finditer(summary_text):
        rate_raw = match.group(4).upper()
        breakdowns.append(
            CorreosTaxBreakdown(
                tax_label=match.group(1).upper(),
                vat_percent=Decimal("0.00") if rate_raw == "EXENTO" else _parse_decimal(match.group(4)),
                net_amount=_parse_decimal(match.group(2)),
                vat_amount=_parse_decimal(match.group(5)),
            )
        )
    return breakdowns


def _period_with_most_days(start: date, end: date) -> str:
    month_counts: dict[str, int] = {}
    current = start
    while current <= end:
        key = current.strftime("%Y%m")
        month_counts[key] = month_counts.get(key, 0) + 1
        current = date.fromordinal(current.toordinal() + 1)
    return max(month_counts.items(), key=lambda item: (item[1], item[0]))[0]


def _parse_decimal(raw_value: str) -> Decimal:
    return Decimal(raw_value.replace(".", "").replace(",", "."))
