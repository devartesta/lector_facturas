from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
import re

from pypdf import PdfReader


TWOPLACES = Decimal("0.01")


@dataclass(frozen=True)
class PayrollSummary:
    company_code: str
    provider_code: str
    provider_name: str
    payroll_period_start: date
    payroll_period_end: date
    period_yyyymm: str
    employee_count: int
    gross_pay_amount: Decimal
    employee_deductions_amount: Decimal
    net_pay_amount: Decimal
    employer_social_security_amount: Decimal
    total_company_cost_amount: Decimal
    social_security_liquidation_amount: Decimal
    tax_withholdings_amount: Decimal
    currency_code: str
    original_filename: str
    parser_name: str = "payroll_summary"
    parser_confidence: Decimal = Decimal("0.9950")

    @property
    def extracted_raw(self) -> dict[str, object]:
        return {
            "company_code": self.company_code,
            "provider_code": self.provider_code,
            "provider_name": self.provider_name,
            "payroll_period_start": self.payroll_period_start.isoformat(),
            "payroll_period_end": self.payroll_period_end.isoformat(),
            "period_yyyymm": self.period_yyyymm,
            "employee_count": self.employee_count,
            "gross_pay_amount": format(self.gross_pay_amount, "f"),
            "employee_deductions_amount": format(self.employee_deductions_amount, "f"),
            "net_pay_amount": format(self.net_pay_amount, "f"),
            "employer_social_security_amount": format(self.employer_social_security_amount, "f"),
            "total_company_cost_amount": format(self.total_company_cost_amount, "f"),
            "social_security_liquidation_amount": format(self.social_security_liquidation_amount, "f"),
            "tax_withholdings_amount": format(self.tax_withholdings_amount, "f"),
            "currency_code": self.currency_code,
        }


def parse_payroll_summary_pdf(path: Path) -> PayrollSummary:
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return parse_payroll_summary_text(text, original_filename=path.name)


def parse_payroll_summary_text(text: str, *, original_filename: str) -> PayrollSummary:
    normalized = text.replace("\xa0", " ").replace("\r", "")
    period_start, period_end = _extract_period_range(normalized)
    employee_count = int(_require_match(normalized, r"TOTAL TRABAJADORES EMPRESA\s+(\d+)", "employee count"))

    company_total_line = _find_line_containing(normalized, "TOTAL EMPRESA")
    company_total_numbers = _extract_decimals_from_line(company_total_line)
    if len(company_total_numbers) < 3:
        raise ValueError("Could not extract payroll employer totals.")
    employee_deductions_amount = abs(company_total_numbers[0])
    employer_social_security_amount = company_total_numbers[1]
    total_company_cost_amount = company_total_numbers[2]

    summary_totals_line = _line_after(normalized, company_total_line)
    summary_totals_numbers = _extract_decimals_from_line(summary_totals_line)
    if len(summary_totals_numbers) < 4:
        raise ValueError("Could not extract payroll summary totals.")
    gross_pay_amount = summary_totals_numbers[0]
    tax_withholdings_amount = abs(summary_totals_numbers[1])
    social_security_liquidation_amount = summary_totals_numbers[2]
    net_pay_amount = summary_totals_numbers[3]

    return PayrollSummary(
        company_code="SL",
        provider_code="DOSCONSULTING",
        provider_name="DOS CONSULTING",
        payroll_period_start=period_start,
        payroll_period_end=period_end,
        period_yyyymm=period_end.strftime("%Y%m"),
        employee_count=employee_count,
        gross_pay_amount=gross_pay_amount,
        employee_deductions_amount=employee_deductions_amount,
        net_pay_amount=net_pay_amount,
        employer_social_security_amount=employer_social_security_amount,
        total_company_cost_amount=total_company_cost_amount,
        social_security_liquidation_amount=social_security_liquidation_amount,
        tax_withholdings_amount=tax_withholdings_amount,
        currency_code="EUR",
        original_filename=original_filename,
    )


def _extract_period_range(text: str) -> tuple[date, date]:
    match = re.search(r"PAGA TOTAL DEL\s+([0-9]{2}/[0-9]{2}/[0-9]{4})\s+AL\s+([0-9]{2}/[0-9]{2}/[0-9]{4})", text)
    if not match:
        raise ValueError("Could not extract payroll period.")
    return (
        datetime.strptime(match.group(1), "%d/%m/%Y").date(),
        datetime.strptime(match.group(2), "%d/%m/%Y").date(),
    )


def _find_line_containing(text: str, marker: str) -> str:
    for line in text.splitlines():
        if marker in line:
            return line
    raise ValueError(f"Could not find line containing {marker!r}.")


def _line_after(text: str, target_line: str) -> str:
    found = False
    for line in text.splitlines():
        if not found:
            if line == target_line:
                found = True
            continue
        if line.strip():
            return line
    raise ValueError("Could not find line after payroll totals.")


def _extract_decimals_from_line(line: str) -> list[Decimal]:
    raw_values = re.findall(r"-?[0-9]{1,3}(?:\.[0-9]{3})*,[0-9]{2}", line)
    return [_parse_decimal_es(value) for value in raw_values]


def _require_match(text: str, pattern: str, label: str) -> str:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        raise ValueError(f"Could not extract {label}.")
    return match.group(1)


def _parse_decimal_es(raw: str) -> Decimal:
    return Decimal(raw.replace(".", "").replace(",", ".")).quantize(TWOPLACES, rounding=ROUND_HALF_UP)
