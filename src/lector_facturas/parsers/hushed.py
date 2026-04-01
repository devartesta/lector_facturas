from __future__ import annotations

import calendar
import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from pypdf import PdfReader


COMPANY_NAME = "ARTESTA INC"
ISSUER_COMPANY_NAME = "HUSHED C/O AFFINITYCLICK INC."
SUPPLIER_CODE = "HUSHED"

# Hushed invoice emails come from this sender
SENDER_EMAIL = "invoice+statements@hushed.com"

# Subject patterns:
#   "Your receipt from Hushed c/o AffinityClick Inc. #XXXX-XXXX"
#   "Your receipt from HUSHED.COM #XXXX-XXXX"
SUBJECT_RE = re.compile(
    r"Your receipt from (?:Hushed c/o AffinityClick Inc\.|HUSHED\.COM)\s+#(\d{4}-\d{4})",
    re.IGNORECASE,
)

# Filename patterns for Invoice PDFs and Receipt PDFs
INVOICE_FILENAME_RE = re.compile(r"^Invoice-[A-Z0-9]+-\d+\.pdf$", re.IGNORECASE)
RECEIPT_FILENAME_RE = re.compile(r"^Receipt-(\d{4}-\d{4})\.pdf$", re.IGNORECASE)

MONTHS = {
    "January": 1, "February": 2, "March": 3, "April": 4,
    "May": 5, "June": 6, "July": 7, "August": 8,
    "September": 9, "October": 10, "November": 11, "December": 12,
}


@dataclass(frozen=True)
class HushedInvoice:
    supplier_code: str
    supplier_name: str
    issuer_company_name: str
    billed_company_name: str
    invoice_number: str          # receipt number from subject/filename e.g. "2260-8475"
    internal_invoice_number: str # Hushed internal number e.g. "749E3EE3-0008"
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
    parser_name: str = "hushed"
    parser_confidence: Decimal = Decimal("0.9800")

    @property
    def extracted_raw(self) -> dict[str, object]:
        return {
            "issuer_company_name": self.issuer_company_name,
            "billed_company_name": self.billed_company_name,
            "internal_invoice_number": self.internal_invoice_number,
            "billing_period_start": self.billing_period_start.isoformat(),
            "billing_period_end": self.billing_period_end.isoformat(),
            "period_yyyymm": self.period_yyyymm,
            "currency_code": self.currency_code,
            "vat_percent": format(self.vat_percent, "f"),
            "gross_amount": format(self.gross_amount, "f"),
            "vat_amount": format(self.vat_amount, "f"),
            "net_amount": format(self.net_amount, "f"),
            "sender_email": self.sender_email,
        }


def parse_hushed_invoice_pdf(path: Path, *, receipt_number: str = "") -> HushedInvoice:
    """Parse a Hushed Invoice PDF (Invoice-XXXXXXXX-NNNN.pdf).

    Args:
        path: Path to the Invoice PDF.
        receipt_number: The receipt/order number from the email subject or Receipt filename
                        (e.g. "2260-8475").  If empty, the function attempts to derive it
                        from the PDF (not always possible for Invoice PDFs).
    """
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return _parse_hushed_invoice_text(text, original_filename=path.name, receipt_number=receipt_number)


def parse_hushed_receipt_pdf(path: Path) -> HushedInvoice:
    """Parse a Hushed Receipt PDF (Receipt-XXXX-XXXX.pdf).

    Extracts the receipt number from the filename and the invoice data from the PDF text.
    """
    m = RECEIPT_FILENAME_RE.match(path.name)
    receipt_number = m.group(1) if m else ""
    text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
    return _parse_hushed_invoice_text(text, original_filename=path.name, receipt_number=receipt_number)


def _parse_hushed_invoice_text(
    text: str, *, original_filename: str, receipt_number: str = ""
) -> HushedInvoice:
    """Core parser shared by Invoice and Receipt PDFs."""
    # Extract internal invoice number  e.g. "749E3EE3 0008"
    inv_num_m = re.search(r"Invoice number\s+([A-Z0-9]+ [0-9]+)", text, re.IGNORECASE)
    internal_number = inv_num_m.group(1).replace(" ", "-") if inv_num_m else ""

    # Extract date: "Date of issue September 20, 2025" (Invoice)
    #           or  "Date paid September 20, 2025"     (Receipt)
    date_m = re.search(
        r"(?:Date of issue|Date paid)\s+([A-Za-z]+ \d+,\s+\d{4})", text, re.IGNORECASE
    )
    if not date_m:
        raise ValueError("Could not extract date from Hushed PDF.")
    inv_date = _parse_english_date(date_m.group(1))

    # Extract amount: "$4.99 USD due ..." (Invoice) or from "Amount paid $4.99" (Receipt)
    amt_m = re.search(r"\$([0-9]+\.[0-9]{2})\s+USD due", text)
    if not amt_m:
        amt_m = re.search(r"Amount (?:due|paid)\s+\$([0-9]+\.[0-9]{2})", text)
    if not amt_m:
        amt_m = re.search(r"Total\s+\$([0-9]+\.[0-9]{2})", text)
    if not amt_m:
        raise ValueError("Could not extract amount from Hushed PDF.")
    gross_amount = Decimal(amt_m.group(1))

    # Billing period: description line e.g. "Sep 20 – Oct 20, 2025"
    # Try to parse from description; fall back to invoice_date month
    period_start = inv_date
    period_end_day = calendar.monthrange(inv_date.year, inv_date.month)[1]
    period_end = date(inv_date.year, inv_date.month, period_end_day)
    period_yyyymm = inv_date.strftime("%Y%m")

    # If receipt_number not supplied, try to extract from receipt PDF payment history line
    if not receipt_number:
        rcpt_m = re.search(r"Receipt number\s+([0-9]+ [0-9]+)", text, re.IGNORECASE)
        if rcpt_m:
            receipt_number = rcpt_m.group(1).replace(" ", "-")

    if not receipt_number:
        receipt_number = internal_number  # fall back to internal number

    return HushedInvoice(
        supplier_code=SUPPLIER_CODE,
        supplier_name=SUPPLIER_CODE,
        issuer_company_name=ISSUER_COMPANY_NAME,
        billed_company_name=COMPANY_NAME,
        invoice_number=receipt_number,
        internal_invoice_number=internal_number,
        invoice_date=inv_date,
        billing_period_start=period_start,
        billing_period_end=period_end,
        period_yyyymm=period_yyyymm,
        currency_code="USD",
        vat_percent=Decimal("0"),
        gross_amount=gross_amount,
        vat_amount=Decimal("0"),
        net_amount=gross_amount,
        original_filename=original_filename,
        sender_email=SENDER_EMAIL,
    )


def extract_receipt_number_from_subject(subject: str) -> str | None:
    """Extract the HUSHED receipt number (e.g. '2260-8475') from an email subject.

    Returns None if the subject does not look like a Hushed receipt email.
    """
    m = SUBJECT_RE.search(subject)
    return m.group(1) if m else None


def is_hushed_email(*, sender_email: str = "", subject: str = "") -> bool:
    """Return True if the email looks like a Hushed receipt notification."""
    if SENDER_EMAIL in sender_email.lower():
        return True
    if SUBJECT_RE.search(subject):
        return True
    return False


def _parse_english_date(raw: str) -> date:
    """Parse 'September 20, 2025' -> date(2025, 9, 20)."""
    raw = raw.replace(",", "").strip()
    parts = raw.split()
    if len(parts) != 3:
        raise ValueError(f"Unexpected date format: {raw!r}")
    month_num = MONTHS.get(parts[0])
    if not month_num:
        raise ValueError(f"Unknown month: {parts[0]!r}")
    return date(int(parts[2]), month_num, int(parts[1]))
