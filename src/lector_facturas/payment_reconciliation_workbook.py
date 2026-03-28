"""Build the payment reconciliation Excel workbook.

Two tabs (Shopify + PayPal), each with three sections:
  1. Solo en ventas (accounting entry, no payment match)
  2. Solo en pago   (payment charge, no accounting entry for the period)
  3. Diferencias    (both present but amounts differ)

Columns per section:
  Nº Pedido | Fecha | País | Moneda | Importe ventas | Importe pago | Diferencia
  | Link Shopify | Tarjeta regalo | Chargeback
"""
from __future__ import annotations

from datetime import datetime, UTC
from decimal import Decimal
from io import BytesIO
from typing import Any

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from lector_facturas.payment_reconciliation import (
    ChannelReconciliation,
    ReconciliationReport,
    ReconciliationRow,
)

# ---------------------------------------------------------------------------
# Styles
# ---------------------------------------------------------------------------

HEADER_FILL   = PatternFill("solid", fgColor="1F4E78")   # dark blue – title row
SECTION_FILL  = PatternFill("solid", fgColor="2E74B5")   # mid blue  – section header
COL_HDR_FILL  = PatternFill("solid", fgColor="D6E4F0")   # light blue – column headers
WARN_FILL     = PatternFill("solid", fgColor="FFF2CC")   # yellow    – diff row highlight
CHARGEBACK_FILL = PatternFill("solid", fgColor="FCE4D6") # orange    – chargeback

WHITE_BOLD    = Font(color="FFFFFF", bold=True, name="Calibri", size=10)
BOLD          = Font(bold=True, name="Calibri", size=10)
NORMAL        = Font(name="Calibri", size=10)
LINK_FONT     = Font(name="Calibri", size=10, color="0563C1", underline="single")
THIN_SIDE     = Side(style="thin", color="BFBFBF")
THIN          = Border(top=THIN_SIDE, bottom=THIN_SIDE, left=THIN_SIDE, right=THIN_SIDE)
CENTER        = Alignment(horizontal="center", vertical="center", wrap_text=False)
LEFT          = Alignment(horizontal="left",   vertical="center", wrap_text=False)
RIGHT         = Alignment(horizontal="right",  vertical="center", wrap_text=False)
MONEY_FMT     = '#,##0.00;[Red](#,##0.00);-'
ROW_H         = 14

MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]

COLUMNS = [
    ("Nº Pedido",       16, LEFT,   None),
    ("Fecha",           12, CENTER, None),
    ("País",             6, CENTER, None),
    ("Moneda",           7, CENTER, None),
    ("Importe ventas",  14, RIGHT,  MONEY_FMT),
    ("Importe pago",    14, RIGHT,  MONEY_FMT),
    ("Diferencia",      12, RIGHT,  MONEY_FMT),
    ("Link Shopify",    14, LEFT,   None),
    ("T. regalo",        9, CENTER, None),
    ("Chargeback",      11, CENTER, None),
    ("Comentarios",     35, LEFT,   None),
]

SECTION_DEFS = [
    ("only_accounting", "SOLO EN VENTAS — en contabilidad, sin registro en el canal de pago"),
    ("only_payment",    "SOLO EN PAGO — cargo en canal de pago, sin registro en contabilidad"),
    ("amount_diff",     "DIFERENCIAS DE IMPORTE — importe contabilidad ≠ importe pago"),
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_reconciliation_workbook(report: ReconciliationReport) -> bytes:
    """Return the xlsx as bytes."""
    wb = Workbook()
    wb.remove(wb.active)  # remove default sheet

    year  = int(report.period_yyyymm[:4])
    month = int(report.period_yyyymm[4:])
    month_label = f"{MONTH_NAMES[month - 1]} {year}"

    _add_channel_sheet(wb, "Shopify",  report.shopify,  report, month_label)
    _add_channel_sheet(wb, "PayPal",   report.paypal,   report, month_label)

    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Sheet builder
# ---------------------------------------------------------------------------

def _add_channel_sheet(
    wb: Workbook,
    channel: str,
    recon: ChannelReconciliation,
    report: ReconciliationReport,
    month_label: str,
) -> None:
    ws = wb.create_sheet(channel)

    # Column widths
    for i, (_, w, _, _) in enumerate(COLUMNS, 1):
        ws.column_dimensions[get_column_letter(i)].width = w

    row = 1

    # ---------- Title ----------
    ws.row_dimensions[row].height = 20
    ws.merge_cells(f"A{row}:{get_column_letter(len(COLUMNS))}{row}")
    c = ws.cell(row=row, column=1)
    c.value = (
        f"Cotejo ventas vs {channel} — {report.company_code} — {month_label}  "
        f"| Generado: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}"
    )
    c.font  = Font(name="Calibri", size=11, bold=True, color="FFFFFF")
    c.fill  = HEADER_FILL
    c.alignment = CENTER
    row += 1

    # ---------- Summary ----------
    ws.row_dimensions[row].height = ROW_H
    only_a = len(recon.only_accounting)
    only_p = len(recon.only_payment)
    diff_n  = len(recon.amount_diff)
    ws.merge_cells(f"A{row}:{get_column_letter(len(COLUMNS))}{row}")
    c = ws.cell(row=row, column=1)
    c.value = (
        f"Solo en ventas: {only_a}   |   Solo en pago: {only_p}   |   "
        f"Diferencias importe: {diff_n}"
    )
    c.font = Font(name="Calibri", size=9, italic=True, color="404040")
    c.alignment = CENTER
    row += 2  # blank line

    # ---------- Sections ----------
    for attr, section_title in SECTION_DEFS:
        rows_data: list[ReconciliationRow] = getattr(recon, attr)
        row = _write_section(ws, row, section_title, rows_data)

    ws.freeze_panes = "A4"


def _write_section(ws: Any, row: int, title: str, rows: list[ReconciliationRow]) -> int:
    ncols = len(COLUMNS)
    last_col = get_column_letter(ncols)

    # Section title
    ws.row_dimensions[row].height = ROW_H + 2
    ws.merge_cells(f"A{row}:{last_col}{row}")
    c = ws.cell(row=row, column=1, value=title)
    c.font  = WHITE_BOLD
    c.fill  = SECTION_FILL
    c.alignment = LEFT
    row += 1

    if not rows:
        # "No hay diferencias" row
        ws.row_dimensions[row].height = ROW_H
        ws.merge_cells(f"A{row}:{last_col}{row}")
        c = ws.cell(row=row, column=1, value="✓ Sin diferencias")
        c.font      = Font(name="Calibri", size=10, italic=True, color="548235")
        c.alignment = LEFT
        row += 2
        return row

    # Column headers
    ws.row_dimensions[row].height = ROW_H
    for col, (header, _, align, _) in enumerate(COLUMNS, 1):
        c = ws.cell(row=row, column=col, value=header)
        c.font      = BOLD
        c.fill      = COL_HDR_FILL
        c.alignment = CENTER
        c.border    = THIN
    row += 1

    # Data rows
    for data_row in rows:
        ws.row_dimensions[row].height = ROW_H
        fill = CHARGEBACK_FILL if data_row.is_chargeback else (
            WARN_FILL if data_row.diff is not None and abs(data_row.diff) > Decimal("10") else None
        )
        vals: list[tuple[Any, Alignment, str | None, Font | None]] = [
            (data_row.order_name,            LEFT,   None,      None),
            (data_row.order_date,            CENTER, None,      None),
            (data_row.shipping_country_code, CENTER, None,      None),
            (data_row.currency,              CENTER, None,      None),
            (data_row.accounting_amount,     RIGHT,  MONEY_FMT, None),
            (data_row.payment_amount,        RIGHT,  MONEY_FMT, None),
            (data_row.diff,                  RIGHT,  MONEY_FMT, None),
            (None,                           LEFT,   None,      LINK_FONT),   # hyperlink placeholder
            ("Sí" if data_row.is_gift_card  else "",  CENTER, None, None),
            ("Sí" if data_row.is_chargeback else "",  CENTER, None, None),
            ("",                                      LEFT,   None, None),   # Comentarios (blank, for manual notes)
        ]
        for col, (value, align, num_fmt, font_override) in enumerate(vals, 1):
            c = ws.cell(row=row, column=col, value=value)
            c.alignment = align
            c.border    = THIN
            if num_fmt:
                c.number_format = num_fmt
            if font_override:
                c.font = font_override
            else:
                c.font = NORMAL
            if fill:
                c.fill = fill

        # Hyperlink in column 8 (Link Shopify)
        if data_row.shopify_url:
            link_cell = ws.cell(row=row, column=8)
            link_cell.value     = "Ver pedido"
            link_cell.hyperlink = data_row.shopify_url
            link_cell.font      = LINK_FONT

        row += 1

    row += 1  # blank line between sections
    return row
