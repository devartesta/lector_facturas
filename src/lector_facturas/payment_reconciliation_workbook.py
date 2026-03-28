"""Build the payment reconciliation Excel workbook.

Four sheets:
  1. Resumen       — overview table (counts + totals) and legend
  2. Chargebacks   — inventory of all chargebacks last 12 months (open + won)
  3. Shopify       — detailed cotejo rows for Shopify Payments channel
  4. PayPal        — detailed cotejo rows for PayPal channel

Each detail sheet (Shopify / PayPal) has three sections:
  · Solo en ventas  — accounting entry with no matching payment
  · Solo en pago    — payment charge with no accounting entry this period
  · Diferencias     — both present but amounts differ > 0.01

Columns per section:
  Nº Pedido | Fecha | País | Moneda | Importe ventas | Importe pago | Diferencia
  | Link Shopify | T. regalo | Estado disp. | Comentarios
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
    CB_OPEN,
    CB_WON,
    ChannelReconciliation,
    ChargebackInventoryRow,
    ReconciliationReport,
    ReconciliationRow,
)

# ---------------------------------------------------------------------------
# Styles
# ---------------------------------------------------------------------------

HEADER_FILL  = PatternFill("solid", fgColor="1F4E78")   # dark blue   – title row
SECTION_FILL = PatternFill("solid", fgColor="2E74B5")   # mid blue    – section header
COL_HDR_FILL = PatternFill("solid", fgColor="D6E4F0")   # light blue  – column headers
WARN_FILL    = PatternFill("solid", fgColor="FFF2CC")   # yellow      – diff row highlight
CB_OPEN_FILL = PatternFill("solid", fgColor="FCE4D6")   # orange      – dispute open
CB_WON_FILL  = PatternFill("solid", fgColor="E2EFDA")   # light green – dispute won
LEGEND_FILL  = PatternFill("solid", fgColor="F2F2F2")   # grey        – legend background
TOTAL_FILL   = PatternFill("solid", fgColor="BDD7EE")   # blue-grey   – totals row
OK_FILL      = PatternFill("solid", fgColor="E2EFDA")   # light green – zero issues

WHITE_BOLD  = Font(color="FFFFFF", bold=True, name="Calibri", size=10)
BOLD        = Font(bold=True, name="Calibri", size=10)
BOLD_SMALL  = Font(bold=True, name="Calibri", size=9)
NORMAL      = Font(name="Calibri", size=10)
ITALIC_GREY = Font(name="Calibri", size=9, italic=True, color="595959")
LINK_FONT   = Font(name="Calibri", size=10, color="0563C1", underline="single")
THIN_SIDE   = Side(style="thin", color="BFBFBF")
THIN        = Border(top=THIN_SIDE, bottom=THIN_SIDE, left=THIN_SIDE, right=THIN_SIDE)
CENTER      = Alignment(horizontal="center", vertical="center", wrap_text=False)
LEFT        = Alignment(horizontal="left",   vertical="center", wrap_text=False)
LEFT_WRAP   = Alignment(horizontal="left",   vertical="top",    wrap_text=True)
RIGHT       = Alignment(horizontal="right",  vertical="center", wrap_text=False)
MONEY_FMT   = '#,##0.00;[Red](#,##0.00);-'
ROW_H       = 14

MONTH_NAMES_ES = [
    "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
    "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre",
]

# Detail columns for Shopify / PayPal sheets
COLUMNS = [
    ("Nº Pedido",      16, LEFT,   None),
    ("Fecha",          12, CENTER, None),
    ("País",            6, CENTER, None),
    ("Moneda",          7, CENTER, None),
    ("Importe ventas", 14, RIGHT,  MONEY_FMT),
    ("Importe pago",   14, RIGHT,  MONEY_FMT),
    ("Diferencia",     12, RIGHT,  MONEY_FMT),
    ("Link Shopify",   14, LEFT,   None),
    ("T. regalo",        9, CENTER, None),
    ("Chargeback",      18, CENTER, None),
    ("Comentarios",     35, LEFT,   None),
]

# Section definitions: (attribute, title, explanation)
SECTION_DEFS = [
    (
        "only_accounting",
        "SOLO EN VENTAS",
        "Pedidos registrados en contabilidad sin ningún movimiento en el canal de pago este mes."
        " Puede indicar un cobro pendiente, un pago por otra vía (transferencia, etc.)"
        " o un error de asignación de gateway.",
    ),
    (
        "only_payment",
        "SOLO EN PAGO",
        "Cobros en el canal de pago sin registro contable para este período."
        " Verificar si falta emitir factura o si el pedido pertenece a otro mes en contabilidad.",
    ),
    (
        "amount_diff",
        "DIFERENCIAS DE IMPORTE",
        "El importe liquidado por el canal de pago no coincide con el importe contabilizado."
        " Causas habituales: reembolsos parciales, disputas (chargeback), comisiones no recogidas"
        " o diferencias de cambio.",
    ),
]

# Chargeback inventory columns
CB_COLUMNS = [
    ("Canal",            10, CENTER, None),
    ("Nº Pedido",        16, LEFT,   None),
    ("Fecha pedido",     12, CENTER, None),
    ("País",              6, CENTER, None),
    ("Moneda",            7, CENTER, None),
    ("Importe venta",    14, RIGHT,  MONEY_FMT),
    ("Fecha retención",  13, CENTER, None),
    ("Importe retenido", 15, RIGHT,  MONEY_FMT),
    ("Fecha resolución", 13, CENTER, None),
    ("Importe recuperado", 16, RIGHT, MONEY_FMT),
    ("Impacto neto",     13, RIGHT,  MONEY_FMT),
    ("Estado",           13, CENTER, None),
    ("Link Shopify",     14, LEFT,   None),
    ("Comentarios",      35, LEFT,   None),
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_reconciliation_workbook(report: ReconciliationReport) -> bytes:
    """Return the xlsx as bytes."""
    wb = Workbook()
    wb.remove(wb.active)  # remove default sheet

    year        = int(report.period_yyyymm[:4])
    month       = int(report.period_yyyymm[4:])
    month_label = f"{MONTH_NAMES_ES[month - 1]} {year}"

    _add_summary_sheet(wb, report, month_label)
    _add_chargeback_sheet(wb, report, month_label)
    _add_channel_sheet(wb, "Shopify", report.shopify, report, month_label)
    _add_channel_sheet(wb, "PayPal",  report.paypal,  report, month_label)

    buf = BytesIO()
    wb.save(buf)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Summary sheet
# ---------------------------------------------------------------------------

def _add_summary_sheet(
    wb: Workbook,
    report: ReconciliationReport,
    month_label: str,
) -> None:
    ws = wb.create_sheet("Resumen")

    ws.column_dimensions["A"].width = 22
    ws.column_dimensions["B"].width = 30
    ws.column_dimensions["C"].width = 12
    ws.column_dimensions["D"].width = 16
    ws.column_dimensions["E"].width = 16
    ws.column_dimensions["F"].width = 16
    ws.column_dimensions["G"].width = 40

    row = 1

    # Title
    ws.row_dimensions[row].height = 22
    ws.merge_cells("A1:G1")
    c = ws.cell(row=1, column=1,
        value=(f"Cotejo de pagos — {report.company_code} — {month_label}"
               f"   |   Generado: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}"))
    c.font      = Font(name="Calibri", size=12, bold=True, color="FFFFFF")
    c.fill      = HEADER_FILL
    c.alignment = CENTER
    row += 1

    # Intro
    ws.row_dimensions[row].height = 18
    ws.merge_cells(f"A{row}:G{row}")
    c = ws.cell(row=row, column=1,
        value=("Este informe compara las ventas registradas en contabilidad con los movimientos en "
               "Shopify Payments y PayPal, pedido a pedido, para detectar cobros sin facturar, "
               "facturas sin cobro o diferencias de importe."))
    c.font      = ITALIC_GREY
    c.alignment = LEFT_WRAP
    row += 2

    # Summary table header
    headers = ["Canal", "Categoría", "Nº pedidos", "Importe ventas", "Importe pago",
               "Diferencia neta", "Acción recomendada"]
    ws.row_dimensions[row].height = ROW_H
    for col, h in enumerate(headers, 1):
        c = ws.cell(row=row, column=col, value=h)
        c.font = BOLD; c.fill = COL_HDR_FILL; c.alignment = CENTER; c.border = THIN
    row += 1

    actions = {
        "only_accounting": "Verificar si el cobro llegó por otra vía o si hay que reclamar al canal de pago.",
        "only_payment":    "Verificar si falta registrar la venta en contabilidad.",
        "amount_diff":     "Revisar reembolsos parciales, chargebacks o comisiones no registradas.",
    }

    for channel_label, recon in [("Shopify Payments", report.shopify), ("PayPal", report.paypal)]:
        for attr, sec_title, _ in SECTION_DEFS:
            rows_data: list[ReconciliationRow] = getattr(recon, attr)
            n        = len(rows_data)
            _D0      = Decimal("0")
            sum_acct = sum(((r.accounting_amount or _D0) for r in rows_data), _D0).quantize(Decimal("0.01"))
            sum_pay  = sum(((r.payment_amount    or _D0) for r in rows_data), _D0).quantize(Decimal("0.01"))
            sum_diff = sum(((r.diff              or _D0) for r in rows_data), _D0).quantize(Decimal("0.01"))
            fill     = OK_FILL if n == 0 else None

            ws.row_dimensions[row].height = ROW_H
            vals = [
                (channel_label, LEFT,   None,      BOLD),
                (sec_title,     LEFT,   None,      NORMAL),
                (n,             CENTER, None,      BOLD if n > 0 else NORMAL),
                (sum_acct if sum_acct else None, RIGHT, MONEY_FMT, NORMAL),
                (sum_pay  if sum_pay  else None, RIGHT, MONEY_FMT, NORMAL),
                (sum_diff if sum_diff else None, RIGHT, MONEY_FMT, NORMAL),
                ("✓ Sin diferencias" if n == 0 else actions[attr], LEFT, None,
                 ITALIC_GREY if n == 0 else Font(name="Calibri", size=9)),
            ]
            for col, (val, align, fmt, font) in enumerate(vals, 1):
                c = ws.cell(row=row, column=col, value=val)
                c.font = font; c.alignment = align; c.border = THIN
                if fmt: c.number_format = fmt
                if fill: c.fill = fill
            row += 1
        row += 1  # separator between channels

    # Chargeback summary
    n_open = sum(1 for r in report.chargeback_inventory if r.status == CB_OPEN)
    n_won  = sum(1 for r in report.chargeback_inventory if r.status == CB_WON)
    open_impact = sum(
        (r.net_impact or Decimal("0")) for r in report.chargeback_inventory if r.status == CB_OPEN
    ).quantize(Decimal("0.01"))

    row += 1
    ws.row_dimensions[row].height = ROW_H + 2
    ws.merge_cells(f"A{row}:G{row}")
    c = ws.cell(row=row, column=1, value="RESUMEN DE CHARGEBACKS (últimos 12 meses)")
    c.font = WHITE_BOLD; c.fill = SECTION_FILL; c.alignment = LEFT
    row += 1

    shopify_cbs = [r for r in report.chargeback_inventory if r.channel == "Shopify"]
    paypal_cbs  = [r for r in report.chargeback_inventory if r.channel == "PayPal"]
    shopify_open_impact = sum(
        ((r.net_impact or Decimal("0")) for r in shopify_cbs if r.status == CB_OPEN),
        Decimal("0"),
    ).quantize(Decimal("0.01"))

    cb_summary = [
        ("Shopify Payments", CB_OPEN,
         sum(1 for r in shopify_cbs if r.status == CB_OPEN),
         f"Retenido pendiente: {shopify_open_impact:,.2f}" if any(r.status == CB_OPEN for r in shopify_cbs) else "Ninguno abierto"),
        ("Shopify Payments", CB_WON,
         sum(1 for r in shopify_cbs if r.status == CB_WON),
         "Disputas resueltas a nuestro favor. Ver pestaña Chargebacks."),
        ("PayPal", CB_OPEN,
         len(paypal_cbs),
         "⚠ Estado no detectable automáticamente. Confirmar en el portal de PayPal."),
    ]
    for channel_lbl, status, count, note in cb_summary:
        fill = CB_OPEN_FILL if status == CB_OPEN else CB_WON_FILL
        ws.row_dimensions[row].height = ROW_H
        vals2 = [
            (channel_lbl,  LEFT, None, BOLD),
            (status,       LEFT, None, NORMAL),
            (count,        CENTER, None, BOLD if count > 0 else NORMAL),
            (None, RIGHT, None, NORMAL),
            (None, RIGHT, None, NORMAL),
            (None, RIGHT, None, NORMAL),
            (note, LEFT, None, Font(name="Calibri", size=9)),
        ]
        for col, (val, align, fmt, font) in enumerate(vals2, 1):
            c = ws.cell(row=row, column=col, value=val)
            c.font = font; c.alignment = align; c.border = THIN; c.fill = fill
        row += 1

    # Legend
    row += 2
    ws.row_dimensions[row].height = ROW_H + 2
    ws.merge_cells(f"A{row}:G{row}")
    c = ws.cell(row=row, column=1, value="LEYENDA DE COLORES Y ESTADOS")
    c.font = WHITE_BOLD; c.fill = SECTION_FILL; c.alignment = LEFT
    row += 1

    legend_items = [
        (CB_OPEN_FILL, "Naranja — Disputa abierta (\"En disputa\")",
         "El cliente ha iniciado un chargeback y el canal de pago ha retenido el importe."
         " Hay que responder antes del plazo indicado por Shopify/PayPal."),
        (CB_WON_FILL,  "Verde — Disputa ganada (\"Ganado\")",
         "La disputa se resolvió a nuestro favor y se recuperó el importe."
         " La devolución puede caer en un mes diferente a la retención original,"
         " lo que puede generar una diferencia de importe en el cotejo mensual."),
        (WARN_FILL,    "Amarillo — Diferencia > 10 en importe",
         "El importe contabilizado y el liquidado difieren en más de 10 unidades de moneda."
         " Revisar si hay reembolsos parciales o comisiones no registradas."),
    ]
    for fill, label, explanation in legend_items:
        ws.row_dimensions[row].height = 32
        c = ws.cell(row=row, column=1, value="")
        c.fill = fill; c.border = THIN
        c = ws.cell(row=row, column=2, value=label)
        c.font = BOLD_SMALL; c.alignment = LEFT; c.border = THIN
        ws.merge_cells(f"C{row}:G{row}")
        c = ws.cell(row=row, column=3, value=explanation)
        c.font = Font(name="Calibri", size=9); c.alignment = LEFT_WRAP
        c.fill = LEGEND_FILL; c.border = THIN
        row += 1

    ws.freeze_panes = "A3"


# ---------------------------------------------------------------------------
# Chargeback inventory sheet
# ---------------------------------------------------------------------------

def _add_chargeback_sheet(
    wb: Workbook,
    report: ReconciliationReport,
    month_label: str,
) -> None:
    ws = wb.create_sheet("Chargebacks")

    for i, (_, w, _, _) in enumerate(CB_COLUMNS, 1):
        ws.column_dimensions[get_column_letter(i)].width = w

    ncols    = len(CB_COLUMNS)
    last_col = get_column_letter(ncols)
    row      = 1

    # Title
    ws.row_dimensions[row].height = 22
    ws.merge_cells(f"A{row}:{last_col}{row}")
    c = ws.cell(row=row, column=1,
        value=(f"Inventario de chargebacks — {report.company_code}"
               f"   |   Últimos 12 meses   |   Generado: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}"))
    c.font = Font(name="Calibri", size=11, bold=True, color="FFFFFF")
    c.fill = HEADER_FILL; c.alignment = CENTER
    row += 1

    # Summary line
    inventory   = report.chargeback_inventory
    shopify_cbs = [r for r in inventory if r.channel == "Shopify"]
    paypal_cbs  = [r for r in inventory if r.channel == "PayPal"]
    n_shopify_open = sum(1 for r in shopify_cbs if r.status == CB_OPEN)
    n_shopify_won  = sum(1 for r in shopify_cbs if r.status == CB_WON)
    shopify_open_impact = sum(
        ((r.net_impact or Decimal("0")) for r in shopify_cbs if r.status == CB_OPEN),
        Decimal("0"),
    ).quantize(Decimal("0.01"))

    ws.row_dimensions[row].height = 22
    ws.merge_cells(f"A{row}:{last_col}{row}")
    c = ws.cell(row=row, column=1,
        value=(f"Shopify: {n_shopify_open} en disputa, {n_shopify_won} ganados "
               f"(impacto abierto: {shopify_open_impact:,.2f})   |   "
               f"PayPal: {len(paypal_cbs)} disputas — estado a confirmar en portal PayPal   |   "
               f"Naranja = acción requerida · Verde = resuelto"))
    c.font = ITALIC_GREY; c.alignment = LEFT_WRAP
    row += 2

    if not inventory:
        ws.merge_cells(f"A{row}:{last_col}{row}")
        c = ws.cell(row=row, column=1, value="✓ Sin chargebacks en los últimos 12 meses")
        c.font = Font(name="Calibri", size=10, italic=True, color="548235")
        c.alignment = LEFT
        return

    # ---- Shopify section ----
    row = _write_cb_section(
        ws, row, last_col,
        title="SHOPIFY PAYMENTS",
        note=("Estado detectable automáticamente: 'En disputa' = retención activa sin reversal todavía; "
              "'Ganado' = el canal devolvió el importe (dispute_reversal recibido)."),
        items=shopify_cbs,
        paypal_warning=False,
    )

    # ---- PayPal section ----
    row = _write_cb_section(
        ws, row, last_col,
        title="PAYPAL",
        note=("⚠ PayPal no genera una transacción de 'disputa resuelta' en nuestros datos (no hay T1112). "
              "Todas las disputas aparecen como 'En disputa' aunque puedan estar ya cerradas. "
              "Confirmar el estado real en el portal de PayPal (Resolution Center)."),
        items=paypal_cbs,
        paypal_warning=True,
    )

    ws.freeze_panes = "A4"


def _write_cb_section(
    ws: Any,
    row: int,
    last_col: str,
    title: str,
    note: str,
    items: list[ChargebackInventoryRow],
    paypal_warning: bool,
) -> int:
    """Write one channel block (header + col headers + rows + totals) in the chargeback sheet."""
    ncols = len(CB_COLUMNS) - 1  # CB_COLUMNS includes Canal which we drop here
    # We use CB_COLUMNS[1:] (skip the Canal column since it's in the section header)
    detail_cols = CB_COLUMNS[1:]
    detail_last = get_column_letter(len(detail_cols))

    # Section header
    ws.row_dimensions[row].height = ROW_H + 2
    ws.merge_cells(f"A{row}:{last_col}{row}")
    c = ws.cell(row=row, column=1, value=title)
    c.font = WHITE_BOLD; c.fill = SECTION_FILL; c.alignment = LEFT
    row += 1

    # Note
    ws.row_dimensions[row].height = 30
    ws.merge_cells(f"A{row}:{last_col}{row}")
    c = ws.cell(row=row, column=1, value=note)
    c.font = Font(name="Calibri", size=9,
                  italic=True,
                  color="843C0C" if paypal_warning else "404040")
    c.fill = LEGEND_FILL; c.alignment = LEFT_WRAP
    row += 1

    if not items:
        ws.row_dimensions[row].height = ROW_H
        ws.merge_cells(f"A{row}:{last_col}{row}")
        c = ws.cell(row=row, column=1, value="✓ Sin chargebacks en los últimos 12 meses")
        c.font = Font(name="Calibri", size=10, italic=True, color="548235")
        c.alignment = LEFT
        return row + 2

    # Column headers (skip Canal column)
    ws.row_dimensions[row].height = ROW_H
    for col, (header, _, _, _) in enumerate(detail_cols, 1):
        c = ws.cell(row=row, column=col, value=header)
        c.font = BOLD; c.fill = COL_HDR_FILL; c.alignment = CENTER; c.border = THIN
    row += 1

    # Data rows
    for r in items:
        ws.row_dimensions[row].height = ROW_H
        fill = CB_OPEN_FILL if r.status == CB_OPEN else CB_WON_FILL
        # For PayPal, we can't confirm status so always use open colour
        if paypal_warning:
            fill = CB_OPEN_FILL

        rev_date_val = r.reversal_date if r.reversal_date else ("—" if not paypal_warning else "Desconocido")
        rev_date_font = ITALIC_GREY if not r.reversal_date else None

        vals: list[tuple[Any, Alignment, str | None, Font | None]] = [
            (r.order_name,            LEFT,   None,      BOLD),
            (r.order_date,            CENTER, None,      None),
            (r.shipping_country_code, CENTER, None,      None),
            (r.currency,              CENTER, None,      None),
            (r.accounting_amount,     RIGHT,  MONEY_FMT, None),
            (r.withdrawal_date,       CENTER, None,      None),
            (r.withdrawal_amount,     RIGHT,  MONEY_FMT, None),
            (rev_date_val,            CENTER, None,      rev_date_font),
            (r.reversal_amount,       RIGHT,  MONEY_FMT, None),
            (r.net_impact,            RIGHT,  MONEY_FMT, BOLD),
            (r.status if not paypal_warning else "Ver PayPal", CENTER, None, BOLD),
            (None,                    LEFT,   None,      LINK_FONT),
            ("",                      LEFT,   None,      None),
        ]
        for col, (value, align, fmt, font) in enumerate(vals, 1):
            c = ws.cell(row=row, column=col, value=value)
            c.alignment = align; c.border = THIN; c.fill = fill
            c.font = font if font else NORMAL
            if fmt: c.number_format = fmt

        # Hyperlink (column 12 = Link Shopify, now without Canal col)
        if r.shopify_url:
            lc = ws.cell(row=row, column=12)
            lc.value = "Ver pedido"; lc.hyperlink = r.shopify_url; lc.font = LINK_FONT

        row += 1

    # Subtotals
    ws.row_dimensions[row].height = ROW_H
    _D0     = Decimal("0")
    t_acct  = sum(((r.accounting_amount or _D0) for r in items), _D0).quantize(Decimal("0.01"))
    t_w     = sum(((r.withdrawal_amount or _D0) for r in items), _D0).quantize(Decimal("0.01"))
    t_v     = sum(((r.reversal_amount   or _D0) for r in items), _D0).quantize(Decimal("0.01"))
    t_net   = sum(((r.net_impact        or _D0) for r in items), _D0).quantize(Decimal("0.01"))
    total_vals: list[tuple[Any, Alignment, str | None]] = [
        (f"TOTAL {title} ({len(items)})", LEFT, None),
        (None, CENTER, None), (None, CENTER, None), (None, CENTER, None),
        (t_acct, RIGHT, MONEY_FMT),
        (None, CENTER, None),
        (t_w,   RIGHT, MONEY_FMT),
        (None, CENTER, None),
        (t_v,   RIGHT, MONEY_FMT),
        (t_net, RIGHT, MONEY_FMT),
        (None, CENTER, None), (None, LEFT, None), (None, LEFT, None),
    ]
    for col, (val, align, fmt) in enumerate(total_vals, 1):
        c = ws.cell(row=row, column=col, value=val)
        c.font = BOLD; c.fill = TOTAL_FILL; c.alignment = align; c.border = THIN
        if fmt: c.number_format = fmt

    return row + 2


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cb_label(status: str | None, is_chargeback: bool) -> str:
    """Human-readable chargeback label for the detail sheet column."""
    if status == CB_WON:
        return "Chargeback — Ganado"
    if status == CB_OPEN:
        return "Chargeback — En disputa"
    if is_chargeback:
        return "Chargeback"
    return ""


# ---------------------------------------------------------------------------
# Detail sheet builder
# ---------------------------------------------------------------------------

def _add_channel_sheet(
    wb: Workbook,
    channel: str,
    recon: ChannelReconciliation,
    report: ReconciliationReport,
    month_label: str,
) -> None:
    ws = wb.create_sheet(channel)

    for i, (_, w, _, _) in enumerate(COLUMNS, 1):
        ws.column_dimensions[get_column_letter(i)].width = w

    row = 1

    # Title
    ws.row_dimensions[row].height = 22
    ws.merge_cells(f"A{row}:{get_column_letter(len(COLUMNS))}{row}")
    c = ws.cell(row=row, column=1)
    c.value = (f"Cotejo ventas vs {channel} — {report.company_code} — {month_label}  "
               f"| Generado: {datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')}")
    c.font = Font(name="Calibri", size=11, bold=True, color="FFFFFF")
    c.fill = HEADER_FILL; c.alignment = CENTER
    row += 1

    # Quick summary
    only_a = len(recon.only_accounting)
    only_p = len(recon.only_payment)
    diff_n = len(recon.amount_diff)
    ws.row_dimensions[row].height = ROW_H
    ws.merge_cells(f"A{row}:{get_column_letter(len(COLUMNS))}{row}")
    c = ws.cell(row=row, column=1)
    c.value = (f"Solo en ventas: {only_a}   |   Solo en pago: {only_p}   |   "
               f"Diferencias importe: {diff_n}   "
               f"{'✓ Todo cuadra' if only_a + only_p + diff_n == 0 else '⚠ Revisar diferencias — ver pestaña Resumen'}")
    c.font = ITALIC_GREY; c.alignment = CENTER
    row += 2

    for attr, sec_title, sec_explanation in SECTION_DEFS:
        rows_data: list[ReconciliationRow] = getattr(recon, attr)
        row = _write_section(ws, row, sec_title, sec_explanation, rows_data)

    ws.freeze_panes = "A4"


def _write_section(
    ws: Any,
    row: int,
    title: str,
    explanation: str,
    rows: list[ReconciliationRow],
) -> int:
    ncols    = len(COLUMNS)
    last_col = get_column_letter(ncols)

    # Section title
    ws.row_dimensions[row].height = ROW_H + 2
    ws.merge_cells(f"A{row}:{last_col}{row}")
    c = ws.cell(row=row, column=1, value=title)
    c.font = WHITE_BOLD; c.fill = SECTION_FILL; c.alignment = LEFT
    row += 1

    # Explanation
    ws.row_dimensions[row].height = 28
    ws.merge_cells(f"A{row}:{last_col}{row}")
    c = ws.cell(row=row, column=1, value=explanation)
    c.font = Font(name="Calibri", size=9, italic=True, color="404040")
    c.fill = LEGEND_FILL; c.alignment = LEFT_WRAP
    row += 1

    if not rows:
        ws.row_dimensions[row].height = ROW_H
        ws.merge_cells(f"A{row}:{last_col}{row}")
        c = ws.cell(row=row, column=1, value="✓ Sin diferencias")
        c.font = Font(name="Calibri", size=10, italic=True, color="548235")
        c.alignment = LEFT
        row += 2
        return row

    # Column headers
    ws.row_dimensions[row].height = ROW_H
    for col, (header, _, _, _) in enumerate(COLUMNS, 1):
        c = ws.cell(row=row, column=col, value=header)
        c.font = BOLD; c.fill = COL_HDR_FILL; c.alignment = CENTER; c.border = THIN
    row += 1

    # Data rows
    for data_row in rows:
        ws.row_dimensions[row].height = ROW_H
        if data_row.chargeback_status == CB_WON:
            fill = CB_WON_FILL
        elif data_row.chargeback_status == CB_OPEN or data_row.is_chargeback:
            fill = CB_OPEN_FILL
        elif data_row.diff is not None and abs(data_row.diff) > Decimal("10"):
            fill = WARN_FILL
        else:
            fill = None

        vals: list[tuple[Any, Alignment, str | None, Font | None]] = [
            (data_row.order_name,             LEFT,   None,      None),
            (data_row.order_date,             CENTER, None,      None),
            (data_row.shipping_country_code,  CENTER, None,      None),
            (data_row.currency,               CENTER, None,      None),
            (data_row.accounting_amount,      RIGHT,  MONEY_FMT, None),
            (data_row.payment_amount,         RIGHT,  MONEY_FMT, None),
            (data_row.diff,                   RIGHT,  MONEY_FMT, None),
            (None,                            LEFT,   None,      LINK_FONT),
            ("Sí" if data_row.is_gift_card else "",                CENTER, None, None),
            (_cb_label(data_row.chargeback_status, data_row.is_chargeback), CENTER, None, BOLD if data_row.is_chargeback else None),
            ("",                              LEFT,   None,      None),
        ]
        for col, (value, align, num_fmt, font_override) in enumerate(vals, 1):
            c = ws.cell(row=row, column=col, value=value)
            c.alignment = align; c.border = THIN
            if num_fmt: c.number_format = num_fmt
            c.font = font_override if font_override else NORMAL
            if fill: c.fill = fill

        if data_row.shopify_url:
            lc = ws.cell(row=row, column=8)
            lc.value = "Ver pedido"; lc.hyperlink = data_row.shopify_url; lc.font = LINK_FONT

        row += 1

    # Totals row
    ws.row_dimensions[row].height = ROW_H
    sum_acct = sum((r.accounting_amount or Decimal("0")) for r in rows).quantize(Decimal("0.01"))
    sum_pay  = sum((r.payment_amount    or Decimal("0")) for r in rows).quantize(Decimal("0.01"))
    sum_diff = sum((r.diff              or Decimal("0")) for r in rows).quantize(Decimal("0.01"))
    total_vals: list[tuple[Any, Alignment, str | None]] = [
        (f"TOTAL ({len(rows)} pedidos)", LEFT, None),
        (None, CENTER, None), (None, CENTER, None), (None, CENTER, None),
        (sum_acct, RIGHT, MONEY_FMT),
        (sum_pay,  RIGHT, MONEY_FMT),
        (sum_diff, RIGHT, MONEY_FMT),
        (None, LEFT, None), (None, CENTER, None), (None, CENTER, None), (None, LEFT, None),
    ]
    for col, (val, align, fmt) in enumerate(total_vals, 1):
        c = ws.cell(row=row, column=col, value=val)
        c.font = BOLD; c.fill = TOTAL_FILL; c.alignment = align; c.border = THIN
        if fmt: c.number_format = fmt
    row += 2

    return row
