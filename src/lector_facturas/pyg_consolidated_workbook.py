from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from lector_facturas.fx_rates import EcbFxService, FxRateAuditRow
from lector_facturas.pyg_inc_workbook import collect_pyg_inc_data
from lector_facturas.pyg_ltd_workbook import collect_pyg_ltd_data
from lector_facturas.pyg_sl_workbook import collect_pyg_sl_data

REPORTING_CURRENCY = "EUR"
DISPLAY_TIMEZONE = ZoneInfo("Europe/Madrid")
MONTH_NAMES_ES = [
    "Enero",
    "Febrero",
    "Marzo",
    "Abril",
    "Mayo",
    "Junio",
    "Julio",
    "Agosto",
    "Septiembre",
    "Octubre",
    "Noviembre",
    "Diciembre",
]
MONEY_FORMAT = '#,##0.00;[Red](#,##0.00);-'
PERCENT_FORMAT = '0.0%;[Red](0.0%);-'
ROW_HEIGHT = 12
HEADER_FILL = PatternFill("solid", fgColor="1F4E78")
SECTION_FILL = PatternFill("solid", fgColor="D9EAF7")
SUBSECTION_FILL = PatternFill("solid", fgColor="EAF3F8")
KPI_FILL = PatternFill("solid", fgColor="FFF2CC")
WHITE_BOLD = Font(color="FFFFFF", bold=True)
BOLD = Font(bold=True)
TITLE_FONT = Font(size=12, bold=True, color="FFFFFF")
THIN_TOP_BORDER = Border(top=Side(style="thin", color="A6A6A6"))
MEDIUM_TOP_BORDER = Border(top=Side(style="medium", color="7F7F7F"))

MAIN_LINES = [
    ("turnover", "Turnover"),
    ("product_sales", "  Product sales"),
    ("marketplaces", "  Marketplaces"),
    ("services", "  Services"),
    ("rappels", "  Rappels"),
    ("supplies", "  Supplies"),
    ("otros_ingresos", "  Otros ingresos"),
    ("diferencias_divisas", "  Diferencias divisas"),
    ("expenses", "Expenses"),
    ("cogs", "  COGS"),
    ("manufacturing", "    Manufacturing"),
    ("logistics", "    Logistics"),
    ("royalties", "    Royalties"),
    ("payment_fees", "    Payment fees"),
    ("gross_margin", "GROSS MARGIN (SALES-MANUFACTURING)"),
    ("gross_margin_pct", "% GROSS MARGIN"),
    ("contributive_margin", "CONTRIBUTIVE MARGIN (TURNOVER-COGS)"),
    ("contributive_margin_pct", "% CONTRIBUTIVE MARGIN"),
    ("opex", "  Opex"),
    ("marketing", "    Marketing"),
    ("staff", "    Staff"),
    ("shared_services", "    Shared services"),
    ("administration", "    Administration"),
    ("technology", "    Technology"),
    ("otros_gastos", "    Otros gastos"),
    ("profit", "PROFIT"),
    ("profit_pct", "% Profit / turnover"),
]


@dataclass(frozen=True)
class ConsolidatedEntityMonth:
    company_code: str
    yyyymm: str
    turnover: Decimal = Decimal("0")
    product_sales: Decimal = Decimal("0")
    marketplaces: Decimal = Decimal("0")
    services: Decimal = Decimal("0")
    rappels: Decimal = Decimal("0")
    supplies: Decimal = Decimal("0")
    otros_ingresos: Decimal = Decimal("0")
    diferencias_divisas: Decimal = Decimal("0")
    expenses: Decimal = Decimal("0")
    cogs: Decimal = Decimal("0")
    manufacturing: Decimal = Decimal("0")
    logistics: Decimal = Decimal("0")
    royalties: Decimal = Decimal("0")
    payment_fees: Decimal = Decimal("0")
    gross_margin: Decimal = Decimal("0")
    contributive_margin: Decimal = Decimal("0")
    opex: Decimal = Decimal("0")
    marketing: Decimal = Decimal("0")
    staff: Decimal = Decimal("0")
    shared_services: Decimal = Decimal("0")
    administration: Decimal = Decimal("0")
    technology: Decimal = Decimal("0")
    otros_gastos: Decimal = Decimal("0")
    profit: Decimal = Decimal("0")


@dataclass(frozen=True)
class ConsolidatedPygBundle:
    year: int
    generated_at: datetime
    rows: tuple[ConsolidatedEntityMonth, ...]
    fx_rate_rows: tuple[FxRateAuditRow, ...] = ()


def default_output_path(root: Path, year: int) -> Path:
    return root / "output" / "spreadsheet" / f"pyg_consolidado_{year}.xlsx"


def month_keys(year: int) -> list[str]:
    return [f"{year}{month:02d}" for month in range(1, 13)]


def collect_pyg_consolidated_data(*, year: int, database_url: str | None) -> ConsolidatedPygBundle:
    if not database_url:
        return ConsolidatedPygBundle(year=year, generated_at=datetime.now(UTC), rows=())

    fx = EcbFxService()
    rows: list[ConsolidatedEntityMonth] = []
    fx_rows: list[FxRateAuditRow] = []
    sl_rows, sl_fx_rows = _collect_sl(year=year, database_url=database_url, fx=fx)
    ltd_rows, ltd_fx_rows = _collect_ltd(year=year, database_url=database_url, fx=fx)
    inc_rows, inc_fx_rows = _collect_inc(year=year, database_url=database_url, fx=fx)
    rows.extend(sl_rows)
    rows.extend(ltd_rows)
    rows.extend(inc_rows)
    fx_rows.extend(sl_fx_rows)
    fx_rows.extend(ltd_fx_rows)
    fx_rows.extend(inc_fx_rows)
    return ConsolidatedPygBundle(year=year, generated_at=datetime.now(UTC), rows=tuple(rows), fx_rate_rows=_dedupe_fx_rows(tuple(fx_rows)))


def build_pyg_consolidated_workbook(bundle: ConsolidatedPygBundle, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    wb = Workbook()
    ws = wb.active
    ws.title = "P&G-CONSOLIDADO"

    for idx, yyyymm in enumerate(month_keys(bundle.year), start=4):
        ws.cell(row=1, column=idx, value=yyyymm)
        ws.cell(row=2, column=idx, value=MONTH_NAMES_ES[idx - 4])
    ws["P1"] = "TOTAL"
    ws["P2"] = "Total"
    ws["A2"] = f"P&G CONSOLIDADO {bundle.year} ({REPORTING_CURRENCY})"
    ws.merge_cells("A2:C2")
    generated_at_local = bundle.generated_at.astimezone(DISPLAY_TIMEZONE) if bundle.generated_at.tzinfo else bundle.generated_at.replace(tzinfo=UTC).astimezone(DISPLAY_TIMEZONE)
    ws["A3"] = f"Actualizado: {generated_at_local.strftime('%d/%m/%Y %H:%M')} h"
    ws.merge_cells("A3:C3")

    for row in (1, 2):
        for cell in ws[row]:
            cell.fill = HEADER_FILL
            cell.font = WHITE_BOLD
            cell.alignment = Alignment(horizontal="center", vertical="center")
    ws["A2"].font = TITLE_FONT
    ws["A3"].font = Font(size=9, italic=True, color="666666")
    ws.row_dimensions[1].hidden = True

    row_map: dict[str, int] = {}
    row = 4
    for key, label in MAIN_LINES:
        row_map[key] = row
        ws[f"A{row}"] = label
        row += 1

    totals = _aggregate_totals(bundle.rows)
    for idx, yyyymm in enumerate(month_keys(bundle.year), start=4):
        col = get_column_letter(idx)
        month_total = totals.get(yyyymm, {})
        for key, _ in MAIN_LINES:
            value = month_total.get(key)
            if value is None:
                continue
            if key.endswith("_pct"):
                continue
            ws[f"{col}{row_map[key]}"] = float(value)
        ws[f"{col}{row_map['gross_margin_pct']}"] = f'=IFERROR({col}{row_map["gross_margin"]}/{col}{row_map["product_sales"]},0)'
        ws[f"{col}{row_map['contributive_margin_pct']}"] = f'=IFERROR({col}{row_map["contributive_margin"]}/{col}{row_map["turnover"]},0)'
        ws[f"{col}{row_map['profit_pct']}"] = f'=IFERROR({col}{row_map["profit"]}/{col}{row_map["turnover"]},0)'

    for row_idx in row_map.values():
        ws[f"P{row_idx}"] = f"=SUM(D{row_idx}:O{row_idx})"

    major_rows = {row_map["turnover"], row_map["expenses"], row_map["gross_margin"], row_map["contributive_margin"], row_map["profit"]}
    subtotal_rows = {row_map["cogs"], row_map["opex"]}
    section_rows = {
        row_map["product_sales"],
        row_map["marketplaces"],
        row_map["services"],
        row_map["rappels"],
        row_map["supplies"],
        row_map["otros_ingresos"],
        row_map["manufacturing"],
        row_map["logistics"],
        row_map["royalties"],
        row_map["payment_fees"],
        row_map["marketing"],
        row_map["staff"],
        row_map["shared_services"],
        row_map["administration"],
        row_map["technology"],
        row_map["otros_gastos"],
    }
    percent_rows = {row_map["gross_margin_pct"], row_map["contributive_margin_pct"], row_map["profit_pct"]}

    for row_idx in range(2, max(row_map.values()) + 1):
        ws.row_dimensions[row_idx].height = ROW_HEIGHT
        for col_idx in range(1, 17):
            cell = ws.cell(row=row_idx, column=col_idx)
            if row_idx in major_rows:
                cell.fill = KPI_FILL if row_idx in {row_map["gross_margin"], row_map["contributive_margin"], row_map["profit"]} else SECTION_FILL
                cell.border = MEDIUM_TOP_BORDER
            elif row_idx in subtotal_rows:
                cell.fill = SECTION_FILL
                cell.border = THIN_TOP_BORDER
            elif row_idx in section_rows:
                cell.fill = SUBSECTION_FILL
        for col_idx in range(4, 17):
            ws.cell(row=row_idx, column=col_idx).number_format = PERCENT_FORMAT if row_idx in percent_rows else MONEY_FORMAT
    for col, width in (("A", 38), ("B", 2), ("C", 2), ("P", 14)):
        ws.column_dimensions[col].width = width
    for idx in range(4, 16):
        ws.column_dimensions[get_column_letter(idx)].width = 14
    ws.freeze_panes = "D4"
    ws.sheet_view.showGridLines = False

    _write_entity_detail_sheet(wb, bundle.rows)
    _write_fx_rates_sheet(wb, bundle.fx_rate_rows)
    wb.save(output_path)
    return output_path


def _collect_sl(*, year: int, database_url: str, fx: EcbFxService) -> tuple[list[ConsolidatedEntityMonth], tuple[FxRateAuditRow, ...]]:
    bundle = collect_pyg_sl_data(year=year, database_url=database_url)
    months = month_keys(year)
    out: list[ConsolidatedEntityMonth] = []
    fx_rows: list[FxRateAuditRow] = []
    for yyyymm in months:
        product_sales, a = _sum_stage(bundle.shopify_rows, yyyymm=yyyymm, fx=fx); fx_rows.extend(a)
        marketplaces, a = _sum_stage(bundle.marketplace_rows, yyyymm=yyyymm, fx=fx); fx_rows.extend(a)
        services, a = _sum_stage(bundle.service_rows, yyyymm=yyyymm, fx=fx); fx_rows.extend(a)
        rappels, a = _sum_stage(bundle.rappel_rows, yyyymm=yyyymm, fx=fx); fx_rows.extend(a)
        supplies, a = _sum_stage(bundle.supplies_rows, yyyymm=yyyymm, fx=fx); fx_rows.extend(a)
        manufacturing, a = _sum_expense(bundle.expense_rows, yyyymm=yyyymm, fx=fx, category="cogs", subcategory="manufacturing"); fx_rows.extend(a)
        logistics, a = _sum_expense(bundle.expense_rows, yyyymm=yyyymm, fx=fx, category="cogs", subcategory="logistics"); fx_rows.extend(a)
        royalties, a = _sum_expense(bundle.expense_rows, yyyymm=yyyymm, fx=fx, category="cogs", subcategory="royalties"); fx_rows.extend(a)
        payment_fees, a = _sum_payment_fees(bundle.payment_fee_rows, yyyymm=yyyymm, fx=fx); fx_rows.extend(a)
        marketing, a = _sum_expense(bundle.expense_rows, yyyymm=yyyymm, fx=fx, category="opex", subcategory="marketing"); fx_rows.extend(a)
        staff, a = _sum_expense(bundle.expense_rows, yyyymm=yyyymm, fx=fx, category="opex", subcategory="staff"); fx_rows.extend(a)
        shared_services, a = _sum_expense(bundle.expense_rows, yyyymm=yyyymm, fx=fx, category="opex", subcategory="shared_services"); fx_rows.extend(a)
        administration, a = _sum_expense(bundle.expense_rows, yyyymm=yyyymm, fx=fx, category="opex", subcategory="administration"); fx_rows.extend(a)
        technology, a = _sum_expense(bundle.expense_rows, yyyymm=yyyymm, fx=fx, category="opex", subcategory="technology"); fx_rows.extend(a)
        otros_gastos, a = _sum_expense(bundle.expense_rows, yyyymm=yyyymm, fx=fx, category="opex", subcategory="otros_gastos"); fx_rows.extend(a)
        otros_ingresos = bundle.otros_ingresos_by_period.get(yyyymm, Decimal("0"))
        diferencias_divisas = bundle.diferencias_divisas_by_period.get(yyyymm, Decimal("0"))
        out.append(_compose_row(
            company_code="SL",
            yyyymm=yyyymm,
            product_sales=product_sales,
            marketplaces=marketplaces,
            services=services,
            rappels=rappels,
            supplies=supplies,
            otros_ingresos=otros_ingresos,
            diferencias_divisas=diferencias_divisas,
            manufacturing=manufacturing,
            logistics=logistics,
            royalties=royalties,
            payment_fees=payment_fees,
            marketing=marketing,
            staff=staff,
            shared_services=shared_services,
            administration=administration,
            technology=technology,
            otros_gastos=otros_gastos,
        ))
    return out, tuple(fx_rows)


def _collect_ltd(*, year: int, database_url: str, fx: EcbFxService) -> tuple[list[ConsolidatedEntityMonth], tuple[FxRateAuditRow, ...]]:
    bundle = collect_pyg_ltd_data(year=year, database_url=database_url)
    out: list[ConsolidatedEntityMonth] = []
    fx_rows: list[FxRateAuditRow] = []
    for yyyymm in month_keys(year):
        product_sales, a = _sum_stage(bundle.sales_rows, yyyymm=yyyymm, fx=fx); fx_rows.extend(a)
        manufacturing, a = _sum_expense(bundle.expense_rows, yyyymm=yyyymm, fx=fx, subcategory="manufacturing"); fx_rows.extend(a)
        logistics, a = _sum_expense(bundle.expense_rows, yyyymm=yyyymm, fx=fx, subcategory="logistics"); fx_rows.extend(a)
        payment_fees, a = _sum_payment_fees(bundle.payment_fee_rows, yyyymm=yyyymm, fx=fx); fx_rows.extend(a)
        shared_services, a = _sum_expense(bundle.expense_rows, yyyymm=yyyymm, fx=fx, subcategory="shared_services"); fx_rows.extend(a)
        administration, a = _sum_expense(bundle.expense_rows, yyyymm=yyyymm, fx=fx, subcategory="administration"); fx_rows.extend(a)
        technology, a = _sum_expense(bundle.expense_rows, yyyymm=yyyymm, fx=fx, subcategory="technology"); fx_rows.extend(a)
        otros_gastos, a = _sum_expense(bundle.expense_rows, yyyymm=yyyymm, fx=fx, subcategory="otros_gastos"); fx_rows.extend(a)
        otros_ingresos = bundle.otros_ingresos_by_period.get(yyyymm, Decimal("0"))
        diferencias_divisas = bundle.diferencias_divisas_by_period.get(yyyymm, Decimal("0"))
        out.append(_compose_row(
            company_code="LTD",
            yyyymm=yyyymm,
            product_sales=product_sales,
            otros_ingresos=otros_ingresos,
            diferencias_divisas=diferencias_divisas,
            manufacturing=manufacturing,
            logistics=logistics,
            payment_fees=payment_fees,
            shared_services=shared_services,
            administration=administration,
            technology=technology,
            otros_gastos=otros_gastos,
        ))
    return out, tuple(fx_rows)


def _collect_inc(*, year: int, database_url: str, fx: EcbFxService) -> tuple[list[ConsolidatedEntityMonth], tuple[FxRateAuditRow, ...]]:
    bundle = collect_pyg_inc_data(year=year, database_url=database_url)
    out: list[ConsolidatedEntityMonth] = []
    fx_rows: list[FxRateAuditRow] = []
    for yyyymm in month_keys(year):
        product_sales, a = _sum_stage(bundle.sales_rows, yyyymm=yyyymm, fx=fx); fx_rows.extend(a)
        manufacturing, a = _sum_expense(bundle.expense_rows, yyyymm=yyyymm, fx=fx, subcategory="manufacturing"); fx_rows.extend(a)
        logistics, a = _sum_expense(bundle.expense_rows, yyyymm=yyyymm, fx=fx, subcategory="logistics"); fx_rows.extend(a)
        payment_fees, a = _sum_payment_fees(bundle.payment_fee_rows, yyyymm=yyyymm, fx=fx); fx_rows.extend(a)
        shared_services, a = _sum_expense(bundle.expense_rows, yyyymm=yyyymm, fx=fx, subcategory="shared_services"); fx_rows.extend(a)
        administration, a = _sum_expense(bundle.expense_rows, yyyymm=yyyymm, fx=fx, subcategory="administration"); fx_rows.extend(a)
        technology, a = _sum_expense(bundle.expense_rows, yyyymm=yyyymm, fx=fx, subcategory="technology"); fx_rows.extend(a)
        otros_gastos, a = _sum_expense(bundle.expense_rows, yyyymm=yyyymm, fx=fx, subcategory="otros_gastos"); fx_rows.extend(a)
        otros_ingresos = bundle.otros_ingresos_by_period.get(yyyymm, Decimal("0"))
        diferencias_divisas = bundle.diferencias_divisas_by_period.get(yyyymm, Decimal("0"))
        out.append(_compose_row(
            company_code="INC",
            yyyymm=yyyymm,
            product_sales=product_sales,
            otros_ingresos=otros_ingresos,
            diferencias_divisas=diferencias_divisas,
            manufacturing=manufacturing,
            logistics=logistics,
            payment_fees=payment_fees,
            shared_services=shared_services,
            administration=administration,
            technology=technology,
            otros_gastos=otros_gastos,
        ))
    return out, tuple(fx_rows)


def _sum_stage(rows: Iterable, *, yyyymm: str, fx: EcbFxService) -> tuple[Decimal, tuple[FxRateAuditRow, ...]]:
    total = Decimal("0")
    audits: list[FxRateAuditRow] = []
    for row in rows:
        if row.yyyymm != yyyymm:
            continue
        converted, audit = fx.convert(amount=row.amount_net, source_currency=row.currency, reporting_currency=REPORTING_CURRENCY, yyyymm=yyyymm)
        total += converted.amount_reporting
        audits.append(audit)
    return total, tuple(audits)


def _sum_expense(rows: Iterable, *, yyyymm: str, fx: EcbFxService, category: str | None = None, subcategory: str | None = None) -> tuple[Decimal, tuple[FxRateAuditRow, ...]]:
    total = Decimal("0")
    audits: list[FxRateAuditRow] = []
    for row in rows:
        if row.yyyymm != yyyymm:
            continue
        if category and row.category != category:
            continue
        if subcategory and row.subcategory != subcategory:
            continue
        converted, audit = fx.convert(amount=row.amount_net, source_currency=row.currency, reporting_currency=REPORTING_CURRENCY, yyyymm=yyyymm)
        total += converted.amount_reporting
        audits.append(audit)
    return total, tuple(audits)


def _sum_payment_fees(rows: Iterable, *, yyyymm: str, fx: EcbFxService) -> tuple[Decimal, tuple[FxRateAuditRow, ...]]:
    total = Decimal("0")
    audits: list[FxRateAuditRow] = []
    for row in rows:
        if row.yyyymm != yyyymm:
            continue
        converted, audit = fx.convert(amount=row.amount_net, source_currency=row.currency, reporting_currency=REPORTING_CURRENCY, yyyymm=yyyymm)
        total += converted.amount_reporting
        audits.append(audit)
    return total, tuple(audits)


def _compose_row(
    *,
    company_code: str,
    yyyymm: str,
    product_sales: Decimal = Decimal("0"),
    marketplaces: Decimal = Decimal("0"),
    services: Decimal = Decimal("0"),
    rappels: Decimal = Decimal("0"),
    supplies: Decimal = Decimal("0"),
    otros_ingresos: Decimal = Decimal("0"),
    diferencias_divisas: Decimal = Decimal("0"),
    manufacturing: Decimal = Decimal("0"),
    logistics: Decimal = Decimal("0"),
    royalties: Decimal = Decimal("0"),
    payment_fees: Decimal = Decimal("0"),
    marketing: Decimal = Decimal("0"),
    staff: Decimal = Decimal("0"),
    shared_services: Decimal = Decimal("0"),
    administration: Decimal = Decimal("0"),
    technology: Decimal = Decimal("0"),
    otros_gastos: Decimal = Decimal("0"),
) -> ConsolidatedEntityMonth:
    turnover = product_sales + marketplaces + services + rappels + supplies + otros_ingresos + diferencias_divisas
    cogs = manufacturing + logistics + royalties + payment_fees
    gross_margin = product_sales - manufacturing
    contributive_margin = turnover - cogs
    opex = marketing + staff + shared_services + administration + technology + otros_gastos
    expenses = cogs + opex
    profit = turnover - expenses
    return ConsolidatedEntityMonth(
        company_code=company_code,
        yyyymm=yyyymm,
        turnover=turnover,
        product_sales=product_sales,
        marketplaces=marketplaces,
        services=services,
        rappels=rappels,
        supplies=supplies,
        otros_ingresos=otros_ingresos,
        diferencias_divisas=diferencias_divisas,
        expenses=expenses,
        cogs=cogs,
        manufacturing=manufacturing,
        logistics=logistics,
        royalties=royalties,
        payment_fees=payment_fees,
        gross_margin=gross_margin,
        contributive_margin=contributive_margin,
        opex=opex,
        marketing=marketing,
        staff=staff,
        shared_services=shared_services,
        administration=administration,
        technology=technology,
        otros_gastos=otros_gastos,
        profit=profit,
    )


def _aggregate_totals(rows: tuple[ConsolidatedEntityMonth, ...]) -> dict[str, dict[str, Decimal]]:
    keys = [
        "turnover", "product_sales", "marketplaces", "services", "rappels", "supplies", "otros_ingresos",
        "diferencias_divisas", "expenses", "cogs", "manufacturing", "logistics", "royalties", "payment_fees",
        "gross_margin", "contributive_margin", "opex", "marketing", "staff",
        "shared_services", "administration", "technology", "otros_gastos", "profit",
    ]
    totals: dict[str, dict[str, Decimal]] = {}
    for row in rows:
        bucket = totals.setdefault(row.yyyymm, {key: Decimal("0") for key in keys})
        for key in keys:
            bucket[key] += getattr(row, key)
    return totals


def _write_entity_detail_sheet(wb: Workbook, rows: tuple[ConsolidatedEntityMonth, ...]) -> None:
    ws = wb.create_sheet("entity-detail")
    headers = [
        "yyyymm", "company_code", "turnover", "product_sales", "marketplaces", "services",
        "rappels", "supplies", "otros_ingresos", "diferencias_divisas", "expenses", "cogs", "manufacturing", "logistics",
        "royalties", "payment_fees", "gross_margin", "contributive_margin", "opex",
        "marketing", "staff", "shared_services", "administration", "technology", "otros_gastos", "profit",
    ]
    ws.append(headers)
    for item in rows:
        ws.append([
            item.yyyymm, item.company_code, float(item.turnover), float(item.product_sales),
            float(item.marketplaces), float(item.services), float(item.rappels), float(item.supplies),
            float(item.otros_ingresos), float(item.diferencias_divisas), float(item.expenses), float(item.cogs), float(item.manufacturing), float(item.logistics),
            float(item.royalties), float(item.payment_fees), float(item.gross_margin), float(item.contributive_margin),
            float(item.opex), float(item.marketing), float(item.staff), float(item.shared_services),
            float(item.administration), float(item.technology), float(item.otros_gastos), float(item.profit),
        ])
    for cell in ws[1]:
        cell.fill = HEADER_FILL
        cell.font = WHITE_BOLD
    for idx, header in enumerate(headers, start=1):
        ws.column_dimensions[get_column_letter(idx)].width = max(14, len(header) + 2)
        if header not in {"yyyymm", "company_code"}:
            for row_idx in range(2, ws.max_row + 1):
                ws.cell(row=row_idx, column=idx).number_format = MONEY_FORMAT
    ws.freeze_panes = "A2"


def _write_fx_rates_sheet(wb: Workbook, rows: tuple[FxRateAuditRow, ...]) -> None:
    ws = wb.create_sheet("fx-rates")
    headers = ["yyyymm", "rate_date", "currency_original", "reporting_currency", "reference_rate", "fx_rate", "source"]
    ws.append(headers)
    for item in rows:
        ws.append([item.yyyymm, item.rate_date, item.currency_original, item.reporting_currency, float(item.reference_rate), float(item.fx_rate), item.source])
    for cell in ws[1]:
        cell.fill = HEADER_FILL
        cell.font = WHITE_BOLD
    for idx, header in enumerate(headers, start=1):
        ws.column_dimensions[get_column_letter(idx)].width = max(14, len(header) + 2)
        if header in {"reference_rate", "fx_rate"}:
            for row_idx in range(2, ws.max_row + 1):
                ws.cell(row=row_idx, column=idx).number_format = "0.00000000"
    ws.freeze_panes = "A2"


def _dedupe_fx_rows(rows: tuple[FxRateAuditRow, ...]) -> tuple[FxRateAuditRow, ...]:
    unique: dict[tuple[str, str, str], FxRateAuditRow] = {}
    for row in rows:
        unique[(row.yyyymm, row.currency_original, row.reporting_currency)] = row
    return tuple(sorted(unique.values(), key=lambda item: (item.yyyymm, item.currency_original, item.reporting_currency)))
