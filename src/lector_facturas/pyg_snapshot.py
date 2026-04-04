from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal

from lector_facturas.fx_rates import EcbFxService
from lector_facturas.google_drive import GoogleDriveClient
from lector_facturas.pyg_consolidated_workbook import REPORTING_CURRENCY as CONSOLIDATED_REPORTING_CURRENCY
from lector_facturas.pyg_inc_workbook import (
    COMPANY_CODE as INC_COMPANY_CODE,
    DEFAULT_ADMIN_LINES as INC_DEFAULT_ADMIN_LINES,
    DEFAULT_LOGISTICS_LINES as INC_DEFAULT_LOGISTICS_LINES,
    DEFAULT_MANUFACTURING_LINES as INC_DEFAULT_MANUFACTURING_LINES,
    DEFAULT_PAYMENT_FEE_LINES as INC_DEFAULT_PAYMENT_FEE_LINES,
    DEFAULT_SALES_MARKETS as INC_DEFAULT_SALES_MARKETS,
    DEFAULT_SHARED_SERVICE_LINES as INC_DEFAULT_SHARED_SERVICE_LINES,
    DEFAULT_TECH_LINES as INC_DEFAULT_TECH_LINES,
    REPORTING_CURRENCY as INC_REPORTING_CURRENCY,
    collect_pyg_inc_data,
)
from lector_facturas.pyg_inc_workbook import _map_expense_subcategory as map_inc_expense_subcategory
from lector_facturas.pyg_ltd_workbook import (
    COMPANY_CODE as LTD_COMPANY_CODE,
    DEFAULT_ADMIN_LINES as LTD_DEFAULT_ADMIN_LINES,
    DEFAULT_LOGISTICS_LINES as LTD_DEFAULT_LOGISTICS_LINES,
    DEFAULT_MANUFACTURING_LINES as LTD_DEFAULT_MANUFACTURING_LINES,
    DEFAULT_PAYMENT_FEE_LINES as LTD_DEFAULT_PAYMENT_FEE_LINES,
    DEFAULT_SALES_MARKETS as LTD_DEFAULT_SALES_MARKETS,
    DEFAULT_SHARED_SERVICE_LINES as LTD_DEFAULT_SHARED_SERVICE_LINES,
    DEFAULT_TECH_LINES as LTD_DEFAULT_TECH_LINES,
    REPORTING_CURRENCY as LTD_REPORTING_CURRENCY,
    collect_pyg_ltd_data,
)
from lector_facturas.pyg_ltd_workbook import _map_expense_subcategory as map_ltd_expense_subcategory
from lector_facturas.pyg_sl_workbook import (
    ADMINISTRATION_DETAIL_LINES,
    DEFAULT_MARKETING_REGIONS,
    DEFAULT_PAYMENT_FEE_LINES as SL_DEFAULT_PAYMENT_FEE_LINES,
    DEFAULT_SERVICE_LINES,
    REPORTING_CURRENCY as SL_REPORTING_CURRENCY,
    collect_pyg_sl_data,
)
from lector_facturas.pyg_sl_workbook import _provider_groups
from lector_facturas.settings import AppSettings

PygCompany = Literal["consolidado", "sl", "ltd", "inc"]
PygKind = Literal["major", "subtotal", "section", "detail", "metric", "info"]


@dataclass(frozen=True)
class PygSnapshotRow:
    code: str
    label: str
    level: int
    kind: PygKind
    parent_code: str | None
    style_key: str
    default_expanded: bool
    values_base: tuple[Decimal, ...]
    values_eur: tuple[Decimal, ...]

    @property
    def total_base(self) -> Decimal:
        return sum(self.values_base, Decimal("0"))

    @property
    def total_eur(self) -> Decimal:
        return sum(self.values_eur, Decimal("0"))


@dataclass(frozen=True)
class PygSnapshot:
    company: PygCompany
    base_currency: str
    months: tuple[str, ...]
    generated_at: datetime
    drive_file_name: str
    drive_file_url: str
    fx_mode: str
    rows: tuple[PygSnapshotRow, ...]


@dataclass(frozen=True)
class _RowDef:
    code: str
    label: str
    level: int
    kind: PygKind
    parent_code: str | None
    style_key: str
    default_expanded: bool = False


def month_window(*, year: int | None = None, start_yyyymm: str | None = None, mode: str = "year") -> list[str]:
    if mode == "rolling":
        anchor = start_yyyymm or f"{year or datetime.now().year}01"
    else:
        anchor_year = year or int((start_yyyymm or f"{datetime.now().year}01")[:4])
        anchor = f"{anchor_year}01"
    return [_shift_yyyymm(anchor, offset) for offset in range(12)]


def build_pyg_snapshot(
    *,
    company: PygCompany,
    months: list[str],
    database_url: str,
    settings: AppSettings | None = None,
) -> PygSnapshot:
    normalized_months = sorted(dict.fromkeys(months))
    if not normalized_months:
        raise ValueError("months is required")
    if company == "sl":
        return _build_sl_snapshot(months=normalized_months, database_url=database_url, settings=settings)
    if company == "ltd":
        return _build_ltd_snapshot(months=normalized_months, database_url=database_url, settings=settings)
    if company == "inc":
        return _build_inc_snapshot(months=normalized_months, database_url=database_url, settings=settings)
    if company == "consolidado":
        return _build_consolidated_snapshot(months=normalized_months, database_url=database_url, settings=settings)
    raise ValueError(f"Unsupported company: {company}")


def _build_sl_snapshot(*, months: list[str], database_url: str, settings: AppSettings | None) -> PygSnapshot:
    bundles = [collect_pyg_sl_data(year=year, database_url=database_url) for year in _years_for_months(months)]
    provider_rows = tuple(row for bundle in bundles for row in bundle.provider_catalog_rows)
    groups = _provider_groups(provider_rows)
    fx_service = EcbFxService()
    base_maps: dict[str, dict[str, Decimal]] = {}
    eur_maps: dict[str, dict[str, Decimal]] = {}

    for bundle in bundles:
        for row in bundle.shopify_rows:
            if row.yyyymm in months:
                code = f"shopify_{row.line_item.lower()}"
                _add_amount(base_maps, code, row.yyyymm, _to_currency(fx_service, row.amount_net, row.currency, SL_REPORTING_CURRENCY, row.yyyymm))
                _add_amount(eur_maps, code, row.yyyymm, _to_currency(fx_service, row.amount_net, row.currency, "EUR", row.yyyymm))
        for row in bundle.marketplace_rows:
            if row.yyyymm in months:
                code = f"marketplace_{row.line_item.lower()}"
                _add_amount(base_maps, code, row.yyyymm, _to_currency(fx_service, row.amount_net, row.currency, SL_REPORTING_CURRENCY, row.yyyymm))
                _add_amount(eur_maps, code, row.yyyymm, _to_currency(fx_service, row.amount_net, row.currency, "EUR", row.yyyymm))
        for row in bundle.rappel_rows:
            if row.yyyymm in months:
                _add_amount(base_maps, "rappel_livitum", row.yyyymm, _to_currency(fx_service, row.amount_net, row.currency, SL_REPORTING_CURRENCY, row.yyyymm))
                _add_amount(eur_maps, "rappel_livitum", row.yyyymm, _to_currency(fx_service, row.amount_net, row.currency, "EUR", row.yyyymm))
        for row in bundle.supplies_rows:
            if row.yyyymm in months:
                _add_amount(base_maps, "supplies_rever", row.yyyymm, _to_currency(fx_service, row.amount_net, row.currency, SL_REPORTING_CURRENCY, row.yyyymm))
                _add_amount(eur_maps, "supplies_rever", row.yyyymm, _to_currency(fx_service, row.amount_net, row.currency, "EUR", row.yyyymm))
        for row in bundle.service_rows:
            if row.yyyymm in months:
                code = f"service_{row.line_item.lower()}"
                base_value = _to_currency(fx_service, row.amount_net, row.currency, SL_REPORTING_CURRENCY, row.yyyymm)
                eur_value = _to_currency(fx_service, row.amount_net, row.currency, "EUR", row.yyyymm)
                _add_amount(base_maps, code, row.yyyymm, base_value)
                _add_amount(eur_maps, code, row.yyyymm, eur_value)
                if row.line_item not in {"Ltd", "Inc"} and row.detail != "renting_cnc":
                    _add_amount(base_maps, "services_external", row.yyyymm, base_value)
                    _add_amount(eur_maps, "services_external", row.yyyymm, eur_value)
        for row in bundle.expense_rows:
            if row.yyyymm not in months:
                continue
            base_value = _to_currency(fx_service, row.amount_net, row.currency, SL_REPORTING_CURRENCY, row.yyyymm)
            eur_value = _to_currency(fx_service, row.amount_net, row.currency, "EUR", row.yyyymm)
            if row.subcategory == "manufacturing":
                code = f"manufacturing_{row.supplier_code.lower()}"
            elif row.subcategory == "logistics":
                code = f"logistics_{row.supplier_code.lower()}"
            elif row.subcategory == "royalties":
                code = "royalties_total"
            elif row.subcategory == "marketing":
                region = (row.detail or "EU").upper()
                code = f"marketing_{row.supplier_code.lower()}_{region.lower()}"
            elif row.subcategory == "staff":
                code = f"staff_{row.supplier_code.lower()}"
            elif row.subcategory == "administration":
                code = f"administration_{row.supplier_code.lower()}"
                if row.supplier_code in ADMINISTRATION_DETAIL_LINES and row.detail:
                    detail_code = f"administration_{row.supplier_code.lower()}_{row.detail.lower().replace(' ', '_')}"
                    _add_amount(base_maps, detail_code, row.yyyymm, base_value)
                    _add_amount(eur_maps, detail_code, row.yyyymm, eur_value)
            elif row.subcategory == "technology":
                code = f"technology_{row.supplier_code.lower()}"
            elif row.subcategory == "otros_gastos":
                code = "otros_gastos"
            else:
                continue
            _add_amount(base_maps, code, row.yyyymm, base_value)
            _add_amount(eur_maps, code, row.yyyymm, eur_value)
        for row in bundle.payment_fee_rows:
            if row.yyyymm in months:
                code = f"payment_fee_{row.supplier_code.lower()}"
                _add_amount(base_maps, code, row.yyyymm, _to_currency(fx_service, row.amount_net, row.currency, SL_REPORTING_CURRENCY, row.yyyymm))
                _add_amount(eur_maps, code, row.yyyymm, _to_currency(fx_service, row.amount_net, row.currency, "EUR", row.yyyymm))
        for yyyymm, amount in bundle.otros_ingresos_by_period.items():
            if yyyymm in months:
                _add_amount(base_maps, "otros_ingresos", yyyymm, amount)
                _add_amount(eur_maps, "otros_ingresos", yyyymm, amount)
        for yyyymm, amount in bundle.diferencias_divisas_by_period.items():
            if yyyymm in months:
                _add_amount(base_maps, "diferencias_divisas", yyyymm, amount)
                _add_amount(eur_maps, "diferencias_divisas", yyyymm, amount)
        for scope, values in bundle.royalties_by_scope.items():
            for yyyymm, amount in values.items():
                if yyyymm in months:
                    code = f"royalties_{scope.lower()}"
                    _add_amount(base_maps, code, yyyymm, amount)
                    _add_amount(eur_maps, code, yyyymm, amount)

    row_defs: list[_RowDef] = [
        _RowDef("turnover", "Turnover", 0, "major", None, "major", True),
        _RowDef("product_sales", "Product sales", 1, "subtotal", "turnover", "subtotal", True),
        _RowDef("shopify", "Shopify", 2, "section", "product_sales", "section", True),
        *[_RowDef(f"shopify_{market.lower()}", market, 3, "detail", "shopify", "detail") for market in bundles[0].shopify_markets],
        _RowDef("marketplaces", "Marketplaces", 2, "section", "product_sales", "section", True),
        *[_RowDef(f"marketplace_{code.lower()}", code, 3, "detail", "marketplaces", "detail") for code in ("HANNUN", "TOASTY", "CHOOSE")],
        _RowDef("rappels", "Rappels", 2, "section", "product_sales", "section"),
        _RowDef("rappel_livitum", "LIVITUM", 3, "detail", "rappels", "detail"),
        _RowDef("supplies", "Supplies", 2, "section", "product_sales", "section"),
        _RowDef("supplies_rever", "REVER", 3, "detail", "supplies", "detail"),
        _RowDef("services", "Services", 1, "subtotal", "turnover", "subtotal", True),
        *[_RowDef(f"service_{code.lower()}", code, 2, "detail", "services", "detail") for code in DEFAULT_SERVICE_LINES],
        _RowDef("otros_ingresos_group", "Otros ingresos", 1, "section", "turnover", "section", True),
        _RowDef("otros_ingresos", "Otros ingresos", 2, "detail", "otros_ingresos_group", "detail"),
        _RowDef("expenses", "Expenses", 0, "major", None, "major", True),
        _RowDef("cogs", "COGS", 1, "subtotal", "expenses", "subtotal", True),
        _RowDef("manufacturing", "Manufacturing", 2, "section", "cogs", "section", True),
        *[_RowDef(f"manufacturing_{supplier.lower()}", supplier, 3, "detail", "manufacturing", "detail") for supplier in groups["manufacturing"]],
        _RowDef("manufacturing_pct", "% Manufacturing / sales", 2, "metric", "manufacturing", "metric"),
        _RowDef("logistics", "Logistics", 2, "section", "cogs", "section", True),
        *[_RowDef(f"logistics_{supplier.lower()}", supplier, 3, "detail", "logistics", "detail") for supplier in groups["logistics"]],
        _RowDef("logistics_pct", "% Logistics / sales", 2, "metric", "logistics", "metric"),
        _RowDef("royalties", "Royalties", 2, "section", "cogs", "section"),
        _RowDef("royalties_total", "ROYALTIES", 3, "detail", "royalties", "detail"),
        _RowDef("royalties_eu", "eu", 3, "detail", "royalties", "detail"),
        _RowDef("royalties_uk", "uk", 3, "detail", "royalties", "detail"),
        _RowDef("royalties_us", "us", 3, "detail", "royalties", "detail"),
        _RowDef("royalties_pct", "% Royalties / sales", 2, "metric", "royalties", "metric"),
        _RowDef("payment_fees", "Payment fees", 2, "section", "cogs", "section"),
        *[_RowDef(f"payment_fee_{code.lower()}", code, 3, "detail", "payment_fees", "detail") for code in SL_DEFAULT_PAYMENT_FEE_LINES],
        _RowDef("payment_fees_pct", "% Payment fees / sales", 2, "metric", "payment_fees", "metric"),
        _RowDef("gross_margin", "GROSS MARGIN (SALES-MANUFACTURING)", 0, "metric", None, "kpi"),
        _RowDef("gross_margin_pct", "% GROSS MARGIN", 0, "metric", None, "kpi"),
        _RowDef("contributive_margin", "CONTRIBUTIVE MARGIN (TURNOVER-COGS)", 0, "metric", None, "kpi"),
        _RowDef("contributive_margin_pct", "% CONTRIBUTIVE MARGIN", 0, "metric", None, "kpi"),
        _RowDef("opex", "Opex", 1, "subtotal", "expenses", "subtotal", True),
        _RowDef("marketing", "Marketing", 2, "section", "opex", "section", True),
        _RowDef("marketing_metaads", "METAADS", 3, "section", "marketing", "section"),
        *[_RowDef(f"marketing_metaads_{region.lower()}", region, 4, "detail", "marketing_metaads", "detail") for region in DEFAULT_MARKETING_REGIONS],
        _RowDef("marketing_googleads", "GOOGLEADS", 3, "section", "marketing", "section"),
        *[_RowDef(f"marketing_googleads_{region.lower()}", region, 4, "detail", "marketing_googleads", "detail") for region in DEFAULT_MARKETING_REGIONS],
        _RowDef("marketing_pct", "Sales / mkt EU", 2, "metric", "marketing", "metric"),
        _RowDef("staff", "Staff", 2, "section", "opex", "section"),
        _RowDef("staff_payroll", "PAYROLL", 3, "detail", "staff", "detail"),
        _RowDef("staff_dosconsulting", "DOSCONSULTING", 3, "detail", "staff", "detail"),
        _RowDef("administration", "Administration", 2, "section", "opex", "section", True),
        *[
            row_def
            for supplier in groups["administration"]
            for row_def in (
                [_RowDef(f"administration_{supplier.lower()}", supplier, 3, "detail", "administration", "detail")]
                + [
                    _RowDef(
                        f"administration_{supplier.lower()}_{detail.lower().replace(' ', '_')}",
                        detail,
                        4,
                        "detail",
                        f"administration_{supplier.lower()}",
                        "detail",
                    )
                    for detail in ADMINISTRATION_DETAIL_LINES.get(supplier, [])
                ]
            )
        ],
        _RowDef("technology", "Technology", 2, "section", "opex", "section", True),
        *[_RowDef(f"technology_{supplier.lower()}", supplier, 3, "detail", "technology", "detail") for supplier in groups["technology"]],
        _RowDef("otros_gastos_group", "Otros gastos", 2, "section", "opex", "section", True),
        _RowDef("otros_gastos", "Otros gastos", 3, "detail", "otros_gastos_group", "detail"),
        _RowDef("diferencias_divisas_group", "Diferencias divisas", 1, "section", "expenses", "section", True),
        _RowDef("diferencias_divisas", "Diferencias divisas", 2, "detail", "diferencias_divisas_group", "detail"),
        _RowDef("profit", "PROFIT", 0, "metric", None, "kpi"),
        _RowDef("profit_pct", "% Profit / product sales", 0, "metric", None, "kpi"),
    ]
    rows = _materialize_rows(
        row_defs=row_defs,
        months=months,
        base_maps=base_maps,
        eur_maps=eur_maps,
        formulas={
            "shopify": ("sum_children",),
            "marketplaces": ("sum_children",),
            "rappels": ("sum_children",),
            "supplies": ("sum_children",),
            "services": ("sum_children",),
            "otros_ingresos_group": ("sum_children",),
            "product_sales": ("sum_codes", ("shopify", "marketplaces", "rappels", "supplies")),
            "turnover": ("sum_codes", ("product_sales", "services", "otros_ingresos_group")),
            "manufacturing": ("sum_children",),
            "manufacturing_pct": ("ratio", "manufacturing", "product_sales"),
            "logistics": ("sum_children",),
            "logistics_pct": ("ratio", "logistics", "product_sales"),
            "royalties": ("sum_codes", ("royalties_total",)),
            "royalties_pct": ("ratio", "royalties", "product_sales"),
            "payment_fees": ("sum_children",),
            "payment_fees_pct": ("ratio", "payment_fees", "product_sales"),
            "cogs": ("sum_codes", ("manufacturing", "logistics", "royalties", "payment_fees")),
            "gross_margin": ("diff", "product_sales", "manufacturing"),
            "gross_margin_pct": ("ratio", "gross_margin", "product_sales"),
            "contributive_margin": ("diff", "turnover", "cogs"),
            "contributive_margin_pct": ("ratio", "contributive_margin", "product_sales"),
            "marketing_metaads": ("sum_children",),
            "marketing_googleads": ("sum_children",),
            "marketing": ("sum_codes", ("marketing_metaads", "marketing_googleads")),
            "marketing_pct": ("ratio", "product_sales", "marketing"),
            "staff": ("sum_children",),
            "administration": ("sum_first_level_children",),
            "technology": ("sum_children",),
            "otros_gastos_group": ("sum_children",),
            "diferencias_divisas_group": ("sum_children",),
            "opex": ("sum_codes", ("marketing", "staff", "administration", "technology", "otros_gastos_group")),
            "expenses": ("sum_codes", ("cogs", "opex")),
            "profit": ("subtract_many", "turnover", ("cogs", "opex", "diferencias_divisas_group")),
            "profit_pct": ("ratio", "profit", "product_sales"),
        },
    )
    return PygSnapshot(
        company="sl",
        base_currency=SL_REPORTING_CURRENCY,
        months=tuple(months),
        generated_at=max(bundle.generated_at for bundle in bundles),
        drive_file_name=f"pyg_sl_{_official_year(months)}.xlsx",
        drive_file_url=_find_drive_file_url(settings=settings, file_name=f"pyg_sl_{_official_year(months)}.xlsx"),
        fx_mode="monthly_historical",
        rows=rows,
    )


def _build_ltd_snapshot(*, months: list[str], database_url: str, settings: AppSettings | None) -> PygSnapshot:
    return _build_simple_company_snapshot(
        company="ltd",
        reporting_currency=LTD_REPORTING_CURRENCY,
        months=months,
        database_url=database_url,
        settings=settings,
        collect_bundle=collect_pyg_ltd_data,
        expense_mapper=map_ltd_expense_subcategory,
        sales_markets=LTD_DEFAULT_SALES_MARKETS,
        manufacturing_lines=LTD_DEFAULT_MANUFACTURING_LINES,
        logistics_lines=LTD_DEFAULT_LOGISTICS_LINES,
        payment_fee_lines=LTD_DEFAULT_PAYMENT_FEE_LINES,
        shared_service_lines=LTD_DEFAULT_SHARED_SERVICE_LINES,
        administration_lines=LTD_DEFAULT_ADMIN_LINES,
        technology_lines=LTD_DEFAULT_TECH_LINES,
        file_name=f"pyg_ltd_{_official_year(months)}.xlsx",
    )


def _build_inc_snapshot(*, months: list[str], database_url: str, settings: AppSettings | None) -> PygSnapshot:
    return _build_simple_company_snapshot(
        company="inc",
        reporting_currency=INC_REPORTING_CURRENCY,
        months=months,
        database_url=database_url,
        settings=settings,
        collect_bundle=collect_pyg_inc_data,
        expense_mapper=map_inc_expense_subcategory,
        sales_markets=INC_DEFAULT_SALES_MARKETS,
        manufacturing_lines=INC_DEFAULT_MANUFACTURING_LINES,
        logistics_lines=INC_DEFAULT_LOGISTICS_LINES,
        payment_fee_lines=INC_DEFAULT_PAYMENT_FEE_LINES,
        shared_service_lines=INC_DEFAULT_SHARED_SERVICE_LINES,
        administration_lines=INC_DEFAULT_ADMIN_LINES,
        technology_lines=INC_DEFAULT_TECH_LINES,
        file_name=f"pyg_inc_{_official_year(months)}.xlsx",
    )


def _materialize_rows(
    *,
    row_defs: list[_RowDef],
    months: list[str],
    base_maps: dict[str, dict[str, Decimal]],
    eur_maps: dict[str, dict[str, Decimal]],
    formulas: dict[str, tuple[object, ...]],
) -> tuple[PygSnapshotRow, ...]:
    child_map: dict[str, list[str]] = {}
    row_kind_map = {row.code: row.kind for row in row_defs}
    for row in row_defs:
        if row.parent_code:
            child_map.setdefault(row.parent_code, []).append(row.code)

    computed_base: dict[str, tuple[Decimal, ...]] = {}
    computed_eur: dict[str, tuple[Decimal, ...]] = {}

    def values_for(code: str) -> tuple[Decimal, ...]:
        if code not in computed_base:
            computed_base[code] = _compute_values(code, months, base_maps, child_map, row_kind_map, formulas, computed_base)
        return computed_base[code]

    def eur_values_for(code: str) -> tuple[Decimal, ...]:
        if code not in computed_eur:
            computed_eur[code] = _compute_values(code, months, eur_maps, child_map, row_kind_map, formulas, computed_eur)
        return computed_eur[code]

    return tuple(
        PygSnapshotRow(
            code=row.code,
            label=row.label,
            level=row.level,
            kind=row.kind,
            parent_code=row.parent_code,
            style_key=row.style_key,
            default_expanded=row.default_expanded,
            values_base=values_for(row.code),
            values_eur=eur_values_for(row.code),
        )
        for row in row_defs
    )


def _compute_values(
    code: str,
    months: list[str],
    maps: dict[str, dict[str, Decimal]],
    child_map: dict[str, list[str]],
    row_kind_map: dict[str, str],
    formulas: dict[str, tuple[object, ...]],
    cache: dict[str, tuple[Decimal, ...]],
    stack: tuple[str, ...] = (),
) -> tuple[Decimal, ...]:
    if code in cache:
        return cache[code]
    if code in stack:
        raise RuntimeError(f"Cycle detected in PYG snapshot rows: {' -> '.join((*stack, code))}")
    if code in maps and code not in formulas:
        cache[code] = tuple(maps[code].get(month, Decimal("0")) for month in months)
        return cache[code]
    formula = formulas.get(code)
    if not formula:
        cache[code] = tuple(maps.get(code, {}).get(month, Decimal("0")) for month in months)
        return cache[code]
    op = formula[0]
    if op == "sum_children":
        children = [child for child in child_map.get(code, []) if row_kind_map.get(child) not in {"metric", "info"}]
        cache[code] = tuple(sum((_compute_values(child, months, maps, child_map, row_kind_map, formulas, cache, (*stack, code))[idx] for child in children), Decimal("0")) for idx in range(len(months)))
    elif op == "sum_first_level_children":
        children = [child for child in child_map.get(code, []) if child_map.get(child) and row_kind_map.get(child) not in {"metric", "info"}] or [child for child in child_map.get(code, []) if row_kind_map.get(child) not in {"metric", "info"}]
        cache[code] = tuple(sum((_compute_values(child, months, maps, child_map, row_kind_map, formulas, cache, (*stack, code))[idx] for child in children), Decimal("0")) for idx in range(len(months)))
    elif op == "sum_codes":
        codes = formula[1]
        cache[code] = tuple(sum((_compute_values(child, months, maps, child_map, row_kind_map, formulas, cache, (*stack, code))[idx] for child in codes), Decimal("0")) for idx in range(len(months)))
    elif op == "diff":
        left = _compute_values(formula[1], months, maps, child_map, row_kind_map, formulas, cache, (*stack, code))
        right = _compute_values(formula[2], months, maps, child_map, row_kind_map, formulas, cache, (*stack, code))
        cache[code] = tuple(left[idx] - right[idx] for idx in range(len(months)))
    elif op == "subtract_many":
        left = _compute_values(formula[1], months, maps, child_map, row_kind_map, formulas, cache, (*stack, code))
        rights = formula[2]
        cache[code] = tuple(left[idx] - sum((_compute_values(right, months, maps, child_map, row_kind_map, formulas, cache, (*stack, code))[idx] for right in rights), Decimal("0")) for idx in range(len(months)))
    elif op == "ratio":
        numerator = _compute_values(formula[1], months, maps, child_map, row_kind_map, formulas, cache, (*stack, code))
        denominator = _compute_values(formula[2], months, maps, child_map, row_kind_map, formulas, cache, (*stack, code))
        cache[code] = tuple((numerator[idx] / denominator[idx]) if denominator[idx] else Decimal("0") for idx in range(len(months)))
    else:
        cache[code] = tuple(maps.get(code, {}).get(month, Decimal("0")) for month in months)
    return cache[code]


def _build_simple_company_snapshot(
    *,
    company: PygCompany,
    reporting_currency: str,
    months: list[str],
    database_url: str,
    settings: AppSettings | None,
    collect_bundle,
    expense_mapper,
    sales_markets: tuple[str, ...],
    manufacturing_lines: tuple[str, ...],
    logistics_lines: tuple[str, ...],
    payment_fee_lines: tuple[str, ...],
    shared_service_lines: tuple[str, ...],
    administration_lines: tuple[str, ...],
    technology_lines: tuple[str, ...],
    file_name: str,
) -> PygSnapshot:
    bundles = [collect_bundle(year=year, database_url=database_url) for year in _years_for_months(months)]
    fx_service = EcbFxService()
    base_maps: dict[str, dict[str, Decimal]] = {}
    eur_maps: dict[str, dict[str, Decimal]] = {}

    for bundle in bundles:
        supplier_map = {row.supplier_code: row for row in bundle.provider_catalog_rows}
        for row in bundle.sales_rows:
            if row.yyyymm in months:
                code = f"shopify_{row.line_item.lower()}"
                _add_amount(base_maps, code, row.yyyymm, _to_currency(fx_service, row.amount_net, row.currency, reporting_currency, row.yyyymm))
                _add_amount(eur_maps, code, row.yyyymm, _to_currency(fx_service, row.amount_net, row.currency, "EUR", row.yyyymm))
        for row in bundle.expense_rows:
            if row.yyyymm not in months:
                continue
            supplier_meta = supplier_map.get(row.supplier_code)
            subcategory = expense_mapper(
                supplier_code=row.supplier_code,
                division_invoice=row.detail or "",
                supplier_meta=supplier_meta.__dict__ if supplier_meta else None,
            ) or row.subcategory
            base_value = _to_currency(fx_service, row.amount_net, row.currency, reporting_currency, row.yyyymm)
            eur_value = _to_currency(fx_service, row.amount_net, row.currency, "EUR", row.yyyymm)
            if subcategory == "manufacturing":
                code = f"manufacturing_{row.supplier_code.lower()}"
            elif subcategory == "logistics":
                code = f"logistics_{row.supplier_code.lower()}"
            elif subcategory == "shared_services":
                code = f"shared_services_{row.supplier_code.lower()}"
            elif subcategory == "administration":
                code = f"administration_{row.supplier_code.lower()}"
            elif subcategory == "technology":
                code = f"technology_{row.supplier_code.lower()}"
            elif subcategory == "otros_gastos":
                code = "otros_gastos"
            else:
                continue
            _add_amount(base_maps, code, row.yyyymm, base_value)
            _add_amount(eur_maps, code, row.yyyymm, eur_value)
        for row in bundle.payment_fee_rows:
            if row.yyyymm in months:
                code = f"payment_fee_{row.supplier_code.lower()}"
                _add_amount(base_maps, code, row.yyyymm, _to_currency(fx_service, row.amount_net, row.currency, reporting_currency, row.yyyymm))
                _add_amount(eur_maps, code, row.yyyymm, _to_currency(fx_service, row.amount_net, row.currency, "EUR", row.yyyymm))
        for yyyymm, amount in bundle.otros_ingresos_by_period.items():
            if yyyymm in months:
                _add_amount(base_maps, "otros_ingresos", yyyymm, amount)
                _add_amount(eur_maps, "otros_ingresos", yyyymm, amount)
        for yyyymm, amount in bundle.diferencias_divisas_by_period.items():
            if yyyymm in months:
                _add_amount(base_maps, "diferencias_divisas", yyyymm, amount)
                _add_amount(eur_maps, "diferencias_divisas", yyyymm, amount)
        for yyyymm, amount in bundle.frame_consumed_by_period.items():
            if yyyymm in months:
                _add_amount(base_maps, "marcos_consumed", yyyymm, amount)
                _add_amount(eur_maps, "marcos_consumed", yyyymm, _to_currency(fx_service, amount, reporting_currency, "EUR", yyyymm))
        for yyyymm, amount in bundle.frame_opening_by_period.items():
            if yyyymm in months:
                _add_amount(base_maps, "stock_inicial", yyyymm, amount)
                _add_amount(eur_maps, "stock_inicial", yyyymm, _to_currency(fx_service, amount, reporting_currency, "EUR", yyyymm))
        for yyyymm, amount in bundle.frame_closing_by_period.items():
            if yyyymm in months:
                _add_amount(base_maps, "stock_final", yyyymm, amount)
                _add_amount(eur_maps, "stock_final", yyyymm, _to_currency(fx_service, amount, reporting_currency, "EUR", yyyymm))

    row_defs = [
        _RowDef("turnover", "Turnover", 0, "major", None, "major", True),
        _RowDef("product_sales", "Product sales", 1, "subtotal", "turnover", "subtotal", True),
        _RowDef("shopify", "Shopify", 2, "section", "product_sales", "section", True),
        *[_RowDef(f"shopify_{market.lower()}", market, 3, "detail", "shopify", "detail") for market in sales_markets],
        _RowDef("otros_ingresos_group", "Otros ingresos", 1, "section", "turnover", "section", True),
        _RowDef("otros_ingresos", "Otros ingresos", 2, "detail", "otros_ingresos_group", "detail"),
        _RowDef("expenses", "Expenses", 0, "major", None, "major", True),
        _RowDef("cogs", "COGS", 1, "subtotal", "expenses", "subtotal", True),
        _RowDef("manufacturing", "Manufacturing", 2, "section", "cogs", "section", True),
        *[_RowDef(f"manufacturing_{code.lower()}", code, 3, "detail", "manufacturing", "detail") for code in manufacturing_lines],
        _RowDef("marcos_consumed", "Consumo marcos", 3, "detail", "manufacturing", "detail"),
        _RowDef("manufacturing_pct", "% Manufacturing / sales", 2, "metric", "manufacturing", "metric"),
        _RowDef("logistics", "Logistics", 2, "section", "cogs", "section", True),
        *[_RowDef(f"logistics_{code.lower()}", code, 3, "detail", "logistics", "detail") for code in logistics_lines],
        _RowDef("logistics_pct", "% Logistics / sales", 2, "metric", "logistics", "metric"),
        _RowDef("payment_fees", "Payment fees", 2, "section", "cogs", "section"),
        *[_RowDef(f"payment_fee_{code.lower()}", code, 3, "detail", "payment_fees", "detail") for code in payment_fee_lines],
        _RowDef("payment_fees_pct", "% Payment fees / sales", 2, "metric", "payment_fees", "metric"),
        _RowDef("gross_margin", "GROSS MARGIN (SALES-MANUFACTURING)", 0, "metric", None, "kpi"),
        _RowDef("gross_margin_pct", "% GROSS MARGIN", 0, "metric", None, "kpi"),
        _RowDef("contributive_margin", "CONTRIBUTIVE MARGIN (TURNOVER-COGS)", 0, "metric", None, "kpi"),
        _RowDef("contributive_margin_pct", "% CONTRIBUTIVE MARGIN", 0, "metric", None, "kpi"),
        _RowDef("opex", "Opex", 1, "subtotal", "expenses", "subtotal", True),
        _RowDef("shared_services", "Shared services", 2, "section", "opex", "section"),
        *[_RowDef(f"shared_services_{code.lower()}", code, 3, "detail", "shared_services", "detail") for code in shared_service_lines],
        _RowDef("administration", "Administration", 2, "section", "opex", "section"),
        *[_RowDef(f"administration_{code.lower()}", code, 3, "detail", "administration", "detail") for code in administration_lines],
        _RowDef("technology", "Technology", 2, "section", "opex", "section"),
        *[_RowDef(f"technology_{code.lower()}", code, 3, "detail", "technology", "detail") for code in technology_lines],
        _RowDef("otros_gastos_group", "Otros gastos", 2, "section", "opex", "section", True),
        _RowDef("otros_gastos", "Otros gastos", 3, "detail", "otros_gastos_group", "detail"),
        _RowDef("diferencias_divisas_group", "Diferencias divisas", 1, "section", "expenses", "section", True),
        _RowDef("diferencias_divisas", "Diferencias divisas", 2, "detail", "diferencias_divisas_group", "detail"),
        _RowDef("profit", "PROFIT", 0, "metric", None, "kpi"),
        _RowDef("profit_pct", "% Profit / product sales", 0, "metric", None, "kpi"),
        _RowDef("stock_inicial", "Stock inicial marcos", 0, "info", None, "info"),
        _RowDef("stock_final", "Stock final marcos", 0, "info", None, "info"),
    ]
    rows = _materialize_rows(
        row_defs=row_defs,
        months=months,
        base_maps=base_maps,
        eur_maps=eur_maps,
        formulas={
            "shopify": ("sum_children",),
            "otros_ingresos_group": ("sum_children",),
            "product_sales": ("sum_codes", ("shopify",)),
            "turnover": ("sum_codes", ("product_sales", "otros_ingresos_group")),
            "manufacturing": ("sum_children",),
            "manufacturing_pct": ("ratio", "manufacturing", "product_sales"),
            "logistics": ("sum_children",),
            "logistics_pct": ("ratio", "logistics", "product_sales"),
            "payment_fees": ("sum_children",),
            "payment_fees_pct": ("ratio", "payment_fees", "product_sales"),
            "cogs": ("sum_codes", ("manufacturing", "logistics", "payment_fees")),
            "gross_margin": ("diff", "product_sales", "manufacturing"),
            "gross_margin_pct": ("ratio", "gross_margin", "product_sales"),
            "contributive_margin": ("diff", "turnover", "cogs"),
            "contributive_margin_pct": ("ratio", "contributive_margin", "product_sales"),
            "shared_services": ("sum_children",),
            "administration": ("sum_children",),
            "technology": ("sum_children",),
            "otros_gastos_group": ("sum_children",),
            "diferencias_divisas_group": ("sum_children",),
            "opex": ("sum_codes", ("shared_services", "administration", "technology", "otros_gastos_group")),
            "expenses": ("sum_codes", ("cogs", "opex")),
            "profit": ("subtract_many", "turnover", ("cogs", "opex", "diferencias_divisas_group")),
            "profit_pct": ("ratio", "profit", "product_sales"),
        },
    )
    return PygSnapshot(
        company=company,
        base_currency=reporting_currency,
        months=tuple(months),
        generated_at=max(bundle.generated_at for bundle in bundles),
        drive_file_name=file_name,
        drive_file_url=_find_drive_file_url(settings=settings, file_name=file_name),
        fx_mode="monthly_historical",
        rows=rows,
    )


def _build_consolidated_snapshot(*, months: list[str], database_url: str, settings: AppSettings | None) -> PygSnapshot:
    sl = _build_sl_snapshot(months=months, database_url=database_url, settings=settings)
    ltd = _build_ltd_snapshot(months=months, database_url=database_url, settings=settings)
    inc = _build_inc_snapshot(months=months, database_url=database_url, settings=settings)
    base_maps: dict[str, dict[str, Decimal]] = {}
    eur_maps: dict[str, dict[str, Decimal]] = {}
    source_rows = {"sl": _snapshot_row_map(sl), "ltd": _snapshot_row_map(ltd), "inc": _snapshot_row_map(inc)}

    def load(code: str, month: str, company_key: str) -> Decimal:
        row = source_rows[company_key].get(code)
        if row is None:
            return Decimal("0")
        idx = sl.months.index(month)
        return row.values_eur[idx]

    for month in months:
        _set_amount(base_maps, "shopify_sl", month, load("shopify", month, "sl"))
        _set_amount(base_maps, "shopify_ltd", month, load("product_sales", month, "ltd"))
        _set_amount(base_maps, "shopify_inc", month, load("product_sales", month, "inc"))
        _set_amount(base_maps, "marketplaces", month, load("marketplaces", month, "sl"))
        _set_amount(base_maps, "services", month, load("services_external", month, "sl"))
        _set_amount(base_maps, "rappels", month, load("rappels", month, "sl"))
        _set_amount(base_maps, "supplies", month, load("supplies", month, "sl"))
        _set_amount(base_maps, "otros_ingresos", month, load("otros_ingresos_group", month, "sl") + load("otros_ingresos_group", month, "ltd") + load("otros_ingresos_group", month, "inc"))
        _set_amount(
            base_maps,
            "manufacturing",
            month,
            (load("manufacturing", month, "sl") - load("manufacturing_bbvacnc", month, "sl")) + load("manufacturing", month, "ltd") + load("manufacturing", month, "inc"),
        )
        for key in ("logistics", "payment_fees", "marketing", "staff", "administration", "technology", "otros_gastos_group", "diferencias_divisas_group"):
            _set_amount(base_maps, key, month, load(key, month, "sl") + load(key, month, "ltd") + load(key, month, "inc"))
        _set_amount(base_maps, "royalties", month, load("royalties_total", month, "sl"))
    eur_maps = {key: dict(values) for key, values in base_maps.items()}

    row_defs = [
        _RowDef("turnover", "TURNOVER", 0, "major", None, "major", True),
        _RowDef("product_sales", "Product sales", 1, "subtotal", "turnover", "subtotal", True),
        _RowDef("shopify", "Shopify", 2, "section", "product_sales", "section", True),
        _RowDef("shopify_sl", "SL", 3, "detail", "shopify", "detail"),
        _RowDef("shopify_ltd", "Ltd", 3, "detail", "shopify", "detail"),
        _RowDef("shopify_inc", "Inc", 3, "detail", "shopify", "detail"),
        _RowDef("marketplaces", "Marketplaces", 2, "section", "product_sales", "section"),
        _RowDef("services", "Services", 2, "section", "turnover", "section"),
        _RowDef("rappels", "Rappels", 2, "section", "product_sales", "section"),
        _RowDef("supplies", "Supplies", 2, "section", "product_sales", "section"),
        _RowDef("otros_ingresos", "Otros ingresos", 2, "section", "turnover", "section"),
        _RowDef("expenses", "EXPENSES", 0, "major", None, "major", True),
        _RowDef("cogs", "COGS", 1, "subtotal", "expenses", "subtotal", True),
        _RowDef("manufacturing", "Manufacturing", 2, "section", "cogs", "section"),
        _RowDef("logistics", "Logistics", 2, "section", "cogs", "section"),
        _RowDef("royalties", "Royalties", 2, "section", "cogs", "section"),
        _RowDef("payment_fees", "Payment fees", 2, "section", "cogs", "section"),
        _RowDef("gross_margin", "GROSS MARGIN (SALES-MANUFACTURING)", 0, "metric", None, "kpi"),
        _RowDef("gross_margin_pct", "% GROSS MARGIN", 0, "metric", None, "kpi"),
        _RowDef("contributive_margin", "CONTRIBUTIVE MARGIN (TURNOVER-COGS)", 0, "metric", None, "kpi"),
        _RowDef("contributive_margin_pct", "% CONTRIBUTIVE MARGIN", 0, "metric", None, "kpi"),
        _RowDef("opex", "OPEX", 1, "subtotal", "expenses", "subtotal", True),
        _RowDef("marketing", "Marketing", 2, "section", "opex", "section"),
        _RowDef("staff", "Staff", 2, "section", "opex", "section"),
        _RowDef("administration", "Administration", 2, "section", "opex", "section"),
        _RowDef("technology", "Technology", 2, "section", "opex", "section"),
        _RowDef("otros_gastos_group", "Otros gastos", 2, "section", "opex", "section"),
        _RowDef("diferencias_divisas_group", "Diferencias divisas", 1, "section", "expenses", "section"),
        _RowDef("profit", "PROFIT", 0, "metric", None, "kpi"),
        _RowDef("profit_pct", "% Profit / product sales", 0, "metric", None, "kpi"),
    ]
    rows = _materialize_rows(
        row_defs=row_defs,
        months=months,
        base_maps=base_maps,
        eur_maps=eur_maps,
        formulas={
            "shopify": ("sum_children",),
            "product_sales": ("sum_codes", ("shopify", "marketplaces", "rappels", "supplies")),
            "turnover": ("sum_codes", ("product_sales", "services", "otros_ingresos")),
            "cogs": ("sum_codes", ("manufacturing", "logistics", "royalties", "payment_fees")),
            "gross_margin": ("diff", "product_sales", "manufacturing"),
            "gross_margin_pct": ("ratio", "gross_margin", "product_sales"),
            "contributive_margin": ("diff", "turnover", "cogs"),
            "contributive_margin_pct": ("ratio", "contributive_margin", "product_sales"),
            "opex": ("sum_codes", ("marketing", "staff", "administration", "technology", "otros_gastos_group")),
            "expenses": ("sum_codes", ("cogs", "opex")),
            "profit": ("subtract_many", "turnover", ("cogs", "opex", "diferencias_divisas_group")),
            "profit_pct": ("ratio", "profit", "product_sales"),
        },
    )
    return PygSnapshot(
        company="consolidado",
        base_currency=CONSOLIDATED_REPORTING_CURRENCY,
        months=tuple(months),
        generated_at=max(sl.generated_at, ltd.generated_at, inc.generated_at),
        drive_file_name=f"pyg_consolidado_{_official_year(months)}.xlsx",
        drive_file_url=_find_drive_file_url(settings=settings, file_name=f"pyg_consolidado_{_official_year(months)}.xlsx"),
        fx_mode="monthly_historical",
        rows=rows,
    )


def _years_for_months(months: list[str]) -> list[int]:
    return sorted({int(month[:4]) for month in months})


def _shift_yyyymm(yyyymm: str, offset: int) -> str:
    year = int(yyyymm[:4])
    month = int(yyyymm[4:])
    total = year * 12 + (month - 1) + offset
    next_year, next_month = divmod(total, 12)
    return f"{next_year:04d}{next_month + 1:02d}"


def _official_year(months: list[str]) -> int:
    return int(months[0][:4])


def _to_currency(fx_service: EcbFxService, amount: Decimal, source_currency: str, target_currency: str, yyyymm: str) -> Decimal:
    return fx_service.convert(amount=amount, source_currency=source_currency, reporting_currency=target_currency, yyyymm=yyyymm)[0].amount_reporting


def _add_amount(maps: dict[str, dict[str, Decimal]], code: str, month: str, amount: Decimal) -> None:
    maps.setdefault(code, {})
    maps[code][month] = maps[code].get(month, Decimal("0")) + amount


def _set_amount(maps: dict[str, dict[str, Decimal]], code: str, month: str, amount: Decimal) -> None:
    maps.setdefault(code, {})
    maps[code][month] = amount


def _snapshot_row_map(snapshot: PygSnapshot) -> dict[str, PygSnapshotRow]:
    return {row.code: row for row in snapshot.rows}


def _find_drive_file_url(*, settings: AppSettings | None, file_name: str) -> str:
    if settings is None or not settings.google_oauth_ready or not settings.drive_root_folder_id:
        return ""
    try:
        client = GoogleDriveClient(settings.to_drive_config())
        files = client.list_files(parent_id=settings.drive_root_folder_id, name=file_name)
        if not files:
            return ""
        return str(files[0].get("webViewLink", ""))
    except Exception:
        return ""
