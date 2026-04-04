from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from lector_facturas.fx_rates import EcbFxService
from lector_facturas.pyg_inc_workbook import (
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
    DEFAULT_PAYMENT_FEE_LINES as SL_DEFAULT_PAYMENT_FEE_LINES,
    REPORTING_CURRENCY as SL_REPORTING_CURRENCY,
    collect_pyg_sl_data,
)
from lector_facturas.pyg_snapshot import PygCompany, PygSnapshot, PygSnapshotRow, build_pyg_snapshot
from lector_facturas.settings import AppSettings

PygPeriodKind = Literal["month", "quarter", "total"]
PygCurrencyMode = Literal["base", "eur"]


@dataclass(frozen=True)
class PygCellDetailItem:
    company: str
    period_yyyymm: str
    label: str
    invoice_number: str
    amount_local: Decimal
    amount_base: Decimal
    amount_eur: Decimal
    currency: str
    source: str
    drive_url: str


@dataclass(frozen=True)
class PygCellDetail:
    company: PygCompany
    row_code: str
    row_label: str
    period_kind: PygPeriodKind
    period_key: str
    currency: str
    cell_amount: Decimal
    supported: bool
    is_reconciled: bool
    message: str
    items: tuple[PygCellDetailItem, ...]


def build_pyg_cell_detail(
    *,
    company: PygCompany,
    row_code: str,
    period_kind: PygPeriodKind,
    period_key: str,
    currency_mode: PygCurrencyMode,
    months: list[str],
    database_url: str,
    settings: AppSettings | None = None,
) -> PygCellDetail:
    snapshot = build_pyg_snapshot(company=company, months=months, database_url=database_url, settings=settings)
    row_map = {row.code: row for row in snapshot.rows}
    row = row_map.get(row_code)
    if row is None:
        raise ValueError(f"Unsupported row_code: {row_code}")

    selected_months = _resolve_period_months(snapshot.months, period_kind, period_key)
    currency = "EUR" if currency_mode == "eur" else snapshot.base_currency
    expected_amount = _cell_amount(row=row, all_months=snapshot.months, selected_months=selected_months, currency_mode=currency_mode)

    items: tuple[PygCellDetailItem, ...]
    supported = True
    message = ""
    if company == "sl":
        items = _build_sl_items(row_code=row_code, selected_months=selected_months, database_url=database_url)
    elif company == "ltd":
        items = _build_simple_company_items(
            company="ltd",
            row_code=row_code,
            selected_months=selected_months,
            database_url=database_url,
            reporting_currency=LTD_REPORTING_CURRENCY,
            collect_bundle=collect_pyg_ltd_data,
            expense_mapper=map_ltd_expense_subcategory,
            sales_markets=LTD_DEFAULT_SALES_MARKETS,
            manufacturing_lines=LTD_DEFAULT_MANUFACTURING_LINES,
            logistics_lines=LTD_DEFAULT_LOGISTICS_LINES,
            payment_fee_lines=LTD_DEFAULT_PAYMENT_FEE_LINES,
            shared_service_lines=LTD_DEFAULT_SHARED_SERVICE_LINES,
            administration_lines=LTD_DEFAULT_ADMIN_LINES,
            technology_lines=LTD_DEFAULT_TECH_LINES,
        )
    elif company == "inc":
        items = _build_simple_company_items(
            company="inc",
            row_code=row_code,
            selected_months=selected_months,
            database_url=database_url,
            reporting_currency=INC_REPORTING_CURRENCY,
            collect_bundle=collect_pyg_inc_data,
            expense_mapper=map_inc_expense_subcategory,
            sales_markets=INC_DEFAULT_SALES_MARKETS,
            manufacturing_lines=INC_DEFAULT_MANUFACTURING_LINES,
            logistics_lines=INC_DEFAULT_LOGISTICS_LINES,
            payment_fee_lines=INC_DEFAULT_PAYMENT_FEE_LINES,
            shared_service_lines=INC_DEFAULT_SHARED_SERVICE_LINES,
            administration_lines=INC_DEFAULT_ADMIN_LINES,
            technology_lines=INC_DEFAULT_TECH_LINES,
        )
    else:
        items = _build_consolidated_items(row_code=row_code, selected_months=selected_months, database_url=database_url)

    if items == ():
        supported = _supports_detail(company=company, row_code=row_code)
        if not supported:
            message = "This P&L line does not expose invoice-level detail."
        else:
            message = "No supporting documents were found for the selected period."

    actual_amount = sum(((item.amount_eur if currency_mode == "eur" else item.amount_base) for item in items), Decimal("0"))
    is_reconciled = _is_close(actual_amount, expected_amount) if supported else False
    if supported and items and not is_reconciled:
        message = f"Detail sum mismatch: expected {expected_amount} but got {actual_amount}."

    return PygCellDetail(
        company=company,
        row_code=row_code,
        row_label=row.label,
        period_kind=period_kind,
        period_key=period_key,
        currency=currency,
        cell_amount=expected_amount,
        supported=supported,
        is_reconciled=is_reconciled,
        message=message,
        items=items,
    )


def _build_sl_items(*, row_code: str, selected_months: tuple[str, ...], database_url: str) -> tuple[PygCellDetailItem, ...]:
    bundles = [collect_pyg_sl_data(year=year, database_url=database_url) for year in sorted({int(month[:4]) for month in selected_months})]
    fx = EcbFxService()
    items: list[PygCellDetailItem] = []

    for bundle in bundles:
        for row in bundle.service_rows:
            if row.yyyymm not in selected_months:
                continue
            if row_code == "services":
                items.append(_stage_item(company="sl", row=row, reporting_currency=SL_REPORTING_CURRENCY, fx=fx))
            elif row_code.startswith("service_") and row.line_item.lower() == row_code.removeprefix("service_"):
                items.append(_stage_item(company="sl", row=row, reporting_currency=SL_REPORTING_CURRENCY, fx=fx))

        for row in bundle.expense_rows:
            if row.yyyymm not in selected_months:
                continue
            if _matches_sl_expense_row(row_code=row_code, supplier_code=row.supplier_code, detail=row.detail, subcategory=row.subcategory):
                items.append(_expense_item(company="sl", row=row, reporting_currency=SL_REPORTING_CURRENCY, fx=fx))

        for row in bundle.payment_fee_rows:
            if row.yyyymm not in selected_months:
                continue
            if row_code == "payment_fees" or row_code == f"payment_fee_{row.supplier_code.lower()}":
                items.append(_payment_fee_item(company="sl", row=row, reporting_currency=SL_REPORTING_CURRENCY, fx=fx))

        if row_code in {"diferencias_divisas_group", "diferencias_divisas"}:
            for month in selected_months:
                amount = bundle.diferencias_divisas_by_period.get(month)
                if amount:
                    items.append(_synthetic_item(company="sl", period_yyyymm=month, label="Currency Adjustment", amount_base=amount, amount_eur=amount, currency="EUR", source="diferencias_divisas"))

        if row_code in {"royalties_eu", "royalties_uk", "royalties_us"}:
            scope = row_code.removeprefix("royalties_")
            for month in selected_months:
                amount = bundle.royalties_by_scope.get(scope, {}).get(month)
                if amount:
                    items.append(_synthetic_item(company="sl", period_yyyymm=month, label=scope.upper(), amount_base=amount, amount_eur=amount, currency="EUR", source="artist_royalties_monthly_summary"))

    return tuple(items)


def _build_simple_company_items(
    *,
    company: Literal["ltd", "inc"],
    row_code: str,
    selected_months: tuple[str, ...],
    database_url: str,
    reporting_currency: str,
    collect_bundle,
    expense_mapper,
    sales_markets: tuple[str, ...],
    manufacturing_lines: tuple[str, ...],
    logistics_lines: tuple[str, ...],
    payment_fee_lines: tuple[str, ...],
    shared_service_lines: tuple[str, ...],
    administration_lines: tuple[str, ...],
    technology_lines: tuple[str, ...],
) -> tuple[PygCellDetailItem, ...]:
    del sales_markets, manufacturing_lines, logistics_lines, payment_fee_lines, shared_service_lines, administration_lines, technology_lines
    bundles = [collect_bundle(year=year, database_url=database_url) for year in sorted({int(month[:4]) for month in selected_months})]
    fx = EcbFxService()
    items: list[PygCellDetailItem] = []

    for bundle in bundles:
        supplier_map = {row.supplier_code: row for row in bundle.provider_catalog_rows}
        for row in bundle.expense_rows:
            if row.yyyymm not in selected_months:
                continue
            supplier_meta = supplier_map.get(row.supplier_code)
            subcategory = expense_mapper(
                supplier_code=row.supplier_code,
                division_invoice=row.detail or "",
                supplier_meta=supplier_meta.__dict__ if supplier_meta else None,
            ) or row.subcategory
            if _matches_simple_expense_row(row_code=row_code, supplier_code=row.supplier_code, detail=row.detail, subcategory=subcategory):
                items.append(_expense_item(company=company, row=row, reporting_currency=reporting_currency, fx=fx))

        for row in bundle.payment_fee_rows:
            if row.yyyymm not in selected_months:
                continue
            if row_code == "payment_fees" or row_code == f"payment_fee_{row.supplier_code.lower()}":
                items.append(_payment_fee_item(company=company, row=row, reporting_currency=reporting_currency, fx=fx))

        if row_code == "marcos_consumed":
            for month in selected_months:
                amount_base = bundle.frame_consumed_by_period.get(month)
                if amount_base:
                    amount_eur = fx.convert(
                        amount=amount_base,
                        source_currency=reporting_currency,
                        reporting_currency="EUR",
                        yyyymm=month,
                    )[0].amount_reporting
                    items.append(_synthetic_item(company=company, period_yyyymm=month, label="Frame consumption", amount_base=amount_base, amount_eur=amount_eur, currency=reporting_currency, source="frame_stock"))

        if row_code in {"diferencias_divisas_group", "diferencias_divisas"}:
            for month in selected_months:
                amount = bundle.diferencias_divisas_by_period.get(month)
                if amount:
                    items.append(_synthetic_item(company=company, period_yyyymm=month, label="Currency Adjustment", amount_base=amount, amount_eur=amount, currency="EUR", source="diferencias_divisas"))

    return tuple(items)


def _build_consolidated_items(*, row_code: str, selected_months: tuple[str, ...], database_url: str) -> tuple[PygCellDetailItem, ...]:
    items: list[PygCellDetailItem] = []

    if row_code == "services":
        fx = EcbFxService()
        bundles = [collect_pyg_sl_data(year=year, database_url=database_url) for year in sorted({int(month[:4]) for month in selected_months})]
        for bundle in bundles:
            for row in bundle.service_rows:
                if row.yyyymm not in selected_months:
                    continue
                if row.line_item in {"Ltd", "Inc"} or row.detail == "renting_cnc":
                    continue
                items.append(_stage_item(company="sl", row=row, reporting_currency="EUR", fx=fx))
        return tuple(items)

    direct_mapped_codes = {"logistics", "payment_fees", "technology", "otros_gastos_group", "diferencias_divisas_group"}
    if row_code in direct_mapped_codes:
        items.extend(_build_sl_items(row_code=row_code, selected_months=selected_months, database_url=database_url))
        items.extend(_build_simple_company_items(company="ltd", row_code=row_code, selected_months=selected_months, database_url=database_url, reporting_currency=LTD_REPORTING_CURRENCY, collect_bundle=collect_pyg_ltd_data, expense_mapper=map_ltd_expense_subcategory, sales_markets=LTD_DEFAULT_SALES_MARKETS, manufacturing_lines=LTD_DEFAULT_MANUFACTURING_LINES, logistics_lines=LTD_DEFAULT_LOGISTICS_LINES, payment_fee_lines=LTD_DEFAULT_PAYMENT_FEE_LINES, shared_service_lines=LTD_DEFAULT_SHARED_SERVICE_LINES, administration_lines=LTD_DEFAULT_ADMIN_LINES, technology_lines=LTD_DEFAULT_TECH_LINES))
        items.extend(_build_simple_company_items(company="inc", row_code=row_code, selected_months=selected_months, database_url=database_url, reporting_currency=INC_REPORTING_CURRENCY, collect_bundle=collect_pyg_inc_data, expense_mapper=map_inc_expense_subcategory, sales_markets=INC_DEFAULT_SALES_MARKETS, manufacturing_lines=INC_DEFAULT_MANUFACTURING_LINES, logistics_lines=INC_DEFAULT_LOGISTICS_LINES, payment_fee_lines=INC_DEFAULT_PAYMENT_FEE_LINES, shared_service_lines=INC_DEFAULT_SHARED_SERVICE_LINES, administration_lines=INC_DEFAULT_ADMIN_LINES, technology_lines=INC_DEFAULT_TECH_LINES))
        return tuple(items)

    if row_code == "manufacturing":
        items.extend(item for item in _build_sl_items(row_code="manufacturing", selected_months=selected_months, database_url=database_url) if item.label.upper() != "BBVACNC")
        items.extend(_build_simple_company_items(company="ltd", row_code="manufacturing", selected_months=selected_months, database_url=database_url, reporting_currency=LTD_REPORTING_CURRENCY, collect_bundle=collect_pyg_ltd_data, expense_mapper=map_ltd_expense_subcategory, sales_markets=LTD_DEFAULT_SALES_MARKETS, manufacturing_lines=LTD_DEFAULT_MANUFACTURING_LINES, logistics_lines=LTD_DEFAULT_LOGISTICS_LINES, payment_fee_lines=LTD_DEFAULT_PAYMENT_FEE_LINES, shared_service_lines=LTD_DEFAULT_SHARED_SERVICE_LINES, administration_lines=LTD_DEFAULT_ADMIN_LINES, technology_lines=LTD_DEFAULT_TECH_LINES))
        items.extend(_build_simple_company_items(company="inc", row_code="manufacturing", selected_months=selected_months, database_url=database_url, reporting_currency=INC_REPORTING_CURRENCY, collect_bundle=collect_pyg_inc_data, expense_mapper=map_inc_expense_subcategory, sales_markets=INC_DEFAULT_SALES_MARKETS, manufacturing_lines=INC_DEFAULT_MANUFACTURING_LINES, logistics_lines=INC_DEFAULT_LOGISTICS_LINES, payment_fee_lines=INC_DEFAULT_PAYMENT_FEE_LINES, shared_service_lines=INC_DEFAULT_SHARED_SERVICE_LINES, administration_lines=INC_DEFAULT_ADMIN_LINES, technology_lines=INC_DEFAULT_TECH_LINES))
        return tuple(items)

    if row_code == "administration":
        items.extend(item for item in _build_sl_items(row_code="administration", selected_months=selected_months, database_url=database_url) if item.label.upper() != "BBVACNC")
        items.extend(_build_simple_company_items(company="ltd", row_code="administration", selected_months=selected_months, database_url=database_url, reporting_currency=LTD_REPORTING_CURRENCY, collect_bundle=collect_pyg_ltd_data, expense_mapper=map_ltd_expense_subcategory, sales_markets=LTD_DEFAULT_SALES_MARKETS, manufacturing_lines=LTD_DEFAULT_MANUFACTURING_LINES, logistics_lines=LTD_DEFAULT_LOGISTICS_LINES, payment_fee_lines=LTD_DEFAULT_PAYMENT_FEE_LINES, shared_service_lines=LTD_DEFAULT_SHARED_SERVICE_LINES, administration_lines=LTD_DEFAULT_ADMIN_LINES, technology_lines=LTD_DEFAULT_TECH_LINES))
        items.extend(_build_simple_company_items(company="inc", row_code="administration", selected_months=selected_months, database_url=database_url, reporting_currency=INC_REPORTING_CURRENCY, collect_bundle=collect_pyg_inc_data, expense_mapper=map_inc_expense_subcategory, sales_markets=INC_DEFAULT_SALES_MARKETS, manufacturing_lines=INC_DEFAULT_MANUFACTURING_LINES, logistics_lines=INC_DEFAULT_LOGISTICS_LINES, payment_fee_lines=INC_DEFAULT_PAYMENT_FEE_LINES, shared_service_lines=INC_DEFAULT_SHARED_SERVICE_LINES, administration_lines=INC_DEFAULT_ADMIN_LINES, technology_lines=INC_DEFAULT_TECH_LINES))
        return tuple(items)

    if row_code in {"marketing", "staff"}:
        return _build_sl_items(row_code=row_code, selected_months=selected_months, database_url=database_url)

    if row_code == "royalties":
        return _build_sl_items(row_code="royalties", selected_months=selected_months, database_url=database_url)

    return ()


def _matches_sl_expense_row(*, row_code: str, supplier_code: str, detail: str, subcategory: str) -> bool:
    supplier_key = supplier_code.lower()
    detail_key = _normalize_detail(detail)
    if row_code in {"manufacturing", "logistics", "royalties", "payment_fees", "marketing", "staff", "administration", "technology", "otros_gastos_group", "otros_gastos", "diferencias_divisas_group", "diferencias_divisas"}:
        if row_code == "otros_gastos_group":
            return subcategory == "otros_gastos"
        if row_code == "diferencias_divisas_group":
            return False
        return subcategory == row_code or (row_code == "royalties" and subcategory == "royalties")
    if row_code.startswith("manufacturing_"):
        return subcategory == "manufacturing" and supplier_key == row_code.removeprefix("manufacturing_")
    if row_code.startswith("logistics_"):
        return subcategory == "logistics" and supplier_key == row_code.removeprefix("logistics_")
    if row_code.startswith("staff_"):
        return subcategory == "staff" and supplier_key == row_code.removeprefix("staff_")
    if row_code.startswith("technology_"):
        return subcategory == "technology" and supplier_key == row_code.removeprefix("technology_")
    if row_code.startswith("payment_fee_"):
        return False
    if row_code.startswith("administration_"):
        suffix = row_code.removeprefix("administration_")
        if "_" in suffix:
            supplier_part, detail_part = suffix.split("_", 1)
            return subcategory == "administration" and supplier_key == supplier_part and detail_key == detail_part
        return subcategory == "administration" and supplier_key == suffix
    if row_code.startswith("marketing_metaads_"):
        return subcategory == "marketing" and supplier_key == "metaads" and detail_key == row_code.removeprefix("marketing_metaads_")
    if row_code.startswith("marketing_googleads_"):
        return subcategory == "marketing" and supplier_key == "googleads" and detail_key == row_code.removeprefix("marketing_googleads_")
    if row_code == "marketing_metaads":
        return subcategory == "marketing" and supplier_key == "metaads"
    if row_code == "marketing_googleads":
        return subcategory == "marketing" and supplier_key == "googleads"
    if row_code in {"royalties_total"}:
        return subcategory == "royalties"
    return False


def _matches_simple_expense_row(*, row_code: str, supplier_code: str, detail: str, subcategory: str) -> bool:
    supplier_key = supplier_code.lower()
    del detail
    if row_code in {"manufacturing", "logistics", "shared_services", "administration", "technology", "otros_gastos_group", "otros_gastos", "diferencias_divisas_group", "diferencias_divisas"}:
        if row_code == "otros_gastos_group":
            return subcategory == "otros_gastos"
        if row_code == "diferencias_divisas_group":
            return False
        return subcategory == row_code
    for prefix in ("manufacturing_", "logistics_", "shared_services_", "administration_", "technology_"):
        if row_code.startswith(prefix):
            return subcategory == prefix.removesuffix("_") and supplier_key == row_code.removeprefix(prefix)
    return False


def _supports_detail(*, company: PygCompany, row_code: str) -> bool:
    if company == "consolidado":
        return row_code in {"services", "manufacturing", "logistics", "royalties", "payment_fees", "marketing", "staff", "administration", "technology", "otros_gastos_group", "diferencias_divisas_group"}
    if company == "sl":
        return (
            row_code == "services"
            or row_code.startswith("service_")
            or row_code in {"manufacturing", "logistics", "royalties", "royalties_total", "payment_fees", "marketing", "marketing_metaads", "marketing_googleads", "staff", "administration", "technology", "otros_gastos_group", "otros_gastos", "diferencias_divisas_group", "diferencias_divisas"}
            or row_code.startswith(("manufacturing_", "logistics_", "payment_fee_", "marketing_metaads_", "marketing_googleads_", "staff_", "administration_", "technology_"))
        )
    return (
        row_code in {"manufacturing", "logistics", "payment_fees", "shared_services", "administration", "technology", "otros_gastos_group", "otros_gastos", "diferencias_divisas_group", "diferencias_divisas", "marcos_consumed"}
        or row_code.startswith(("manufacturing_", "logistics_", "payment_fee_", "shared_services_", "administration_", "technology_"))
    )


def _resolve_period_months(all_months: tuple[str, ...], period_kind: PygPeriodKind, period_key: str) -> tuple[str, ...]:
    if period_kind == "month":
        if period_key not in all_months:
            raise ValueError(f"Unsupported month period: {period_key}")
        return (period_key,)
    if period_kind == "quarter":
        if not period_key.startswith("Q"):
            raise ValueError(f"Unsupported quarter period: {period_key}")
        quarter_index = int(period_key.removeprefix("Q")) - 1
        start = quarter_index * 3
        selected = all_months[start : start + 3]
        if not selected:
            raise ValueError(f"Unsupported quarter period: {period_key}")
        return tuple(selected)
    if period_kind == "total":
        return tuple(all_months)
    raise ValueError(f"Unsupported period_kind: {period_kind}")


def _cell_amount(*, row: PygSnapshotRow, all_months: tuple[str, ...], selected_months: tuple[str, ...], currency_mode: PygCurrencyMode) -> Decimal:
    values = row.values_eur if currency_mode == "eur" else row.values_base
    index_map = {month: idx for idx, month in enumerate(all_months)}
    return sum((Decimal(str(values[index_map[month]])) for month in selected_months), Decimal("0"))


def _stage_item(*, company: str, row, reporting_currency: str, fx: EcbFxService) -> PygCellDetailItem:
    amount_base = _convert(fx=fx, amount=row.amount_net, source_currency=row.currency, target_currency=reporting_currency, yyyymm=row.yyyymm)
    amount_eur = _convert(fx=fx, amount=row.amount_net, source_currency=row.currency, target_currency="EUR", yyyymm=row.yyyymm)
    return PygCellDetailItem(
        company=company.upper(),
        period_yyyymm=row.yyyymm,
        label=row.line_item,
        invoice_number=getattr(row, "invoice_number", "") or "",
        amount_local=row.amount_net,
        amount_base=amount_base,
        amount_eur=amount_eur,
        currency=row.currency,
        source=row.source,
        drive_url=getattr(row, "drive_url", "") or "",
    )


def _expense_item(*, company: str, row, reporting_currency: str, fx: EcbFxService) -> PygCellDetailItem:
    amount_base = _convert(fx=fx, amount=row.amount_net, source_currency=row.currency, target_currency=reporting_currency, yyyymm=row.yyyymm)
    amount_eur = _convert(fx=fx, amount=row.amount_net, source_currency=row.currency, target_currency="EUR", yyyymm=row.yyyymm)
    detail = _normalize_detail(row.detail)
    label = row.supplier_code if not detail else f"{row.supplier_code} / {detail.replace('_', ' ')}"
    return PygCellDetailItem(
        company=company.upper(),
        period_yyyymm=row.yyyymm,
        label=label,
        invoice_number=row.invoice_number or "",
        amount_local=row.amount_net,
        amount_base=amount_base,
        amount_eur=amount_eur,
        currency=row.currency,
        source=row.source,
        drive_url=row.drive_url or "",
    )


def _payment_fee_item(*, company: str, row, reporting_currency: str, fx: EcbFxService) -> PygCellDetailItem:
    amount_base = _convert(fx=fx, amount=row.amount_net, source_currency=row.currency, target_currency=reporting_currency, yyyymm=row.yyyymm)
    amount_eur = _convert(fx=fx, amount=row.amount_net, source_currency=row.currency, target_currency="EUR", yyyymm=row.yyyymm)
    return PygCellDetailItem(
        company=company.upper(),
        period_yyyymm=row.yyyymm,
        label=row.supplier_code,
        invoice_number="",
        amount_local=row.amount_net,
        amount_base=amount_base,
        amount_eur=amount_eur,
        currency=row.currency,
        source=row.source,
        drive_url="",
    )


def _synthetic_item(*, company: str, period_yyyymm: str, label: str, amount_base: Decimal, amount_eur: Decimal, currency: str, source: str) -> PygCellDetailItem:
    return PygCellDetailItem(
        company=company.upper(),
        period_yyyymm=period_yyyymm,
        label=label,
        invoice_number="",
        amount_local=amount_base,
        amount_base=amount_base,
        amount_eur=amount_eur,
        currency=currency,
        source=source,
        drive_url="",
    )


def _convert(*, fx: EcbFxService, amount: Decimal, source_currency: str, target_currency: str, yyyymm: str) -> Decimal:
    return fx.convert(amount=amount, source_currency=source_currency, reporting_currency=target_currency, yyyymm=yyyymm)[0].amount_reporting


def _normalize_detail(value: str) -> str:
    return (value or "").strip().lower().replace(" ", "_")


def _is_close(left: Decimal, right: Decimal) -> bool:
    return abs(left - right) <= Decimal("0.01")
