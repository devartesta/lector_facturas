from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from unittest.mock import patch

from lector_facturas.pyg_cell_detail import PygCellDetailItem, build_pyg_cell_detail
from lector_facturas.pyg_inc_workbook import PygIncDataBundle
from lector_facturas.pyg_ltd_workbook import PygLtdDataBundle
from lector_facturas.pyg_sl_workbook import PygSlDataBundle, StageRow
from lector_facturas.pyg_snapshot import PygSnapshot, PygSnapshotRow


def _snapshot_row(code: str, values: tuple[str, ...], *, label: str | None = None) -> PygSnapshotRow:
    decimals = tuple(Decimal(value) for value in values)
    return PygSnapshotRow(
        code=code,
        label=label or code,
        level=0,
        kind="detail",
        parent_code=None,
        style_key="detail",
        default_expanded=False,
        values_base=decimals,
        values_eur=decimals,
    )


def _snapshot(company: str, rows: tuple[PygSnapshotRow, ...], months: tuple[str, ...] = ("202601",)) -> PygSnapshot:
    return PygSnapshot(
        company=company,  # type: ignore[arg-type]
        base_currency="EUR",
        months=months,
        generated_at=datetime(2026, 4, 4, 12, 0, 0),
        drive_file_name="pyg.xlsx",
        drive_file_url="",
        fx_mode="monthly_historical",
        rows=rows,
    )


def test_sl_services_detail_keeps_all_service_rows() -> None:
    snapshot = _snapshot("sl", (_snapshot_row("services", ("14.00",), label="Services"),))
    bundle = PygSlDataBundle(
        year=2026,
        generated_at=datetime(2026, 4, 4, 12, 0, 0),
        shopify_rows=(),
        marketplace_rows=(),
        rappel_rows=(),
        supplies_rows=(),
        service_rows=(
            StageRow("202601", "SL", "HANNUN", "services", Decimal("4.00"), "EUR", "documents", "INV-1", "https://example.com/1"),
            StageRow("202601", "SL", "Ltd", "services", Decimal("10.00"), "EUR", "documents", "INV-2", "https://example.com/2"),
        ),
        expense_rows=(),
        payment_fee_rows=(),
        provider_catalog_rows=(),
        shopify_markets=("ES",),
    )

    with (
        patch("lector_facturas.pyg_cell_detail.build_pyg_snapshot", return_value=snapshot),
        patch("lector_facturas.pyg_cell_detail.collect_pyg_sl_data", return_value=bundle),
    ):
        detail = build_pyg_cell_detail(
            company="sl",
            row_code="services",
            period_kind="month",
            period_key="202601",
            currency_mode="eur",
            months=["202601"],
            database_url="postgres://ignored",
        )

    assert detail.supported is True
    assert detail.is_reconciled is True
    assert [item.label for item in detail.items] == ["HANNUN", "Ltd"]
    assert [item.amount_local for item in detail.items] == [Decimal("4.00"), Decimal("10.00")]
    assert detail.cell_amount == Decimal("14.00")


def test_consolidated_services_detail_excludes_internal_and_renting() -> None:
    snapshot = _snapshot("consolidado", (_snapshot_row("services", ("4.00",), label="Services"),))
    bundle = PygSlDataBundle(
        year=2026,
        generated_at=datetime(2026, 4, 4, 12, 0, 0),
        shopify_rows=(),
        marketplace_rows=(),
        rappel_rows=(),
        supplies_rows=(),
        service_rows=(
            StageRow("202601", "SL", "HANNUN", "services", Decimal("4.00"), "EUR", "documents", "INV-1", "https://example.com/1"),
            StageRow("202601", "SL", "Ltd", "services", Decimal("8.00"), "EUR", "documents", "INV-2", "https://example.com/2"),
            StageRow("202601", "SL", "QHANDS", "renting_cnc", Decimal("5.00"), "EUR", "documents", "INV-3", "https://example.com/3"),
        ),
        expense_rows=(),
        payment_fee_rows=(),
        provider_catalog_rows=(),
        shopify_markets=("ES",),
    )

    with (
        patch("lector_facturas.pyg_cell_detail.build_pyg_snapshot", return_value=snapshot),
        patch("lector_facturas.pyg_cell_detail.collect_pyg_sl_data", return_value=bundle),
    ):
        detail = build_pyg_cell_detail(
            company="consolidado",
            row_code="services",
            period_kind="month",
            period_key="202601",
            currency_mode="eur",
            months=["202601"],
            database_url="postgres://ignored",
        )

    assert detail.supported is True
    assert detail.is_reconciled is True
    assert len(detail.items) == 1
    assert detail.items[0].label == "HANNUN"
    assert detail.cell_amount == Decimal("4.00")


def test_ltd_manufacturing_quarter_detail_includes_frame_consumption() -> None:
    snapshot = _snapshot(
        "ltd",
        (_snapshot_row("marcos_consumed", ("6.00", "8.00", "0.00"), label="Frame consumption"),),
        months=("202601", "202602", "202603"),
    )
    bundle = PygLtdDataBundle(
        year=2026,
        generated_at=datetime(2026, 4, 4, 12, 0, 0),
        sales_rows=(),
        expense_rows=(),
        payment_fee_rows=(),
        provider_catalog_rows=(),
        frame_consumed_by_period={"202601": Decimal("6.00"), "202602": Decimal("8.00")},
    )

    with (
        patch("lector_facturas.pyg_cell_detail.build_pyg_snapshot", return_value=snapshot),
        patch("lector_facturas.pyg_cell_detail.collect_pyg_ltd_data", return_value=bundle),
    ):
        detail = build_pyg_cell_detail(
            company="ltd",
            row_code="marcos_consumed",
            period_kind="quarter",
            period_key="Q1",
            currency_mode="base",
            months=["202601", "202602", "202603"],
            database_url="postgres://ignored",
        )

    assert detail.supported is True
    assert detail.is_reconciled is True
    assert len(detail.items) == 2


def test_consolidated_administration_excludes_bbvacnc_items() -> None:
    snapshot = _snapshot("consolidado", (_snapshot_row("administration", ("8.00",), label="Administration"),))
    bbva = PygCellDetailItem("SL", "202601", "BBVACNC", "INV-1", Decimal("5.00"), Decimal("5.00"), Decimal("5.00"), "EUR", "documents", "")
    claris = PygCellDetailItem("SL", "202601", "CLARIS", "INV-2", Decimal("3.00"), Decimal("3.00"), Decimal("3.00"), "EUR", "documents", "")
    ltd = PygCellDetailItem("LTD", "202601", "YOURACCOUNTSTAXES", "INV-3", Decimal("2.00"), Decimal("2.00"), Decimal("2.00"), "GBP", "documents", "")
    inc = PygCellDetailItem("INC", "202601", "CONTINUUM", "INV-4", Decimal("3.00"), Decimal("3.00"), Decimal("3.00"), "USD", "documents", "")

    with (
        patch("lector_facturas.pyg_cell_detail.build_pyg_snapshot", return_value=snapshot),
        patch("lector_facturas.pyg_cell_detail._build_sl_items", return_value=(bbva, claris)),
        patch("lector_facturas.pyg_cell_detail._build_simple_company_items", side_effect=[(ltd,), (inc,)]),
    ):
        detail = build_pyg_cell_detail(
            company="consolidado",
            row_code="administration",
            period_kind="month",
            period_key="202601",
            currency_mode="eur",
            months=["202601"],
            database_url="postgres://ignored",
        )

    assert detail.supported is True
    assert detail.is_reconciled is True
    assert [item.label for item in detail.items] == ["CLARIS", "YOURACCOUNTSTAXES", "CONTINUUM"]
