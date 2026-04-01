from __future__ import annotations

from datetime import datetime
from pathlib import Path

from openpyxl import load_workbook

from lector_facturas.pyg_sl_workbook import PygSlDataBundle, ProviderCatalogRow, build_pyg_sl_workbook


def test_build_pyg_sl_workbook_creates_expected_sheets_and_formulas(tmp_path: Path) -> None:
    bundle = PygSlDataBundle(
        year=2026,
        generated_at=datetime(2026, 3, 23, 10, 0, 0),
        shopify_rows=(),
        marketplace_rows=(),
        rappel_rows=(),
        supplies_rows=(),
        service_rows=(),
        expense_rows=(),
        payment_fee_rows=(),
        provider_catalog_rows=(
            ProviderCatalogRow("APPHOTOES", "APPHOTOES", "AP Photo", "expenses/cogs/manufacturing", ""),
            ProviderCatalogRow("GLS", "GLS", "GLS", "expenses/cogs/logistics", ""),
            ProviderCatalogRow("CLARIS", "CLARIS", "CLARIS", "expenses/opex/administration", ""),
            ProviderCatalogRow("ADOBE", "ADOBE", "ADOBE", "expenses/opex/technology", ""),
        ),
        shopify_markets=("ES", "FR"),
    )
    output_path = tmp_path / "pyg_sl_2026.xlsx"

    build_pyg_sl_workbook(bundle, output_path)

    workbook = load_workbook(output_path, data_only=False)
    assert workbook.sheetnames[0] == "P&G-SL"
    assert "i-marketplaces-sl" in workbook.sheetnames
    assert "g-expenses-sl" in workbook.sheetnames
    assert "fx-rates" in workbook.sheetnames
    ws = workbook["P&G-SL"]
    assert ws["D1"].value == "202601"
    assert ws["D2"].value == "Enero"
    assert ws["P2"].value == "Total"
    assert ws["A4"].value == "Turnover"
    assert ws["C7"].value == "ES"
    assert ws["D6"].value == "=SUM(D7:D8)"
    assert ws["C8"].value == "FR"
    assert ws["C9"].value == "Marketplaces"
    assert ws["D10"].value == "=SUMIFS('i-marketplaces-sl'!$I:$I,'i-marketplaces-sl'!$A:$A,D$1,'i-marketplaces-sl'!$C:$C,$C10)"
