"""Create frame stock v2 tables and populate initial data.

Actions performed:
  1. Create supply.frame_sku_wac
  2. Create supply.frame_consumption_valued
  3. Create supply.frame_stock_monthly
  4. Fix naming: UPDATE frame_size '100x70' -> '70x100' in frame_purchase_lines
  5. Populate frame_sku_wac from all existing purchases (TGI + future Proco)
  6. Refresh frame_consumption_valued + frame_stock_monthly for all months
     that have consumption data in consumo_marcos_diario

Usage:
    python scripts/migrate_frame_stock_v2.py
    python scripts/migrate_frame_stock_v2.py --database-url postgresql://...
"""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:
    print("psycopg not installed. Run: pip install psycopg[binary]")
    sys.exit(1)


DDL_FRAME_SKU_WAC = """
CREATE TABLE IF NOT EXISTS supply.frame_sku_wac (
    id              SERIAL PRIMARY KEY,
    fabricante      TEXT        NOT NULL,
    frame_color     TEXT        NOT NULL,
    frame_size      TEXT        NOT NULL,
    effective_from  DATE        NOT NULL,
    purchase_id     INTEGER     NOT NULL REFERENCES supply.frame_purchases(id),
    wac             NUMERIC(14, 6) NOT NULL,
    units_on_hand   INTEGER     NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (fabricante, frame_color, frame_size, effective_from)
);
CREATE INDEX IF NOT EXISTS frame_sku_wac_lookup_idx
    ON supply.frame_sku_wac (fabricante, frame_color, frame_size, effective_from);
"""

DDL_FRAME_CONSUMPTION_VALUED = """
CREATE TABLE IF NOT EXISTS supply.frame_consumption_valued (
    fabricante          TEXT        NOT NULL,
    mes_yyyymm          TEXT        NOT NULL,
    frame_color         TEXT        NOT NULL,
    frame_size          TEXT        NOT NULL,

    -- From consumo_marcos_diario (refreshed daily, never manually edited)
    quantity_system     INTEGER     NOT NULL DEFAULT 0,

    -- Manual override (e.g. physical stock count adjustment)
    quantity_override   INTEGER,
    override_notes      TEXT,
    override_set_at     TIMESTAMPTZ,

    -- Effective quantity used for accounting (= override if set, else system)
    quantity_effective  INTEGER     NOT NULL DEFAULT 0,

    -- WAC at start of month (reference; amounts may differ if intra-month purchase)
    unit_wac_opening    NUMERIC(14, 6) NOT NULL DEFAULT 0,
    wac_calculated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Valued amounts (computed by refresh job)
    amount_system       NUMERIC(14, 2) NOT NULL DEFAULT 0,
    amount_effective    NUMERIC(14, 2) NOT NULL DEFAULT 0,

    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (fabricante, mes_yyyymm, frame_color, frame_size)
);
CREATE INDEX IF NOT EXISTS frame_consumption_valued_month_idx
    ON supply.frame_consumption_valued (fabricante, mes_yyyymm);
"""

DDL_FRAME_STOCK_MONTHLY = """
CREATE TABLE IF NOT EXISTS supply.frame_stock_monthly (
    fabricante          TEXT        NOT NULL,
    mes_yyyymm          TEXT        NOT NULL,
    currency            TEXT        NOT NULL,

    opening_units       INTEGER     NOT NULL,
    opening_value       NUMERIC(14, 2) NOT NULL,

    purchased_units     INTEGER     NOT NULL DEFAULT 0,
    purchased_value     NUMERIC(14, 2) NOT NULL DEFAULT 0,

    consumed_units      INTEGER     NOT NULL,
    consumed_value      NUMERIC(14, 2) NOT NULL,

    closing_units       INTEGER     NOT NULL,
    closing_value       NUMERIC(14, 2) NOT NULL,

    calculated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (fabricante, mes_yyyymm)
);
"""


def _read_database_url(repo_root: Path) -> str | None:
    env_path = repo_root / ".env.local"
    if not env_path.exists():
        return None
    for raw_line in env_path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if line.startswith("DATABASE_URL="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate frame stock tables to v2.")
    parser.add_argument("--database-url", default=None)
    args = parser.parse_args()

    database_url = args.database_url or _read_database_url(REPO_ROOT)
    if not database_url:
        print("No DATABASE_URL found. Pass --database-url or set it in .env.local")
        sys.exit(1)

    from lector_facturas.supply_stock import (
        populate_sku_wac_for_purchase,
        refresh_frame_consumption_month,
    )

    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        # 1-3. Create new tables
        print("Creating supply.frame_sku_wac...")
        conn.execute(DDL_FRAME_SKU_WAC)

        print("Creating supply.frame_consumption_valued...")
        conn.execute(DDL_FRAME_CONSUMPTION_VALUED)

        print("Creating supply.frame_stock_monthly...")
        conn.execute(DDL_FRAME_STOCK_MONTHLY)

        # 4. Fix naming: 100x70 → 70x100 in frame_purchase_lines
        updated = conn.execute(
            "UPDATE supply.frame_purchase_lines SET frame_size = '70x100' WHERE frame_size = '100x70'"
        ).rowcount
        print(f"Fixed naming: {updated} rows updated (100x70 -> 70x100).")

        # 5. Populate frame_sku_wac for all existing purchases
        purchases = conn.execute(
            "SELECT id, fabricante, purchase_date FROM supply.frame_purchases ORDER BY purchase_date, id"
        ).fetchall()

        all_months_to_refresh: set[str] = set()
        for purchase in purchases:
            print(f"  Building WAC history for purchase #{purchase['id']} "
                  f"({purchase['fabricante']}, {purchase['purchase_date']})...")
            months = populate_sku_wac_for_purchase(int(purchase["id"]), conn)
            all_months_to_refresh.update(months)

        conn.commit()
        print(f"WAC history built. Months to refresh: {sorted(all_months_to_refresh)}")

        # 6. Find all months with actual consumption data (for any fabricante)
        consumption_months = conn.execute(
            """
            SELECT DISTINCT fabricante, mes_yyyymm
            FROM supply.consumo_marcos_diario
            WHERE fabricante IN ('TGI', 'Proco')
            ORDER BY fabricante, mes_yyyymm
            """
        ).fetchall()

        # Also include any months from purchases that go beyond consumption data
        for row in consumption_months:
            all_months_to_refresh.add(row["mes_yyyymm"])

        # Group months by fabricante
        fabricantes_months: dict[str, set[str]] = {}
        for row in consumption_months:
            fab = row["fabricante"]
            fabricantes_months.setdefault(fab, set()).add(row["mes_yyyymm"])

        # Also ensure purchase months are included
        for purchase in purchases:
            fab = purchase["fabricante"]
            yyyymm = str(purchase["purchase_date"])[:7].replace("-", "")
            fabricantes_months.setdefault(fab, set()).add(yyyymm)

        # 7. Refresh consumption_valued + stock_monthly for each fabricante × month
        for fabricante, months in sorted(fabricantes_months.items()):
            print(f"\nRefreshing {fabricante} ({len(months)} months)...")
            for mes_yyyymm in sorted(months):
                print(f"  Refreshing {fabricante} / {mes_yyyymm}...", end=" ", flush=True)
                refresh_frame_consumption_month(fabricante, mes_yyyymm, conn)
                conn.commit()
                print("done")

        print("\nMigration complete.")


if __name__ == "__main__":
    main()
