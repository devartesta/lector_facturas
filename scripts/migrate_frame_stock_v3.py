"""
migrate_frame_stock_v3.py
-------------------------
Migrates frame_consumption_valued from monthly granularity to daily granularity,
and extracts overrides into a separate frame_consumption_override table.

Steps:
  1. Save existing overrides from old frame_consumption_valued
  2. Drop old table, create new daily frame_consumption_valued
  3. Create frame_consumption_override table
  4. Re-run refresh for all months with data
  5. Re-apply saved overrides via set_frame_consumption_override
"""
import sys, os, argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import psycopg
from psycopg.rows import dict_row
from lector_facturas.supply_stock import (
    populate_sku_wac_for_purchase,
    refresh_frame_consumption_month,
    set_frame_consumption_override,
)


def get_database_url(args) -> str:
    if args.database_url:
        return args.database_url
    if url := os.environ.get("DATABASE_URL"):
        return url
    env_file = os.path.join(os.path.dirname(__file__), "..", ".env.local")
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                if line.startswith("DATABASE_URL="):
                    return line.split("=", 1)[1].strip()
    raise RuntimeError("No DATABASE_URL found. Pass --database-url or set it in .env.local")


def main():
    parser = argparse.ArgumentParser(description="Migrate frame stock to v3 (daily WAC)")
    parser.add_argument("--database-url", default=None)
    args = parser.parse_args()
    db_url = get_database_url(args)

    with psycopg.connect(db_url, row_factory=dict_row) as conn:

        # ----------------------------------------------------------------
        # 1. Save existing overrides from old frame_consumption_valued
        # ----------------------------------------------------------------
        print("Saving existing overrides...")
        try:
            saved_overrides = conn.execute("""
                SELECT fabricante, mes_yyyymm, frame_color, frame_size,
                       quantity_override, override_notes
                FROM supply.frame_consumption_valued
                WHERE quantity_override IS NOT NULL
            """).fetchall()
            print(f"  Found {len(saved_overrides)} override(s) to migrate")
        except Exception as e:
            print(f"  Could not read old overrides (table may already be new format): {e}")
            saved_overrides = []

        # ----------------------------------------------------------------
        # 2. Drop old frame_consumption_valued, create new daily version
        # ----------------------------------------------------------------
        print("Recreating frame_consumption_valued (daily schema)...")
        conn.execute("DROP TABLE IF EXISTS supply.frame_consumption_valued CASCADE")
        conn.execute("""
            CREATE TABLE supply.frame_consumption_valued (
                fabricante        TEXT          NOT NULL,
                fecha             DATE          NOT NULL,
                mes_yyyymm        TEXT          NOT NULL,
                frame_color       TEXT          NOT NULL,
                frame_size        TEXT          NOT NULL,
                quantity          INTEGER       NOT NULL DEFAULT 0,
                unit_wac          NUMERIC(14,6) NOT NULL DEFAULT 0,
                amount            NUMERIC(14,2) NOT NULL DEFAULT 0,
                wac_calculated_at TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
                updated_at        TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
                PRIMARY KEY (fabricante, fecha, frame_color, frame_size)
            )
        """)
        conn.execute("""
            CREATE INDEX ON supply.frame_consumption_valued (fabricante, mes_yyyymm)
        """)
        print("  Done.")

        # ----------------------------------------------------------------
        # 3. Create frame_consumption_override (if not exists)
        # ----------------------------------------------------------------
        print("Creating frame_consumption_override...")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS supply.frame_consumption_override (
                fabricante        TEXT          NOT NULL,
                mes_yyyymm        TEXT          NOT NULL,
                frame_color       TEXT          NOT NULL,
                frame_size        TEXT          NOT NULL,
                quantity_override INTEGER       NOT NULL,
                opening_wac       NUMERIC(14,6) NOT NULL DEFAULT 0,
                notes             TEXT          NOT NULL DEFAULT '',
                set_at            TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
                PRIMARY KEY (fabricante, mes_yyyymm, frame_color, frame_size)
            )
        """)
        conn.commit()
        print("  Done.")

        # ----------------------------------------------------------------
        # 4. Refresh all months with consumption data
        # ----------------------------------------------------------------
        months_by_fab = conn.execute("""
            SELECT fabricante, mes_yyyymm
            FROM supply.consumo_marcos_diario
            WHERE fabricante IN (
                SELECT DISTINCT fabricante FROM supply.frame_purchases
            )
            GROUP BY fabricante, mes_yyyymm
            ORDER BY fabricante, mes_yyyymm
        """).fetchall()

        print(f"\nRefreshing {len(months_by_fab)} fabricante/month combinations...")
        for row in months_by_fab:
            fab = row["fabricante"]
            mes = row["mes_yyyymm"]
            print(f"  {fab}/{mes}...", end=" ", flush=True)
            refresh_frame_consumption_month(fab, mes, conn)
            conn.commit()
            print("OK")

        # ----------------------------------------------------------------
        # 5. Re-apply saved overrides
        # ----------------------------------------------------------------
        if saved_overrides:
            print(f"\nRe-applying {len(saved_overrides)} override(s)...")
            for ov in saved_overrides:
                fab = ov["fabricante"]
                mes = ov["mes_yyyymm"]
                color = ov["frame_color"]
                size = ov["frame_size"]
                qty = ov["quantity_override"]
                notes = ov["override_notes"] or ""
                print(f"  {fab}/{mes}/{color}/{size} -> qty={qty}", end=" ", flush=True)
                set_frame_consumption_override(fab, mes, color, size, qty, notes, conn)
                conn.commit()
                # Refresh the month so frame_stock_monthly reflects the override
                refresh_frame_consumption_month(fab, mes, conn)
                conn.commit()
                print("OK")

        # ----------------------------------------------------------------
        # 6. Summary
        # ----------------------------------------------------------------
        print("\n--- frame_stock_monthly summary ---")
        rows = conn.execute("""
            SELECT fabricante, mes_yyyymm, opening_units, opening_value,
                   purchased_units, purchased_value,
                   consumed_units, consumed_value,
                   closing_units, closing_value
            FROM supply.frame_stock_monthly
            WHERE mes_yyyymm >= '202507'
            ORDER BY fabricante, mes_yyyymm
        """).fetchall()
        print(f"{'Fab':<6} {'Month':<8} {'OpeU':>5} {'OpeV':>10} {'PurU':>5} {'PurV':>10} {'ConU':>5} {'ConV':>10} {'CloU':>5} {'CloV':>10}")
        for r in rows:
            print(f"{r['fabricante']:<6} {r['mes_yyyymm']:<8} {r['opening_units']:>5} {r['opening_value']:>10.2f} {r['purchased_units']:>5} {r['purchased_value']:>10.2f} {r['consumed_units']:>5} {r['consumed_value']:>10.2f} {r['closing_units']:>5} {r['closing_value']:>10.2f}")

        daily_count = conn.execute("SELECT COUNT(*) AS n FROM supply.frame_consumption_valued").fetchone()["n"]
        override_count = conn.execute("SELECT COUNT(*) AS n FROM supply.frame_consumption_override").fetchone()["n"]
        print(f"\nDaily rows in frame_consumption_valued: {daily_count}")
        print(f"Rows in frame_consumption_override: {override_count}")
        print("\nMigration v3 complete.")


if __name__ == "__main__":
    main()
