"""One-off script: update Proco purchase_id=6 lines with freight-inclusive prices.

Steps:
  1. Add currency + total columns to supply.frame_purchase_lines (idempotent)
  2. Update all 21 lines for purchase_id=6 (new unit_price = total/qty)
  3. Update frame_purchases note for id=6
  4. Re-run populate_sku_wac_for_purchase(6) → rebuilds frame_sku_wac
  5. Refresh frame_consumption_valued + frame_stock_monthly for 202601, 202602, 202603
"""
from __future__ import annotations

import os, sys
from decimal import Decimal
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

DATABASE_URL = os.environ.get("DATABASE_URL") or "postgresql://postgres:GzuAahYStgmCVTkPVCNDaodpGyexTHjf@shinkansen.proxy.rlwy.net:33105/railway"

# ---------------------------------------------------------------------------
# New data: (line_id, frame_color, frame_size, qty, total_gbp)
# unit_price stored as total/qty for maximum WAC precision
# ---------------------------------------------------------------------------
LINES = [
    # id,  color,              size,     qty, total
    (139, "1.Blanco",         "40x50",   10, Decimal("66.63")),
    (140, "1.Blanco",         "50x70",   10, Decimal("87.34")),
    (141, "1.Blanco",         "70x100",  20, Decimal("304.69")),
    (142, "2.Negro",          "30x40",   40, Decimal("190.79")),
    (143, "2.Negro",          "40x50",   50, Decimal("326.46")),
    (144, "2.Negro",          "50x50",   10, Decimal("74.11")),
    (145, "2.Negro",          "50x70",  120, Decimal("1047.99")),
    (146, "2.Negro",          "60x90",   70, Decimal("931.51")),
    (147, "2.Negro",          "70x100",  60, Decimal("913.99")),
    (148, "3.Roble",          "20x30",   34, Decimal("171.01")),
    (149, "3.Roble",          "30x40",   40, Decimal("247.92")),
    (150, "3.Roble",          "50x70",   50, Decimal("568.94")),
    (151, "3.Roble",          "60x90",   36, Decimal("613.78")),
    (152, "3.Roble",          "70x100",  50, Decimal("959.66")),
    (153, "A.Roble oscuro",   "20x30",   20, Decimal("106.33")),
    (154, "A.Roble oscuro",   "30x40",   20, Decimal("129.95")),
    (155, "A.Roble oscuro",   "40x50",   10, Decimal("87.34")),
    (156, "A.Roble oscuro",   "50x50",    5, Decimal("47.22")),
    (157, "A.Roble oscuro",   "50x70",   20, Decimal("239.61")),
    (158, "A.Roble oscuro",   "60x90",   10, Decimal("178.73")),
    (159, "A.Roble oscuro",   "70x100",  10, Decimal("200.97")),
]

FABRICANTE = "Proco"
PURCHASE_ID = 6
MONTHS_TO_REFRESH = ["202601", "202602", "202603"]


def main() -> None:
    import psycopg
    from psycopg.rows import dict_row

    print(f"Connecting to DB...")
    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:

        # ---------------------------------------------------------------
        # 1. Add columns (idempotent via IF NOT EXISTS)
        # ---------------------------------------------------------------
        print("Step 1: Adding currency + total columns to frame_purchase_lines...")
        conn.execute("""
            ALTER TABLE supply.frame_purchase_lines
            ADD COLUMN IF NOT EXISTS currency TEXT,
            ADD COLUMN IF NOT EXISTS total NUMERIC(12,4)
        """)
        conn.commit()
        print("  Done.")

        # ---------------------------------------------------------------
        # 2. Update lines for purchase_id=6
        # ---------------------------------------------------------------
        print(f"Step 2: Updating {len(LINES)} lines for purchase_id={PURCHASE_ID}...")
        total_sum = Decimal("0")
        for line_id, color, size, qty, total in LINES:
            unit_price = total / qty  # exact, unrounded
            conn.execute("""
                UPDATE supply.frame_purchase_lines
                SET unit_price = %s,
                    currency   = 'GBP',
                    total      = %s
                WHERE id = %s AND purchase_id = %s
            """, (unit_price, total, line_id, PURCHASE_ID))
            total_sum += total
            print(f"  id={line_id:3d}  {color:20s}  {size:8s}  qty={qty:3d}  total={total:9.2f}  unit={unit_price:.6f}")
        conn.commit()
        print(f"  Grand total: GBP {total_sum:.2f}")

        # ---------------------------------------------------------------
        # 3. Update frame_purchases note
        # ---------------------------------------------------------------
        print("Step 3: Updating frame_purchases note...")
        conn.execute("""
            UPDATE supply.frame_purchases
            SET notes = 'Purchase order tell 3001554 / inv 000201863 (07/01/2026, incl. freight)',
                updated_at = NOW()
            WHERE id = %s
        """, (PURCHASE_ID,))
        conn.commit()
        print("  Done.")

        # ---------------------------------------------------------------
        # 4. Rebuild frame_sku_wac for all SKUs touched by purchase_id=6
        # ---------------------------------------------------------------
        print(f"Step 4: Rebuilding frame_sku_wac for purchase_id={PURCHASE_ID}...")
        from lector_facturas.supply_stock import populate_sku_wac_for_purchase
        months_from_wac = populate_sku_wac_for_purchase(PURCHASE_ID, conn)
        conn.commit()
        print(f"  WAC rebuild done. Months needing refresh: {months_from_wac}")

        # Merge with our known months
        all_months = sorted(set(months_from_wac) | set(MONTHS_TO_REFRESH))
        print(f"  Will refresh months: {all_months}")

    # ---------------------------------------------------------------
    # 5. Refresh frame_consumption_valued + frame_stock_monthly
    # ---------------------------------------------------------------
    print("Step 5: Refreshing consumption + stock monthly...")
    from lector_facturas.supply_stock import refresh_frame_consumption_months
    refresh_frame_consumption_months(FABRICANTE, all_months, DATABASE_URL)
    print("  Done.")

    print("\n✅ All DB updates complete.")
    print("Next: call API to regenerate Excel + PYG.")


if __name__ == "__main__":
    main()
