"""
Seed Proco initial stock count as of 17/11/2025 (GBP).
Inserts one frame_purchase + 41 lines, then rebuilds WAC and refreshes
frame_consumption_valued + frame_stock_monthly for all months 202511–202603.
"""
import sys
import os
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from lector_facturas.supply_stock import (
    populate_sku_wac_for_purchase,
    refresh_frame_consumption_month,
)
import psycopg
from psycopg.rows import dict_row
from decimal import Decimal

# ---- resolve DATABASE_URL ----
def get_database_url() -> str:
    if url := os.environ.get("DATABASE_URL"):
        return url
    env_file = os.path.join(os.path.dirname(__file__), "..", ".env.local")
    if os.path.exists(env_file):
        with open(env_file) as f:
            for line in f:
                if line.startswith("DATABASE_URL="):
                    return line.split("=", 1)[1].strip()
    raise RuntimeError("DATABASE_URL not found. Set it in .env.local or as env var.")

# ---------------------------------------------------------------------------
# Stock data — 100x70 normalized to 70x100
# (color, size, unit_price_gbp, qty)
# ---------------------------------------------------------------------------
LINES = [
    # 1.Blanco
    ("1.Blanco", "20x30",  Decimal("2.95"),  57),
    ("1.Blanco", "30x40",  Decimal("4.17"),  53),
    ("1.Blanco", "40x50",  Decimal("5.67"),  24),
    ("1.Blanco", "50x50",  Decimal("5.83"),  15),
    ("1.Blanco", "50x70",  Decimal("7.12"),  34),
    ("1.Blanco", "60x90",  Decimal("12.51"), 49),
    ("1.Blanco", "70x100", Decimal("12.96"), 19),   # was 100x70
    # 2.Negro
    ("2.Negro",  "20x30",  Decimal("2.93"),  186),
    ("2.Negro",  "30x40",  Decimal("4.05"),  150),
    ("2.Negro",  "40x50",  Decimal("5.55"),  97),
    ("2.Negro",  "50x50",  Decimal("6.30"),  36),
    ("2.Negro",  "50x70",  Decimal("7.43"),  103),
    ("2.Negro",  "60x90",  Decimal("11.32"), 96),
    ("2.Negro",  "70x100", Decimal("12.96"), 65),   # was 100x70
    # 3.Roble
    ("3.Roble",  "20x30",  Decimal("4.28"),  88),
    ("3.Roble",  "30x40",  Decimal("5.27"),  100),
    ("3.Roble",  "40x50",  Decimal("7.09"),  52),
    ("3.Roble",  "50x50",  Decimal("7.69"),  21),
    ("3.Roble",  "50x70",  Decimal("9.68"),  98),
    ("3.Roble",  "60x90",  Decimal("14.51"), 46),
    ("3.Roble",  "70x100", Decimal("16.33"), 25),   # was 100x70
    # A.Roble oscuro
    ("A.Roble oscuro", "20x30",  Decimal("4.52"),  16),
    ("A.Roble oscuro", "30x40",  Decimal("5.53"),  48),
    ("A.Roble oscuro", "40x50",  Decimal("7.43"),  34),
    ("A.Roble oscuro", "50x50",  Decimal("8.03"),   3),
    ("A.Roble oscuro", "50x70",  Decimal("10.19"), 18),
    ("A.Roble oscuro", "60x90",  Decimal("15.21"),  5),
    ("A.Roble oscuro", "70x100", Decimal("17.11"), 19),  # was 100x70
    # 4.Dorado  (skipping 40x50 — qty=0, price=0)
    ("4.Dorado", "20x30",  Decimal("2.79"),  27),
    ("4.Dorado", "30x40",  Decimal("3.93"),  38),
    ("4.Dorado", "50x50",  Decimal("6.24"),   1),
    ("4.Dorado", "50x70",  Decimal("7.33"),  19),
    ("4.Dorado", "60x90",  Decimal("11.03"), 27),
    ("4.Dorado", "70x100", Decimal("12.88"), 11),   # was 100x70
    # 5.Plateado
    ("5.Plateado", "20x30",  Decimal("2.79"),  10),
    ("5.Plateado", "30x40",  Decimal("3.93"),  13),
    ("5.Plateado", "40x50",  Decimal("4.83"),   4),
    ("5.Plateado", "50x50",  Decimal("6.24"),   8),
    ("5.Plateado", "50x70",  Decimal("7.33"),   8),
    ("5.Plateado", "60x90",  Decimal("11.03"),  9),
    ("5.Plateado", "70x100", Decimal("12.88"), 11),  # was 100x70
]

# Sanity check
total_units = sum(q for _, _, _, q in LINES)
total_value = sum(p * q for _, _, p, q in LINES)
assert total_units == 1743, f"Expected 1743 units, got {total_units}"
print(f"Lines: {len(LINES)}, units: {total_units}, value: GBP {total_value:.2f}")


def main():
    database_url = get_database_url()
    conn = psycopg.connect(database_url, row_factory=dict_row)
    conn.autocommit = False

    try:
        cur = conn.cursor()

        # ----------------------------------------------------------------
        # 1. Insert frame_purchase
        # ----------------------------------------------------------------
        cur.execute("""
            INSERT INTO supply.frame_purchases (fabricante, purchase_date, currency, notes)
            VALUES ('Proco', '2025-11-17', 'GBP', 'Initial stock count 17/11/2025')
            RETURNING id
        """)
        purchase_id = cur.fetchone()["id"]
        print(f"Inserted frame_purchase id={purchase_id}")

        # ----------------------------------------------------------------
        # 2. Insert purchase lines
        # ----------------------------------------------------------------
        cur.executemany("""
            INSERT INTO supply.frame_purchase_lines
                (purchase_id, frame_color, frame_size, quantity, unit_price)
            VALUES (%s, %s, %s, %s, %s)
        """, [(purchase_id, color, size, qty, price) for color, size, price, qty in LINES])
        print(f"Inserted {len(LINES)} purchase lines")

        conn.commit()

        # ----------------------------------------------------------------
        # 3. Populate WAC + get dirty months
        # ----------------------------------------------------------------
        print("Building frame_sku_wac...")
        dirty_months = populate_sku_wac_for_purchase(purchase_id, conn)
        conn.commit()
        print(f"WAC built. Dirty months: {sorted(dirty_months)}")

        # ----------------------------------------------------------------
        # 4. Refresh consumption for 202511 → 202603
        # ----------------------------------------------------------------
        months = [
            "202511", "202512",
            "202601", "202602", "202603",
        ]
        for mes in months:
            print(f"Refreshing Proco/{mes}...", end=" ", flush=True)
            refresh_frame_consumption_month("Proco", mes, conn)
            conn.commit()
            print("OK")

        # ----------------------------------------------------------------
        # 5. Quick summary
        # ----------------------------------------------------------------
        cur.execute("""
            SELECT mes_yyyymm, opening_units, opening_value,
                   purchased_units, purchased_value,
                   consumed_units, consumed_value,
                   closing_units, closing_value
            FROM supply.frame_stock_monthly
            WHERE fabricante = 'Proco'
            ORDER BY mes_yyyymm
        """)
        rows = cur.fetchall()
        print("\n--- frame_stock_monthly (Proco) ---")
        print(f"{'Month':<8}  {'OpeU':>5}  {'OpeV':>10}  {'PurU':>5}  {'PurV':>10}  {'ConU':>5}  {'ConV':>10}  {'CloU':>5}  {'CloV':>10}")
        for r in rows:
            print(f"{r['mes_yyyymm']:<8}  {r['opening_units']:>5}  {r['opening_value']:>10.2f}  {r['purchased_units']:>5}  {r['purchased_value']:>10.2f}  {r['consumed_units']:>5}  {r['consumed_value']:>10.2f}  {r['closing_units']:>5}  {r['closing_value']:>10.2f}")

        conn.close()
        print("\nDone.")

    except Exception:
        conn.rollback()
        conn.close()
        raise


if __name__ == "__main__":
    main()
