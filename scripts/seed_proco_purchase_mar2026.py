"""
Seed Proco purchase order dated 19/03/2026 (invoice 000203519, order 3001584).
Prices in GBP (include freight allocation).
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from lector_facturas.supply_stock import populate_sku_wac_for_purchase, refresh_frame_consumption_month
import psycopg
from psycopg.rows import dict_row
from decimal import Decimal

def get_database_url() -> str:
    if url := os.environ.get("DATABASE_URL"):
        return url
    env_file = os.path.join(os.path.dirname(__file__), "..", ".env.local")
    with open(env_file) as f:
        for line in f:
            if line.startswith("DATABASE_URL="):
                return line.split("=", 1)[1].strip()
    raise RuntimeError("DATABASE_URL not found")

# SKU mapping: (frame_color, frame_size, unit_price_gbp, qty)
LINES = [
    ("1.Blanco",       "20x30",  Decimal("3.74"),   8),
    ("1.Blanco",       "30x40",  Decimal("4.86"),   1),
    ("1.Blanco",       "40x50",  Decimal("6.57"),  39),
    ("1.Blanco",       "50x50",  Decimal("7.32"),   5),
    ("1.Blanco",       "50x70",  Decimal("8.62"),  55),
    ("1.Blanco",       "70x100", Decimal("15.04"),  9),
    ("2.Negro",        "30x40",  Decimal("4.72"),  91),
    ("2.Negro",        "40x50",  Decimal("6.44"),  30),
    ("2.Negro",        "50x70",  Decimal("8.62"),  215),
    ("2.Negro",        "60x90",  Decimal("13.13"), 38),
    ("2.Negro",        "70x100", Decimal("15.04"), 74),
    ("3.Roble",        "20x30",  Decimal("4.96"),  73),
    ("3.Roble",        "30x40",  Decimal("6.11"),  73),
    ("3.Roble",        "40x50",  Decimal("8.23"),  101),
    ("3.Roble",        "50x70",  Decimal("11.23"), 73),
    ("3.Roble",        "60x90",  Decimal("16.83"), 85),
    ("3.Roble",        "70x100", Decimal("18.94"), 68),
    ("A.Roble oscuro", "20x30",  Decimal("5.25"),  16),
    ("A.Roble oscuro", "30x40",  Decimal("6.41"),  12),
    ("A.Roble oscuro", "50x50",  Decimal("9.32"),   8),
    ("A.Roble oscuro", "50x70",  Decimal("11.82"), 18),
    ("A.Roble oscuro", "60x90",  Decimal("17.64"), 19),
    ("A.Roble oscuro", "70x100", Decimal("19.84"), 15),
]

total_units = sum(q for _, _, _, q in LINES)
total_gbp   = sum(p * q for _, _, p, q in LINES)
print(f"Lines: {len(LINES)}, units: {total_units}, value: GBP {total_gbp:.2f}")

def main():
    db_url = get_database_url()
    conn = psycopg.connect(db_url, row_factory=dict_row)
    conn.autocommit = False
    try:
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO supply.frame_purchases (fabricante, purchase_date, currency, notes)
            VALUES ('Proco', '2026-03-19', 'GBP',
                    'Purchase order 3001584 / inv 000203519 (19/03/2026, incl. freight)')
            RETURNING id
        """)
        purchase_id = cur.fetchone()["id"]
        print(f"Inserted frame_purchase id={purchase_id}")

        cur.executemany("""
            INSERT INTO supply.frame_purchase_lines
                (purchase_id, frame_color, frame_size, quantity, unit_price)
            VALUES (%s, %s, %s, %s, %s)
        """, [(purchase_id, c, s, q, p) for c, s, p, q in LINES])
        print(f"Inserted {len(LINES)} lines")
        conn.commit()

        print("Building WAC...")
        dirty_months = populate_sku_wac_for_purchase(purchase_id, conn)
        conn.commit()
        print(f"Dirty months: {sorted(dirty_months)}")

        months_to_refresh = sorted(set(dirty_months) | {"202603"})
        for mes in months_to_refresh:
            print(f"Refreshing Proco/{mes}...", end=" ", flush=True)
            refresh_frame_consumption_month("Proco", mes, conn)
            conn.commit()
            print("OK")

        # Summary
        cur.execute("""
            SELECT mes_yyyymm, opening_units, opening_value,
                   purchased_units, purchased_value,
                   consumed_units, consumed_value,
                   closing_units, closing_value
            FROM supply.frame_stock_monthly
            WHERE fabricante = 'Proco' AND mes_yyyymm >= '202511'
            ORDER BY mes_yyyymm
        """)
        rows = cur.fetchall()
        print(f"\n{'Month':<8}  {'OpeU':>5}  {'OpeV':>10}  {'PurU':>5}  {'PurV':>10}  {'ConU':>5}  {'ConV':>10}  {'CloU':>5}  {'CloV':>10}")
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
