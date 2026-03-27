"""
Seed Proco purchase order dated 07/01/2026 (invoice tell 3001554 / 000201863).
Prices in GBP.
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

# color, size, unit_price_gbp, qty
LINES = [
    ("1.Blanco",       "40x50",  Decimal("5.58"),  10),
    ("1.Blanco",       "50x70",  Decimal("7.32"),  10),
    ("1.Blanco",       "70x100", Decimal("12.76"), 20),
    ("2.Negro",        "30x40",  Decimal("4.00"),  40),
    ("2.Negro",        "40x50",  Decimal("5.47"),  50),
    ("2.Negro",        "50x50",  Decimal("6.21"),  10),
    ("2.Negro",        "50x70",  Decimal("7.32"),  120),
    ("2.Negro",        "60x90",  Decimal("11.15"), 70),
    ("2.Negro",        "70x100", Decimal("12.76"), 60),
    ("3.Roble",        "20x30",  Decimal("4.21"),  34),
    ("3.Roble",        "30x40",  Decimal("5.19"),  40),
    ("3.Roble",        "50x70",  Decimal("9.53"),  50),
    ("3.Roble",        "60x90",  Decimal("14.29"), 36),
    ("3.Roble",        "70x100", Decimal("16.09"), 50),
    ("A.Roble oscuro", "20x30",  Decimal("4.45"),  20),
    ("A.Roble oscuro", "30x40",  Decimal("5.44"),  20),
    ("A.Roble oscuro", "40x50",  Decimal("7.32"),  10),
    ("A.Roble oscuro", "50x50",  Decimal("7.91"),   5),
    ("A.Roble oscuro", "50x70",  Decimal("10.04"), 20),
    ("A.Roble oscuro", "60x90",  Decimal("14.98"), 10),
    ("A.Roble oscuro", "70x100", Decimal("16.84"), 10),
]

total_units = sum(q for _, _, _, q in LINES)
total_gbp   = sum(p * q for _, _, p, q in LINES)
print(f"Lines: {len(LINES)}, units: {total_units}, value: GBP {total_gbp:.2f}")
# Expected from invoice: 655 units, £7,233.11 equivalent in GBP
# (user provided GBP prices derived from EUR at exchange rate)

def main():
    db_url = get_database_url()
    conn = psycopg.connect(db_url, row_factory=dict_row)
    conn.autocommit = False
    try:
        cur = conn.cursor()

        # 1. Insert purchase
        cur.execute("""
            INSERT INTO supply.frame_purchases (fabricante, purchase_date, currency, notes)
            VALUES ('Proco', '2026-01-07', 'GBP', 'Purchase order tell 3001554 / inv 000201863 (07/01/2026)')
            RETURNING id
        """)
        purchase_id = cur.fetchone()["id"]
        print(f"Inserted frame_purchase id={purchase_id}")

        # 2. Insert lines
        cur.executemany("""
            INSERT INTO supply.frame_purchase_lines
                (purchase_id, frame_color, frame_size, quantity, unit_price)
            VALUES (%s, %s, %s, %s, %s)
        """, [(purchase_id, color, size, qty, price) for color, size, price, qty in LINES])
        print(f"Inserted {len(LINES)} lines")
        conn.commit()

        # 3. WAC rebuild
        print("Building frame_sku_wac...")
        dirty_months = populate_sku_wac_for_purchase(purchase_id, conn)
        conn.commit()
        print(f"Dirty months: {sorted(dirty_months)}")

        # 4. Refresh affected months
        months_to_refresh = sorted(set(dirty_months) | {"202601", "202602", "202603"})
        for mes in months_to_refresh:
            print(f"Refreshing Proco/{mes}...", end=" ", flush=True)
            refresh_frame_consumption_month("Proco", mes, conn)
            conn.commit()
            print("OK")

        # 5. Summary
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
