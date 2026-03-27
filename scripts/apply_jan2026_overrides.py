"""Apply January 2026 TGI stock adjustment overrides.

Data from physical stock count + manual adjustments (Adjustment + Consumption = Total).
Negative totals = stock returns / corrections.

Usage:
    python scripts/apply_jan2026_overrides.py
"""
from __future__ import annotations

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
    print("psycopg not installed.")
    sys.exit(1)


FABRICANTE = "TGI"
MES = "202601"
NOTES = "Ajuste recuento fisico enero 2026 (Adjustment + Consumption)"

# (frame_color, frame_size, quantity_total)  — Adjustment + Consumption
OVERRIDES = [
    ("1.Blanco",   "20x30",  -1),
    ("1.Blanco",   "30x40",   4),
    ("1.Blanco",   "40x50",   3),
    ("1.Blanco",   "50x50",   1),
    ("1.Blanco",   "50x70",  13),
    ("1.Blanco",   "60x90",   7),
    ("1.Blanco",   "70x100",  3),
    ("2.Negro",    "20x30",   2),
    ("2.Negro",    "30x40",   9),
    ("2.Negro",    "40x50",  10),
    ("2.Negro",    "50x50",   0),
    ("2.Negro",    "50x70",  16),
    ("2.Negro",    "60x90",   3),
    ("2.Negro",    "70x100", 11),
    ("3.Roble",    "20x30",  12),
    ("3.Roble",    "30x40",   6),
    ("3.Roble",    "40x50",  21),
    ("3.Roble",    "50x50",   5),
    ("3.Roble",    "50x70",   6),
    ("3.Roble",    "60x90",  18),
    ("3.Roble",    "70x100", 12),
    ("4.Dorado",   "20x30",   3),
    ("4.Dorado",   "30x40",  -1),
    ("4.Dorado",   "40x50",   4),
    ("4.Dorado",   "50x50",   2),
    ("4.Dorado",   "50x70",   2),
    ("4.Dorado",   "60x90",   1),
    ("4.Dorado",   "70x100",  4),
    ("5.Plateado",  "20x30",   2),
    ("5.Plateado",  "30x40",   1),
    ("5.Plateado",  "40x50",   0),
    ("5.Plateado",  "50x50",   1),
    ("5.Plateado",  "50x70",   0),
    ("5.Plateado",  "60x90",   1),
    ("5.Plateado",  "70x100", -3),
]

assert sum(qty for _, _, qty in OVERRIDES) == 178, \
    f"Expected total 178, got {sum(qty for _, _, qty in OVERRIDES)}"


def _read_database_url() -> str | None:
    env_path = REPO_ROOT / ".env.local"
    if not env_path.exists():
        return None
    for raw_line in env_path.read_text(encoding="utf-8-sig").splitlines():
        line = raw_line.strip()
        if line.startswith("DATABASE_URL="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    return None


def main() -> None:
    database_url = _read_database_url()
    if not database_url:
        print("DATABASE_URL not found in .env.local")
        sys.exit(1)

    from lector_facturas.supply_stock import refresh_frame_consumption_month

    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        print(f"Applying {len(OVERRIDES)} overrides for {FABRICANTE} / {MES}...")

        for color, size, qty in OVERRIDES:
            conn.execute(
                """
                INSERT INTO supply.frame_consumption_valued
                    (fabricante, mes_yyyymm, frame_color, frame_size,
                     quantity_system, quantity_effective, unit_wac_opening,
                     wac_calculated_at, amount_system, amount_effective,
                     quantity_override, override_notes, override_set_at, updated_at)
                VALUES (%s, %s, %s, %s, 0, %s, 0, NOW(), 0, 0, %s, %s, NOW(), NOW())
                ON CONFLICT (fabricante, mes_yyyymm, frame_color, frame_size) DO UPDATE SET
                    quantity_override  = EXCLUDED.quantity_override,
                    override_notes     = EXCLUDED.override_notes,
                    override_set_at    = NOW(),
                    quantity_effective = EXCLUDED.quantity_override,
                    updated_at         = NOW()
                """,
                (FABRICANTE, MES, color, size, qty, qty, NOTES),
            )
            print(f"  {color:12s} {size:6s}  override={qty:4d}")

        conn.commit()
        print("Overrides saved. Running refresh...")
        refresh_frame_consumption_month(FABRICANTE, MES, conn)
        conn.commit()
        print("Refresh complete.")

        # Print result
        row = conn.execute(
            """
            SELECT opening_units, opening_value, consumed_units, consumed_value,
                   closing_units, closing_value
            FROM supply.frame_stock_monthly
            WHERE fabricante = %s AND mes_yyyymm = %s
            """,
            (FABRICANTE, MES),
        ).fetchone()
        if row:
            print(f"\nResult for {FABRICANTE} {MES}:")
            print(f"  Opening:  {row['opening_units']} units  ${row['opening_value']}")
            print(f"  Consumed: {row['consumed_units']} units  ${row['consumed_value']}")
            print(f"  Closing:  {row['closing_units']} units  ${row['closing_value']}")


if __name__ == "__main__":
    main()
