"""Create supply.frame_purchases and supply.frame_purchase_lines tables.

Usage:
    python scripts/migrate_frame_purchases.py
    python scripts/migrate_frame_purchases.py --database-url postgresql://...
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
except ImportError:
    print("psycopg not installed. Run: pip install psycopg[binary]")
    sys.exit(1)


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
    parser = argparse.ArgumentParser(description="Create frame_purchases tables in supply schema.")
    parser.add_argument("--database-url", default=None)
    args = parser.parse_args()

    database_url = args.database_url or _read_database_url(REPO_ROOT)
    if not database_url:
        print("No DATABASE_URL found. Pass --database-url or set it in .env.local")
        sys.exit(1)

    with psycopg.connect(database_url) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS supply.frame_purchases (
                id            SERIAL PRIMARY KEY,
                fabricante    TEXT NOT NULL,
                purchase_date DATE NOT NULL,
                currency      TEXT NOT NULL,
                notes         TEXT NOT NULL DEFAULT '',
                created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        print("Table supply.frame_purchases created (or already exists).")

        conn.execute("""
            CREATE INDEX IF NOT EXISTS frame_purchases_fabricante_date_idx
                ON supply.frame_purchases (fabricante, purchase_date)
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS supply.frame_purchase_lines (
                id          SERIAL PRIMARY KEY,
                purchase_id INTEGER NOT NULL REFERENCES supply.frame_purchases(id) ON DELETE CASCADE,
                frame_color TEXT NOT NULL,
                frame_size  TEXT NOT NULL,
                quantity    INTEGER NOT NULL CHECK (quantity > 0),
                unit_price  NUMERIC(14,4) NOT NULL CHECK (unit_price >= 0),
                UNIQUE (purchase_id, frame_color, frame_size)
            )
        """)
        print("Table supply.frame_purchase_lines created (or already exists).")

        conn.execute("""
            CREATE INDEX IF NOT EXISTS frame_purchase_lines_purchase_id_idx
                ON supply.frame_purchase_lines (purchase_id)
        """)

        conn.commit()
    print("Done.")


if __name__ == "__main__":
    main()
