"""Create invoices.diferencias_divisas table and insert initial data.

Usage:
    python scripts/migrate_diferencias_divisas.py
    python scripts/migrate_diferencias_divisas.py --database-url postgresql://...
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


INITIAL_DATA = [
    # company_code, period_yyyymm, amount_eur, notes
    ("SL", "202601", "-132.90", "Diferencia de cambio enero 2026"),
    ("SL", "202602", "-277.52", "Diferencia de cambio febrero 2026"),
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Create diferencias_divisas table and seed initial data.")
    parser.add_argument("--database-url", default=None)
    args = parser.parse_args()

    database_url = args.database_url or _read_database_url(REPO_ROOT)
    if not database_url:
        print("No DATABASE_URL found. Pass --database-url or set it in .env.local")
        sys.exit(1)

    with psycopg.connect(database_url) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS invoices.diferencias_divisas (
                id SERIAL PRIMARY KEY,
                company_code VARCHAR(10) NOT NULL,
                period_yyyymm VARCHAR(6) NOT NULL,
                amount_eur NUMERIC(12, 2) NOT NULL,
                notes TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (company_code, period_yyyymm)
            )
        """)
        print("Table invoices.diferencias_divisas created (or already exists).")

        for company_code, period_yyyymm, amount_eur, notes in INITIAL_DATA:
            conn.execute("""
                INSERT INTO invoices.diferencias_divisas (company_code, period_yyyymm, amount_eur, notes)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (company_code, period_yyyymm) DO UPDATE
                    SET amount_eur = EXCLUDED.amount_eur, notes = EXCLUDED.notes
            """, (company_code, period_yyyymm, amount_eur, notes))
            print(f"  Upserted: {company_code} {period_yyyymm} = {amount_eur} EUR")

        conn.commit()
    print("Done.")


if __name__ == "__main__":
    main()
