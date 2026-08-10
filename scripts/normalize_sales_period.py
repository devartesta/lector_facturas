from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from lector_facturas.sales_period_repair import normalize_sl_sales_period_in_database
from lector_facturas.settings import load_settings


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("period_yyyymm")
    args = parser.parse_args()
    load_settings()
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        raise RuntimeError("DATABASE_URL is not configured")
    result = normalize_sl_sales_period_in_database(
        database_url=database_url,
        period_yyyymm=args.period_yyyymm,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
