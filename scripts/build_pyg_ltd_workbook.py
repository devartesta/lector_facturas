from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from lector_facturas.pyg_ltd_workbook import build_pyg_ltd_workbook, collect_pyg_ltd_data, default_output_path


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
    parser = argparse.ArgumentParser(description="Build the LTD P&G workbook from classified data.")
    parser.add_argument("--year", type=int, default=2026, help="Fiscal year to build.")
    parser.add_argument("--database-url", default=None, help="Optional DATABASE_URL override.")
    parser.add_argument("--output", default=None, help="Optional xlsx output path.")
    args = parser.parse_args()

    repo_root = REPO_ROOT
    bundle = collect_pyg_ltd_data(year=args.year, database_url=args.database_url or _read_database_url(repo_root))
    output_path = Path(args.output) if args.output else default_output_path(repo_root, args.year)
    print(build_pyg_ltd_workbook(bundle, output_path))


if __name__ == "__main__":
    main()
