from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from lector_facturas.folder_structure import ensure_next_month


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create next month finance folder structures.")
    parser.add_argument("--root", required=True, help="Root finance path.")
    parser.add_argument(
        "--entities",
        nargs="+",
        default=["SL", "Ltd", "Inc"],
        help="Legal entities to create.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    created = ensure_next_month(Path(args.root), entities=tuple(args.entities))
    print(f"Created {len(created)} folders.")
    for path in created:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
