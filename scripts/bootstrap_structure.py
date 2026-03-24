from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from lector_facturas.folder_structure import BootstrapConfig, bootstrap_structure


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create finance folder structures.")
    parser.add_argument("--root", required=True, help="Root finance path.")
    parser.add_argument("--year", type=int, required=True, help="Year to create.")
    parser.add_argument("--start-month", type=int, required=True, help="First month to create.")
    parser.add_argument("--end-month", type=int, required=True, help="Last month to create.")
    parser.add_argument(
        "--entities",
        nargs="+",
        default=["SL", "Ltd", "Inc"],
        help="Legal entities to create.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = BootstrapConfig(
        root=Path(args.root),
        entities=tuple(args.entities),
        year=args.year,
        start_month=args.start_month,
        end_month=args.end_month,
    )
    created = bootstrap_structure(config)
    print(f"Created {len(created)} folders.")
    for path in created:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
