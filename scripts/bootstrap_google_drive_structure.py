from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from lector_facturas.drive_bootstrap import bootstrap_drive_structure
from lector_facturas.google_drive import GoogleDriveClient
from lector_facturas.settings import load_settings


def main() -> int:
    parser = argparse.ArgumentParser(description="Create the 2026 finance folder structure in Google Drive.")
    parser.add_argument("--root-name", default="ARTESTA - 6. Finances")
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--start-month", type=int, default=1)
    parser.add_argument("--end-month", type=int, default=12)
    parser.add_argument("--parent-id", default="root")
    args = parser.parse_args()

    settings = load_settings()
    client = GoogleDriveClient(settings.to_drive_config())
    result = bootstrap_drive_structure(
        client,
        root_name=args.root_name,
        year=args.year,
        start_month=args.start_month,
        end_month=args.end_month,
        parent_id=args.parent_id,
    )
    print(
        json.dumps(
            {
                "root_folder_id": result.root_folder_id,
                "root_folder_name": result.root_folder_name,
                "created_paths_count": len(result.created_paths),
                "sample_paths": list(result.created_paths[:15]),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
