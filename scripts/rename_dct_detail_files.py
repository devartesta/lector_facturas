from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import psycopg

from lector_facturas.google_drive import DriveConfig, GoogleDriveClient, GoogleOAuthConfig


def main() -> int:
    parser = argparse.ArgumentParser(description="Rename DCT detail files in Google Drive to invoice-based names.")
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--client-id", required=True)
    parser.add_argument("--client-secret", required=True)
    parser.add_argument("--refresh-token", required=True)
    args = parser.parse_args()

    client = GoogleDriveClient(
        DriveConfig(
            oauth=GoogleOAuthConfig(
                client_id=args.client_id,
                client_secret=args.client_secret,
                refresh_token=args.refresh_token,
            )
        )
    )

    with psycopg.connect(args.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, invoice_number, invoice_date::text, extracted_raw
                FROM invoices.documents
                WHERE supplier_code = 'DCT'
                ORDER BY invoice_date, invoice_number
                """
            )
            rows = cur.fetchall()
            for document_id, invoice_number, invoice_date, extracted_raw in rows:
                data = extracted_raw if isinstance(extracted_raw, dict) else json.loads(extracted_raw)
                changed = False
                new_details = []
                base = f"DCT_{invoice_date.replace('-', '')}_{invoice_number}"
                safe_base = base.replace('/', '-').replace('\\', '-').replace(' ', '')
                for detail in data.get("detail_files", []):
                    ext = Path(detail["stored_filename"]).suffix.lower()
                    new_name = f"{safe_base}_DETAIL{ext}"
                    if detail["stored_filename"] != new_name:
                        updated = client.update_file_name(file_id=detail["drive_file_id"], name=new_name)
                        detail["stored_filename"] = new_name
                        detail["drive_url"] = str(updated.get("webViewLink", detail["drive_url"]))
                        detail["kind"] = "DETAIL"
                        changed = True
                    new_details.append(detail)
                if changed:
                    data["detail_files"] = new_details
                    cur.execute(
                        "UPDATE invoices.documents SET extracted_raw = %s::jsonb, updated_at = NOW() WHERE id = %s",
                        (json.dumps(data), document_id),
                    )
        conn.commit()
    print("Renamed DCT detail files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
