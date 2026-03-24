from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import psycopg

from lector_facturas.google_drive import DriveConfig, GoogleDriveClient, GoogleOAuthConfig


SCHEMA_NAME = "invoices"


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload imported APPHOTO invoices to Google Drive and update Postgres.")
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--root-folder-id", required=True)
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
            ),
            root_folder_id=args.root_folder_id,
        )
    )

    uploaded: list[tuple[str, str, str]] = []
    with psycopg.connect(args.database_url) as conn:
        with conn.cursor() as cur:
            delete_fake_rows(cur)
            cur.execute(
                f"""
                SELECT id, supplier_code, original_filename, windows_path, extracted_raw->>'local_source_file'
                FROM {SCHEMA_NAME}.documents
                WHERE supplier_code IN ('APPHOTOES', 'APPHOTOCAN')
                ORDER BY invoice_date, invoice_number
                """
            )
            rows = cur.fetchall()
            for document_id, supplier_code, original_filename, windows_path, local_source_file in rows:
                if not local_source_file:
                    continue
                local_path = Path(local_source_file)
                if not local_path.exists():
                    raise FileNotFoundError(local_source_file)
                parent_id = ensure_drive_path(client, root_folder_id=args.root_folder_id, windows_path=windows_path)
                drive_file = client.ensure_file(
                    name=Path(windows_path).name,
                    parent_id=parent_id,
                    content=local_path.read_bytes(),
                    mime_type="application/pdf",
                )
                drive_url = str(drive_file.get("webViewLink", ""))
                drive_file_id = str(drive_file.get("id", ""))
                cur.execute(
                    f"""
                    UPDATE {SCHEMA_NAME}.documents
                    SET drive_file_id = %s,
                        drive_url = %s,
                        storage_root = 'GOOGLE_DRIVE',
                        updated_at = NOW()
                    WHERE id = %s
                    """,
                    (drive_file_id, drive_url, document_id),
                )
                uploaded.append((supplier_code, original_filename, drive_url))
        conn.commit()

    print(f"Uploaded {len(uploaded)} APPHOTO invoices to Google Drive.")
    for supplier_code, original_filename, drive_url in uploaded:
        print(f"- {supplier_code} | {original_filename} | {drive_url}")
    return 0


def delete_fake_rows(cursor) -> None:
    cursor.execute(f"DELETE FROM {SCHEMA_NAME}.review_items")
    cursor.execute(
        f"""
        DELETE FROM {SCHEMA_NAME}.documents
        WHERE invoice_number = ''
           OR supplier_code IN ('SHAREDSERVICESSL', 'ADOBE')
              AND original_filename IN ('Factura_2026-0009.pdf', 'IEE2026001813920.pdf')
        """
    )


def ensure_drive_path(client: GoogleDriveClient, *, root_folder_id: str, windows_path: str) -> str:
    parts = windows_path.split("\\")
    if len(parts) < 3:
        raise ValueError(f"Unexpected windows path: {windows_path}")
    folder_parts = parts[1:-1]
    parent_id = root_folder_id
    for folder_name in folder_parts:
        folder = client.ensure_folder(name=folder_name, parent_id=parent_id)
        parent_id = str(folder["id"])
    return parent_id


if __name__ == "__main__":
    raise SystemExit(main())
