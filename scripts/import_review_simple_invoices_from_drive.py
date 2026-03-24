from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import tempfile
import uuid

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import psycopg

from lector_facturas.google_drive import DriveConfig, GoogleDriveClient, GoogleOAuthConfig
from lector_facturas.parsers.gorgias import parse_gorgias_pdf
from lector_facturas.parsers.hetzner import parse_hetzner_pdf
from lector_facturas.parsers.openai import parse_openai_pdf
from lector_facturas.parsers.masmovil import parse_masmovil_pdf
from lector_facturas.parsers.railway import parse_railway_pdf
from lector_facturas.parsers.yumaai import parse_yumaai_pdf
from lector_facturas.review_workflow import company_folder_name, get_provider


SCHEMA_NAME = "invoices"
COMPANY = "ARTESTA STORE, S.L."
COMPANY_CODE = "SL"


def main() -> int:
    parser = argparse.ArgumentParser(description="Import simple invoice PDFs from review Drive folder.")
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--root-folder-id", required=True)
    parser.add_argument("--review-folder-id", required=True)
    parser.add_argument("--already-loaded-folder-id", required=True)
    parser.add_argument("--client-id", required=True)
    parser.add_argument("--client-secret", required=True)
    parser.add_argument("--refresh-token", required=True)
    parser.add_argument("--providers", nargs="+", required=True)
    args = parser.parse_args()

    provider_set = {value.strip().upper() for value in args.providers}
    drive_client = GoogleDriveClient(
        DriveConfig(
            oauth=GoogleOAuthConfig(
                client_id=args.client_id,
                client_secret=args.client_secret,
                refresh_token=args.refresh_token,
            ),
            root_folder_id=args.root_folder_id,
        )
    )

    review_files = [
        item
        for item in drive_client.list_files(parent_id=args.review_folder_id)
        if item.get("mimeType") != "application/vnd.google-apps.folder"
    ]

    imported: list[str] = []
    moved_loaded: list[str] = []
    skipped: list[str] = []

    with psycopg.connect(args.database_url) as conn:
        with conn.cursor() as cur:
            for item in sorted(review_files, key=lambda file_item: str(file_item["name"])):
                detection = detect_candidate(str(item["name"]))
                if not detection or detection.provider_code not in provider_set:
                    continue

                stored_name = str(item["name"])
                original_name = original_name_from_review_name(stored_name)
                file_bytes = drive_client.download_file_bytes(file_id=str(item["id"]))
                with tempfile.NamedTemporaryFile(delete=False, suffix=Path(original_name).suffix or ".pdf") as handle:
                    handle.write(file_bytes)
                    temp_path = Path(handle.name)
                try:
                    parsed = detection.parser(temp_path)
                finally:
                    temp_path.unlink(missing_ok=True)

                if parsed.invoice_date.year != 2026:
                    skipped.append(f"{stored_name} :: outside_2026")
                    continue

                document_type = getattr(parsed, "document_type", "invoice")
                division_invoice = getattr(parsed, "division_invoice", "")

                if document_exists(
                    cur,
                    supplier_code=detection.provider_code,
                    invoice_number=parsed.invoice_number,
                    document_type=document_type,
                ):
                    drive_client.move_file(file_id=str(item["id"]), new_parent_id=args.already_loaded_folder_id)
                    moved_loaded.append(stored_name)
                    continue

                provider = get_provider(COMPANY, detection.provider_code)
                supplier_id = lookup_supplier_id(cur, COMPANY_CODE, detection.provider_code)
                final_name = build_final_name(
                    supplier_code=detection.provider_code,
                    invoice_date=parsed.invoice_date,
                    invoice_number=parsed.invoice_number,
                    original_filename=original_name,
                )
                windows_path = build_windows_path(COMPANY, parsed.period_yyyymm, provider.destination_path, final_name)
                parent_id = ensure_drive_path(drive_client, args.root_folder_id, windows_path)

                drive_client.update_file_name(file_id=str(item["id"]), name=final_name)
                drive_client.move_file(file_id=str(item["id"]), new_parent_id=parent_id)
                moved_file = drive_client.get_file(str(item["id"]))

                insert_document_row(
                    cur,
                    supplier_id=supplier_id,
                    supplier_code=detection.provider_code,
                    parsed=parsed,
                    windows_path=windows_path,
                    drive_url=str(moved_file.get("webViewLink", "")),
                    drive_file_id=str(moved_file.get("id", "")),
                    original_filename=original_name,
                    review_name=stored_name,
                    division_invoice=division_invoice,
                    document_type=document_type,
                )
                imported.append(final_name)
        conn.commit()

    print(f"Imported {len(imported)} documents.")
    for name in imported:
        print(f"IMPORTED\t{name}")
    for name in moved_loaded:
        print(f"ALREADY_LOADED\t{name}")
    for name in skipped:
        print(f"SKIPPED\t{name}")
    return 0


class Detection:
    def __init__(self, provider_code: str, parser):
        self.provider_code = provider_code
        self.parser = parser


def detect_candidate(stored_name: str) -> Detection | None:
    lower = stored_name.lower()
    if "gorgias" in lower:
        return Detection("GORGIAS", parse_gorgias_pdf)
    if "hetzner" in lower:
        return Detection("HETZNER", parse_hetzner_pdf)
    if "masmovil" in lower:
        return Detection("MASMOVIL", parse_masmovil_pdf)
    if "1602c2f5" in lower:
        return Detection("RAILWAY", parse_railway_pdf)
    if "oqxbyxmp" in lower:
        return Detection("YUMAAI", parse_yumaai_pdf)
    if "bzhjntub" in lower or "7bsdv5am" in lower:
        return Detection("OPENAI", parse_openai_pdf)
    return None


def original_name_from_review_name(stored_name: str) -> str:
    parts = stored_name.split("_", 2)
    return parts[2] if len(parts) == 3 else stored_name


def build_final_name(*, supplier_code: str, invoice_date, invoice_number: str, original_filename: str) -> str:
    extension = Path(original_filename).suffix.lower() or ".pdf"
    safe_invoice = invoice_number.replace("/", "-").replace("\\", "-").replace(" ", "")
    return f"{supplier_code}_{invoice_date.strftime('%Y%m%d')}_{safe_invoice}{extension}"


def lookup_supplier_id(cursor, company_code: str, supplier_code: str) -> str | None:
    row = cursor.execute(
        f"SELECT id FROM {SCHEMA_NAME}.suppliers WHERE company_code = %s AND supplier_code = %s",
        (company_code, supplier_code),
    ).fetchone()
    return str(row[0]) if row else None


def document_exists(cursor, *, supplier_code: str, invoice_number: str, document_type: str) -> bool:
    row = cursor.execute(
        f"""
        SELECT 1
        FROM {SCHEMA_NAME}.documents
        WHERE company_code = %s
          AND supplier_code = %s
          AND invoice_number = %s
          AND document_type = %s
        LIMIT 1
        """,
        (COMPANY_CODE, supplier_code, invoice_number, document_type),
    ).fetchone()
    return bool(row)


def insert_document_row(
    cursor,
    *,
    supplier_id: str | None,
    supplier_code: str,
    parsed,
    windows_path: str,
    drive_url: str,
    drive_file_id: str,
    original_filename: str,
    review_name: str,
    division_invoice: str,
    document_type: str,
) -> None:
    extracted_raw = json.dumps({**parsed.extracted_raw, "review_queue_name": review_name})
    cursor.execute(
        f"""
        INSERT INTO {SCHEMA_NAME}.documents (
            id, invoice_number, invoice_date, issuer_company_name, billed_company_name, supplier_name, company_code, windows_path, drive_url,
            received_at, sender_email, original_filename, division_invoice, billing_period_start, billing_period_end, vat_percent, gross_amount,
            vat_amount, net_amount, supplier_id, supplier_code, currency_code, drive_file_id, storage_root, document_type, status, source_channel,
            email_message_id, email_thread_id, attachment_original_name, parser_name, parser_confidence, extracted_raw, review_notes,
            created_at, updated_at, source_sender, source_subject, period_yyyymm
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s,
            NOW(), NOW(), %s, %s, %s
        )
        """,
        (
            str(uuid.uuid4()),
            parsed.invoice_number,
            parsed.invoice_date,
            parsed.issuer_company_name,
            parsed.billed_company_name,
            parsed.supplier_name,
            COMPANY_CODE,
            windows_path,
            drive_url,
            None,
            parsed.sender_email,
            original_filename,
            division_invoice,
            parsed.billing_period_start,
            parsed.billing_period_end,
            parsed.vat_percent,
            parsed.gross_amount,
            parsed.vat_amount,
            parsed.net_amount,
            supplier_id,
            supplier_code,
            parsed.currency_code,
            drive_file_id,
            "GOOGLE_DRIVE",
            document_type,
            "classified",
            "import",
            "",
            "",
            original_filename,
            parsed.parser_name,
            parsed.parser_confidence,
            extracted_raw,
            "Imported manually from review/invoices queue.",
            parsed.sender_email,
            original_filename,
            parsed.period_yyyymm,
        ),
    )


def build_windows_path(company: str, period_yyyymm: str, destination_path: str, filename: str) -> str:
    return "\\".join(
        [
            "ARTESTA - 6. Finances",
            company_folder_name(company),
            period_yyyymm[:4],
            period_yyyymm,
            *Path(destination_path).parts,
            filename,
        ]
    )


def ensure_drive_path(client: GoogleDriveClient, root_folder_id: str, windows_path: str) -> str:
    parts = windows_path.split("\\")
    parent_id = root_folder_id
    for folder_name in parts[1:-1]:
        folder = client.ensure_folder(name=folder_name, parent_id=parent_id)
        parent_id = str(folder["id"])
    return parent_id


if __name__ == "__main__":
    raise SystemExit(main())
