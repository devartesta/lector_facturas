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
from lector_facturas.parsers.rever import parse_rever_pdf
from lector_facturas.provider_catalog import ProviderRecord
from lector_facturas.review_workflow import company_folder_name, get_provider


SCHEMA_NAME = "invoices"
COMPANY_CODES = {
    "ARTESTA STORE, S.L.": "SL",
    "ARTESTA STORES (UK) LTD": "LTD",
    "ARTESTA INC": "INC",
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Import REVER documents from review Drive folder.")
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--root-folder-id", required=True)
    parser.add_argument("--review-folder-id", required=True)
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

    review_files = [
        item
        for item in client.list_files(parent_id=args.review_folder_id)
        if item.get("mimeType") != "application/vnd.google-apps.folder"
    ]
    rever_files = [item for item in review_files if _is_rever_candidate(str(item["name"]))]

    imported: list[str] = []
    skipped: list[str] = []

    with psycopg.connect(args.database_url) as conn:
        with conn.cursor() as cur:
            for item in sorted(rever_files, key=lambda file_item: str(file_item["name"])):
                stored_name = str(item["name"])
                original_name = _original_name_from_review_name(stored_name)
                file_bytes = client.download_file_bytes(file_id=str(item["id"]))
                with tempfile.NamedTemporaryFile(delete=False, suffix=Path(original_name).suffix or ".pdf") as handle:
                    handle.write(file_bytes)
                    temp_path = Path(handle.name)
                try:
                    parsed = parse_rever_pdf(temp_path)
                finally:
                    temp_path.unlink(missing_ok=True)

                company = parsed.billed_company_name
                company_code = COMPANY_CODES[company]
                provider = get_provider(company, parsed.supplier_code)
                ensure_supplier_row(cur, provider)
                supplier_id = lookup_supplier_id(cur, company_code, parsed.supplier_code)
                if document_exists(
                    cur,
                    company_code=company_code,
                    supplier_code=parsed.supplier_code,
                    invoice_number=parsed.invoice_number,
                    document_type=parsed.document_type,
                ):
                    skipped.append(f"{stored_name} :: already_in_db")
                    continue

                final_name = _build_final_name(parsed, original_name)
                windows_path = build_windows_path(company, parsed.period_yyyymm, _destination_path(parsed.document_type), final_name)
                parent_id = ensure_drive_path(client, args.root_folder_id, windows_path)

                client.update_file_name(file_id=str(item["id"]), name=final_name)
                client.move_file(file_id=str(item["id"]), new_parent_id=parent_id)
                moved_file = client.get_file(str(item["id"]))

                upsert_document_row(
                    cur,
                    supplier_id=supplier_id,
                    company_code=company_code,
                    parsed=parsed,
                    windows_path=windows_path,
                    drive_url=str(moved_file.get("webViewLink", "")),
                    drive_file_id=str(moved_file.get("id", "")),
                    original_filename=original_name,
                    review_name=stored_name,
                    note="Imported manually from review/invoices queue.",
                )
                imported.append(final_name)
        conn.commit()

    print(f"Imported {len(imported)} REVER documents from review folder.")
    for name in imported:
        print(f"IMPORTED\t{name}")
    for name in skipped:
        print(f"SKIPPED\t{name}")
    return 0


def _is_rever_candidate(stored_name: str) -> bool:
    lower = stored_name.lower()
    original = _original_name_from_review_name(stored_name).lower()
    return "invoice-rvr-" in original or "suppliednote" in original or "itsrever" in lower


def _original_name_from_review_name(stored_name: str) -> str:
    parts = stored_name.split("_", 2)
    return parts[2] if len(parts) == 3 else stored_name


def _destination_path(document_type: str) -> str:
    if document_type == "supplied_note":
        return "income/other_income/supplies"
    return "expenses/opex/technology"


def _build_final_name(parsed, original_name: str) -> str:
    extension = Path(original_name).suffix.lower() or ".pdf"
    if parsed.document_type == "supplied_note":
        return f"REVER_SUPPLIED_{parsed.period_yyyymm}{extension}"
    return f"{parsed.supplier_code}_{parsed.invoice_date.strftime('%Y%m%d')}_{parsed.invoice_number}{extension}"


def ensure_supplier_row(cursor, provider: ProviderRecord) -> None:
    company_code = COMPANY_CODES[provider.company]
    supplier_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"suppliers:{company_code}:{provider.supplier_code}"))
    cursor.execute(
        f"""
        INSERT INTO {SCHEMA_NAME}.suppliers (
            id, company_code, current_folder, supplier_code, supplier_name,
            billing_company_name, destination_path, is_active, notes, sender_emails
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, TRUE, %s, %s
        )
        ON CONFLICT (company_code, supplier_code) DO UPDATE SET
            current_folder = EXCLUDED.current_folder,
            supplier_name = EXCLUDED.supplier_name,
            billing_company_name = EXCLUDED.billing_company_name,
            destination_path = EXCLUDED.destination_path,
            is_active = TRUE,
            notes = EXCLUDED.notes,
            sender_emails = EXCLUDED.sender_emails,
            updated_at = NOW()
        """,
        (
            supplier_id,
            company_code,
            provider.current_folder,
            provider.supplier_code,
            provider.supplier_code,
            provider.provider_name,
            provider.destination_path,
            provider.notes,
            json.dumps(list(provider.sender_emails)),
        ),
    )


def lookup_supplier_id(cursor, company_code: str, supplier_code: str) -> str | None:
    row = cursor.execute(
        f"SELECT id FROM {SCHEMA_NAME}.suppliers WHERE company_code = %s AND supplier_code = %s",
        (company_code, supplier_code),
    ).fetchone()
    return str(row[0]) if row else None


def document_exists(cursor, *, company_code: str, supplier_code: str, invoice_number: str, document_type: str) -> bool:
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
        (company_code, supplier_code, invoice_number, document_type),
    ).fetchone()
    return bool(row)


def upsert_document_row(
    cursor,
    *,
    supplier_id: str | None,
    company_code: str,
    parsed,
    windows_path: str,
    drive_url: str,
    drive_file_id: str,
    original_filename: str,
    review_name: str,
    note: str,
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
            company_code,
            windows_path,
            drive_url,
            None,
            parsed.sender_email,
            original_filename,
            "",
            parsed.billing_period_start,
            parsed.billing_period_end,
            parsed.vat_percent,
            parsed.gross_amount,
            parsed.vat_amount,
            parsed.net_amount,
            supplier_id,
            parsed.supplier_code,
            parsed.currency_code,
            drive_file_id,
            "GOOGLE_DRIVE",
            parsed.document_type,
            "classified",
            "import",
            "",
            "",
            original_filename,
            parsed.parser_name,
            parsed.parser_confidence,
            extracted_raw,
            note,
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
