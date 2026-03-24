from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
import sys
import tempfile
import uuid
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import psycopg

from lector_facturas.gmail_sync import download_attachment_bytes, list_messages_in_window
from lector_facturas.google_drive import DriveConfig, GoogleDriveClient, GoogleOAuthConfig
from lector_facturas.parsers.dct import parse_dct_pdf
from lector_facturas.review_notifications import GmailConfig
from lector_facturas.review_workflow import company_folder_name, get_provider


SCHEMA_NAME = "invoices"
COMPANY = "ARTESTA STORE, S.L."
COMPANY_CODE = "SL"


def main() -> int:
    parser = argparse.ArgumentParser(description="Import DCT documents from review Drive folder with detail Excel.")
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--root-folder-id", required=True)
    parser.add_argument("--review-folder-id", required=True)
    parser.add_argument("--client-id", required=True)
    parser.add_argument("--client-secret", required=True)
    parser.add_argument("--refresh-token", required=True)
    parser.add_argument("--gmail-sender", default="andrea@artestastore.com")
    parser.add_argument("--from-at", default="2026-01-01T00:00:00+01:00")
    parser.add_argument("--to-at", default="2026-03-21T23:59:59+01:00")
    args = parser.parse_args()

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
    gmail_config = GmailConfig(
        client_id=args.client_id,
        client_secret=args.client_secret,
        refresh_token=args.refresh_token,
        sender=args.gmail_sender,
        recipients=(args.gmail_sender,),
    )
    messages = list_messages_in_window(
        gmail_config,
        from_at=datetime.fromisoformat(args.from_at),
        to_at=datetime.fromisoformat(args.to_at),
        max_messages=1000,
    )

    review_files = [
        item
        for item in drive_client.list_files(parent_id=args.review_folder_id)
        if item.get("mimeType") != "application/vnd.google-apps.folder"
    ]
    dct_files = [item for item in review_files if _is_dct_candidate(str(item["name"]))]

    imported: list[str] = []
    skipped: list[str] = []
    with psycopg.connect(args.database_url) as conn:
        with conn.cursor() as cur:
            provider = get_provider(COMPANY, "DCT")
            supplier_id = lookup_supplier_id(cur, COMPANY_CODE, "DCT")
            for item in sorted(dct_files, key=lambda file_item: str(file_item["name"])):
                stored_name = str(item["name"])
                original_name = _original_name_from_review_name(stored_name)
                file_bytes = drive_client.download_file_bytes(file_id=str(item["id"]))
                with tempfile.NamedTemporaryFile(delete=False, suffix=Path(original_name).suffix or ".pdf") as handle:
                    handle.write(file_bytes)
                    temp_path = Path(handle.name)
                try:
                    parsed = parse_dct_pdf(temp_path)
                finally:
                    temp_path.unlink(missing_ok=True)

                if document_exists(cur, invoice_number=parsed.invoice_number):
                    skipped.append(f"{stored_name} :: already_in_db")
                    continue

                message, detail_attachments = find_matching_message(messages, original_name)
                final_name = build_base_filename(parsed.invoice_date, parsed.invoice_number) + ".pdf"
                windows_path = build_windows_path(COMPANY, parsed.period_yyyymm, provider.destination_path, final_name)
                parent_id = ensure_drive_path(drive_client, args.root_folder_id, windows_path)

                drive_client.update_file_name(file_id=str(item["id"]), name=final_name)
                drive_client.move_file(file_id=str(item["id"]), new_parent_id=parent_id)
                moved_file = drive_client.get_file(str(item["id"]))

                detail_files = []
                for attachment in detail_attachments:
                    content = download_attachment_bytes(
                        gmail_config,
                        message_id=message.message_id,
                        attachment_id=attachment.attachment_id,
                    )
                    detail_name = build_detail_filename(parsed.invoice_date, parsed.invoice_number, attachment.filename)
                    detail_drive = drive_client.ensure_file(
                        name=detail_name,
                        parent_id=parent_id,
                        content=content,
                        mime_type=mime_type_for(attachment.filename),
                    )
                    detail_files.append(
                        {
                            "kind": "DETAIL",
                            "original_filename": attachment.filename,
                            "stored_filename": detail_name,
                            "drive_file_id": str(detail_drive.get("id", "")),
                            "drive_url": str(detail_drive.get("webViewLink", "")),
                        }
                    )

                upsert_document_row(
                    cur,
                    supplier_id=supplier_id,
                    parsed=parsed,
                    windows_path=windows_path,
                    drive_url=str(moved_file.get("webViewLink", "")),
                    drive_file_id=str(moved_file.get("id", "")),
                    original_filename=original_name,
                    review_name=stored_name,
                    message=message,
                    detail_files=detail_files,
                )
                imported.append(final_name)
        conn.commit()

    print(f"Imported {len(imported)} DCT documents from review folder.")
    for name in imported:
        print(f"IMPORTED\t{name}")
    for name in skipped:
        print(f"SKIPPED\t{name}")
    return 0


def _is_dct_candidate(stored_name: str) -> bool:
    original = _original_name_from_review_name(stored_name).lower()
    return original.startswith("re_26-") and original.endswith(".pdf")


def _original_name_from_review_name(stored_name: str) -> str:
    parts = stored_name.split("_", 2)
    return parts[2] if len(parts) == 3 else stored_name


def find_matching_message(messages, original_filename: str):
    original_lower = original_filename.lower()
    for message in messages:
        if "dct.de" not in message.sender_email.lower():
            continue
        detail_attachments = []
        has_invoice = False
        for attachment in message.attachments:
            filename = attachment.filename.strip()
            lower = filename.lower()
            if lower == original_lower:
                has_invoice = True
            elif lower.endswith(".xls") or lower.endswith(".xlsx"):
                detail_attachments.append(attachment)
        if has_invoice:
            return message, detail_attachments
    raise LookupError(f"Could not find matching Gmail message for {original_filename}")


def build_base_filename(invoice_date, invoice_number: str) -> str:
    safe_invoice_number = invoice_number.replace("/", "-").replace("\\", "-").replace(" ", "")
    return f"DCT_{invoice_date.strftime('%Y%m%d')}_{safe_invoice_number}"


def build_detail_filename(invoice_date, invoice_number: str, original_filename: str) -> str:
    suffix = Path(original_filename).suffix.lower() or ".xls"
    return f"{build_base_filename(invoice_date, invoice_number)}_DETAIL{suffix}"


def mime_type_for(filename: str) -> str:
    if filename.lower().endswith(".xlsx"):
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    return "application/vnd.ms-excel"


def lookup_supplier_id(cursor, company_code: str, supplier_code: str) -> str | None:
    row = cursor.execute(
        f"SELECT id FROM {SCHEMA_NAME}.suppliers WHERE company_code = %s AND supplier_code = %s",
        (company_code, supplier_code),
    ).fetchone()
    return str(row[0]) if row else None


def document_exists(cursor, *, invoice_number: str) -> bool:
    row = cursor.execute(
        f"""
        SELECT 1
        FROM {SCHEMA_NAME}.documents
        WHERE company_code = %s
          AND supplier_code = %s
          AND invoice_number = %s
        LIMIT 1
        """,
        (COMPANY_CODE, "DCT", invoice_number),
    ).fetchone()
    return bool(row)


def upsert_document_row(
    cursor,
    *,
    supplier_id: str | None,
    parsed,
    windows_path: str,
    drive_url: str,
    drive_file_id: str,
    original_filename: str,
    review_name: str,
    message,
    detail_files: list[dict[str, object]],
) -> None:
    extracted_raw = json.dumps(
        {
            **parsed.extracted_raw,
            "detail_files": detail_files,
            "review_queue_name": review_name,
        }
    )
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
            message.received_at,
            message.sender_email,
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
            "invoice",
            "classified",
            "gmail",
            message.message_id,
            message.thread_id,
            original_filename,
            parsed.parser_name,
            parsed.parser_confidence,
            extracted_raw,
            "Imported manually from review/invoices queue with detail attachment.",
            message.sender_email,
            message.subject,
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
