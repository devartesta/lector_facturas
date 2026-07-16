from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import psycopg
from pypdf import PdfReader

from lector_facturas.google_drive import GoogleDriveClient
from lector_facturas.parsers.artesta_income import parse_qhands_pdf
from lector_facturas.review_workflow import company_folder_name, get_provider
from lector_facturas.settings import load_settings


SCHEMA_NAME = "invoices"
COMPANY = "ARTESTA STORE, S.L."
COMPANY_CODE = "SL"
SUPPLIER_CODE = "QHANDS"


def main() -> int:
    settings = load_settings()
    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not configured.")
    if not settings.google_oauth_ready or not settings.drive_root_folder_id:
        raise RuntimeError("Google Drive settings are incomplete.")

    provider = get_provider(COMPANY, SUPPLIER_CODE)
    client = GoogleDriveClient(settings.to_drive_config())
    manifest = json.loads(Path("tmp/qhands_mail_search/download_manifest.json").read_text(encoding="utf-8"))
    specs = build_qhands_specs(manifest)
    if not specs:
        print("No QHANDS invoices found in downloaded mail attachments.")
        return 0

    uploaded: list[tuple[str, str, str, str]] = []
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            ensure_supplier_row(cur, provider)
            supplier_id = lookup_supplier_id(cur)
            for spec in specs:
                pdf_path = Path(spec["path"])
                parsed = parse_qhands_pdf(pdf_path)
                filename = f"{parsed.supplier_code}_{parsed.invoice_date:%Y%m%d}_{parsed.invoice_number.replace('/', '-').replace('_', '-')}.pdf"
                windows_path = build_windows_path(COMPANY, parsed.period_yyyymm, provider.destination_path, filename)
                parent_id = ensure_drive_path(client, settings.drive_root_folder_id, windows_path)
                drive_file = client.ensure_file(
                    name=filename,
                    parent_id=parent_id,
                    content=pdf_path.read_bytes(),
                    mime_type="application/pdf",
                )
                upsert_document_row(
                    cur,
                    supplier_id=supplier_id,
                    parsed=parsed,
                    windows_path=windows_path,
                    drive_url=str(drive_file.get("webViewLink", "")),
                    drive_file_id=str(drive_file.get("id", "")),
                    local_source_file=str(pdf_path),
                    mail=spec,
                )
                uploaded.append(
                    (
                        parsed.invoice_number,
                        parsed.period_yyyymm,
                        str(drive_file.get("id", "")),
                        str(drive_file.get("webViewLink", "")),
                    )
                )
        conn.commit()

    print(f"Upserted {len(uploaded)} QHANDS invoices.")
    for row in uploaded:
        print(" | ".join(row))
    return 0


def build_qhands_specs(manifest: list[dict[str, object]]) -> list[dict[str, object]]:
    specs: dict[str, dict[str, object]] = {}
    for item in manifest:
        filename = str(item.get("filename", ""))
        path = Path(str(item.get("path", "")))
        if not filename.startswith("Factura_2026") or not path.exists():
            continue
        text = "\n".join((page.extract_text() or "") for page in PdfReader(str(path)).pages)
        if "QHANDS" not in text.upper():
            continue
        parsed = parse_qhands_pdf(path)
        current = {**item, "path": str(path)}
        specs[parsed.invoice_number] = current
    return [specs[key] for key in sorted(specs)]


def ensure_supplier_row(cursor, provider) -> None:
    supplier_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"suppliers:{COMPANY_CODE}:{provider.supplier_code}"))
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
            COMPANY_CODE,
            provider.current_folder,
            provider.supplier_code,
            provider.supplier_code,
            provider.provider_name,
            provider.destination_path,
            provider.notes,
            json.dumps(list(provider.sender_emails)),
        ),
    )


def lookup_supplier_id(cursor) -> str | None:
    row = cursor.execute(
        f"SELECT id FROM {SCHEMA_NAME}.suppliers WHERE company_code = %s AND supplier_code = %s",
        (COMPANY_CODE, SUPPLIER_CODE),
    ).fetchone()
    return str(row[0]) if row else None


def upsert_document_row(
    cursor,
    *,
    supplier_id: str | None,
    parsed,
    windows_path: str,
    drive_url: str,
    drive_file_id: str,
    local_source_file: str,
    mail: dict[str, object],
) -> None:
    cursor.execute(
        f"SELECT id FROM {SCHEMA_NAME}.documents WHERE company_code = %s AND supplier_code = %s AND invoice_number = %s AND division_invoice = %s",
        (COMPANY_CODE, parsed.supplier_code, parsed.invoice_number, parsed.division_invoice),
    )
    existing = cursor.fetchone()
    extracted_raw = json.dumps(
        {
            **parsed.extracted_raw,
            "local_source_file": local_source_file,
            "source_email_date": str(mail.get("date", "")),
            "source_email_sender": str(mail.get("from", "")),
            "source_email_subject": str(mail.get("subject", "")),
            "source_email_message_id": str(mail.get("message_id", "")),
            "source_email_thread_id": str(mail.get("thread_id", "")),
        },
        ensure_ascii=True,
    )
    payload = (
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
        parsed.original_filename,
        parsed.division_invoice,
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
        str(mail.get("message_id", "")),
        str(mail.get("thread_id", "")),
        parsed.original_filename,
        parsed.parser_name,
        parsed.parser_confidence,
        extracted_raw,
        "Imported/reconciled QHANDS outgoing CNC rental invoice from Gmail attachment.",
        str(mail.get("from", parsed.sender_email)),
        str(mail.get("subject", parsed.original_filename)),
        parsed.period_yyyymm,
    )
    if existing:
        cursor.execute(
            f"""UPDATE {SCHEMA_NAME}.documents SET
                invoice_date=%s, issuer_company_name=%s, billed_company_name=%s, supplier_name=%s, company_code=%s,
                windows_path=%s, drive_url=%s, received_at=%s, sender_email=%s, original_filename=%s, division_invoice=%s,
                billing_period_start=%s, billing_period_end=%s, vat_percent=%s, gross_amount=%s, vat_amount=%s, net_amount=%s,
                supplier_id=%s, supplier_code=%s, currency_code=%s, drive_file_id=%s, storage_root=%s, document_type=%s,
                status=%s, source_channel=%s, email_message_id=%s, email_thread_id=%s, attachment_original_name=%s, parser_name=%s,
                parser_confidence=%s, extracted_raw=%s::jsonb, review_notes=%s, source_sender=%s, source_subject=%s, period_yyyymm=%s,
                updated_at=NOW()
               WHERE id=%s""",
            payload[1:] + (str(existing[0]),),
        )
        return

    cursor.execute(
        f"""INSERT INTO {SCHEMA_NAME}.documents (
            id, invoice_number, invoice_date, issuer_company_name, billed_company_name, supplier_name, company_code, windows_path, drive_url,
            received_at, sender_email, original_filename, division_invoice, billing_period_start, billing_period_end, vat_percent, gross_amount,
            vat_amount, net_amount, supplier_id, supplier_code, currency_code, drive_file_id, storage_root, document_type, status, source_channel,
            email_message_id, email_thread_id, attachment_original_name, parser_name, parser_confidence, extracted_raw, review_notes,
            created_at, updated_at, source_sender, source_subject, period_yyyymm
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s,
            NOW(), NOW(), %s, %s, %s
        )""",
        (str(uuid.uuid4()),) + payload,
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
