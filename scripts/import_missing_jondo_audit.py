from __future__ import annotations

import json
import os
import sys
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import psycopg

from lector_facturas.google_drive import GoogleDriveClient
from lector_facturas.parsers.jondo import parse_jondo_pdf
from lector_facturas.review_workflow import company_folder_name, get_provider
from lector_facturas.settings import load_settings


SCHEMA = "invoices"


def main() -> int:
    settings = load_settings()
    client = GoogleDriveClient(settings.to_drive_config())
    missing = json.loads(Path("tmp/jondo_filename_missing.json").read_text(encoding="utf-8"))
    done = 0
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        for _, _, _, path_s in missing:
            path = Path(path_s)
            parsed = parse_jondo_pdf(path)
            company_code = "INC" if "ARTESTA INC" in parsed.billed_company_name.upper() else "LTD"
            company = {"INC": "ARTESTA INC", "LTD": "ARTESTA STORES (UK) LTD"}[company_code]
            destination_path = _destination_path(company_code, company)
            with conn.cursor() as cur:
                supplier_id = _ensure_supplier(cur, company_code, destination_path)
                filename = f"JONDO_{parsed.invoice_date:%Y%m%d}_{parsed.invoice_number}.pdf"
                windows_path = _windows_path(company, parsed.period_yyyymm, destination_path, filename)
                parent_id = _ensure_drive_path(client, settings.drive_root_folder_id, windows_path)
                drive_file = client.ensure_file(
                    name=filename,
                    parent_id=parent_id,
                    content=path.read_bytes(),
                    mime_type="application/pdf",
                )
                action = _upsert_document(
                    cur,
                    company_code=company_code,
                    supplier_id=supplier_id,
                    parsed=parsed,
                    windows_path=windows_path,
                    drive_url=str(drive_file.get("webViewLink", "")),
                    drive_file_id=str(drive_file.get("id", "")),
                    local_source_file=str(path),
                )
            conn.commit()
            done += 1
            print(f"{done}/{len(missing)} {action} {company_code} {parsed.period_yyyymm} {parsed.invoice_number}", flush=True)
    return 0


def _destination_path(company_code: str, company: str) -> str:
    if company_code == "INC":
        return "expenses/cogs/manufacturing"
    return get_provider(company, "JONDO").destination_path


def _ensure_supplier(cursor, company_code: str, destination_path: str) -> str:
    row = cursor.execute(
        f"SELECT id FROM {SCHEMA}.suppliers WHERE company_code = %s AND supplier_code = 'JONDO'",
        (company_code,),
    ).fetchone()
    if row:
        return str(row[0])
    supplier_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"suppliers:{company_code}:JONDO"))
    cursor.execute(
        f"""
        INSERT INTO {SCHEMA}.suppliers (
            id, company_code, current_folder, supplier_code, supplier_name,
            billing_company_name, destination_path, is_active, notes, sender_emails
        ) VALUES (
            %s, %s, 'JONDO', 'JONDO', 'JONDO',
            'JONDO', %s, TRUE, 'Created by JONDO audit import.', '[]'::jsonb
        )
        ON CONFLICT (company_code, supplier_code) DO UPDATE SET
            destination_path = EXCLUDED.destination_path,
            is_active = TRUE,
            updated_at = NOW()
        """,
        (supplier_id, company_code, destination_path),
    )
    return supplier_id


def _windows_path(company: str, period_yyyymm: str, destination_path: str, filename: str) -> str:
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


def _ensure_drive_path(client: GoogleDriveClient, root_folder_id: str, windows_path: str) -> str:
    parts = windows_path.split("\\")
    parent_id = root_folder_id
    for folder_name in parts[1:-1]:
        folder = client.ensure_folder(name=folder_name, parent_id=parent_id)
        parent_id = str(folder["id"])
    return parent_id


def _upsert_document(
    cursor,
    *,
    company_code: str,
    supplier_id: str,
    parsed,
    windows_path: str,
    drive_url: str,
    drive_file_id: str,
    local_source_file: str,
) -> str:
    existing = cursor.execute(
        f"""
        SELECT id FROM {SCHEMA}.documents
        WHERE company_code = %s AND supplier_code = 'JONDO' AND invoice_number = %s AND division_invoice = ''
        """,
        (company_code, parsed.invoice_number),
    ).fetchone()
    extracted_raw = json.dumps({**parsed.extracted_raw, "local_source_file": local_source_file}, ensure_ascii=True)
    payload = (
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
        parsed.original_filename,
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
        parsed.original_filename,
        parsed.parser_name,
        parsed.parser_confidence,
        extracted_raw,
        "Imported missing JONDO invoice from finance folder audit.",
        parsed.sender_email,
        parsed.original_filename,
        parsed.period_yyyymm,
    )
    if existing:
        cursor.execute(
            f"""UPDATE {SCHEMA}.documents SET
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
        return "updated"
    cursor.execute(
        f"""INSERT INTO {SCHEMA}.documents (
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
    return "inserted"


if __name__ == "__main__":
    raise SystemExit(main())
