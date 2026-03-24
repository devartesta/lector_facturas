from __future__ import annotations

import argparse
from pathlib import Path
import json
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import psycopg

from lector_facturas.google_drive import DriveConfig, GoogleDriveClient, GoogleOAuthConfig
from lector_facturas.parsers.youraccountstaxes import parse_youraccountstaxes_pdf
from lector_facturas.review_workflow import company_folder_name, get_provider


SCHEMA_NAME = "invoices"
FINANCE_ROOT = Path(r"C:\Users\ADRISE~1\OneDrive - Artesta\ARTESTA - 6. Finances")


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload Your Accounts and Taxes invoices to Google Drive and register them in Postgres.")
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

    invoice_files = discover_invoice_pdfs()
    uploaded: list[tuple[str, str, str]] = []
    with psycopg.connect(args.database_url) as conn:
        with conn.cursor() as cur:
            for invoice_pdf in invoice_files:
                parsed = parse_youraccountstaxes_pdf(invoice_pdf)
                provider = get_provider("ARTESTA STORES (UK) LTD", parsed.supplier_code)
                base_filename = build_base_filename(parsed.supplier_code, parsed.invoice_date, parsed.invoice_number)
                pdf_filename = f"{base_filename}.pdf"
                windows_path = build_windows_path(provider.company, parsed.period_yyyymm, provider.destination_path, pdf_filename)
                parent_id = ensure_drive_path(client, args.root_folder_id, windows_path)
                pdf_drive = client.ensure_file(
                    name=pdf_filename,
                    parent_id=parent_id,
                    content=invoice_pdf.read_bytes(),
                    mime_type="application/pdf",
                )
                supplier_id = lookup_supplier_id(cur, "LTD", parsed.supplier_code)
                upsert_document_row(
                    cur,
                    company_code="LTD",
                    supplier_id=supplier_id,
                    parsed=parsed,
                    windows_path=windows_path,
                    drive_url=str(pdf_drive.get("webViewLink", "")),
                    drive_file_id=str(pdf_drive.get("id", "")),
                    local_source_file=str(invoice_pdf),
                )
                uploaded.append((parsed.supplier_code, parsed.invoice_number, str(pdf_drive.get("webViewLink", ""))))
        conn.commit()

    print(f"Uploaded {len(uploaded)} Your Accounts and Taxes invoices to Google Drive.")
    for supplier_code, invoice_number, drive_url in uploaded:
        print(f"- {supplier_code} | {invoice_number} | {drive_url}")
    return 0


def discover_invoice_pdfs() -> list[Path]:
    folder = FINANCE_ROOT / "Artesta Stores (UK) Ltd" / "2025" / "4Q" / "202512" / "Operating Expenses" / "Your Accounts and Taxes"
    files = list(folder.glob("*.pdf")) + list(folder.glob("*.PDF"))
    deduped: dict[str, Path] = {}
    for file_path in files:
        if not file_path.name.lower().startswith("invoice "):
            continue
        deduped[str(file_path).lower()] = file_path
    unique_by_invoice: dict[str, Path] = {}
    for file_path in sorted(deduped.values()):
        try:
            invoice_number = parse_youraccountstaxes_pdf(file_path).invoice_number
        except Exception:
            invoice_number = file_path.name.lower()
        unique_by_invoice[invoice_number] = file_path
    return sorted(unique_by_invoice.values())


def build_base_filename(supplier_code: str, invoice_date, invoice_number: str) -> str:
    safe_invoice_number = invoice_number.replace("/", "-")
    return f"{supplier_code}_{invoice_date.strftime('%Y%m%d')}_{safe_invoice_number}"


def lookup_supplier_id(cursor, company_code: str, supplier_code: str) -> str | None:
    row = cursor.execute(
        f"SELECT id FROM {SCHEMA_NAME}.suppliers WHERE company_code = %s AND supplier_code = %s",
        (company_code, supplier_code),
    ).fetchone()
    return str(row[0]) if row else None


def upsert_document_row(cursor, *, company_code: str, supplier_id: str | None, parsed, windows_path: str, drive_url: str, drive_file_id: str, local_source_file: str) -> None:
    cursor.execute(
        f"SELECT id FROM {SCHEMA_NAME}.documents WHERE company_code = %s AND supplier_code = %s AND invoice_number = %s AND division_invoice = %s",
        (company_code, parsed.supplier_code, parsed.invoice_number, ""),
    )
    existing = cursor.fetchone()
    extracted_raw = json.dumps({**parsed.extracted_raw, "local_source_file": local_source_file})
    payload = (
        parsed.invoice_number, parsed.invoice_date, parsed.issuer_company_name, parsed.billed_company_name, parsed.supplier_name,
        company_code, windows_path, drive_url, None, parsed.sender_email, parsed.original_filename, "",
        parsed.billing_period_start, parsed.billing_period_end, parsed.vat_percent, parsed.gross_amount, parsed.vat_amount, parsed.net_amount,
        supplier_id, parsed.supplier_code, parsed.currency_code, drive_file_id, "GOOGLE_DRIVE", "invoice", "classified", "import", "", "",
        parsed.original_filename, parsed.parser_name, parsed.parser_confidence, extracted_raw, "Imported from historical Your Accounts and Taxes examples by parser.",
        parsed.sender_email, parsed.original_filename, parsed.period_yyyymm,
    )
    update_payload = payload[1:]
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
            update_payload + (str(existing[0]),),
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
    parts = ["ARTESTA - 6. Finances", company_folder_name(company), period_yyyymm[:4], period_yyyymm, *Path(destination_path).parts, filename]
    return "\\".join(parts)


def ensure_drive_path(client: GoogleDriveClient, root_folder_id: str, windows_path: str) -> str:
    parts = windows_path.split("\\")
    parent_id = root_folder_id
    for folder_name in parts[1:-1]:
        folder = client.ensure_folder(name=folder_name, parent_id=parent_id)
        parent_id = str(folder["id"])
    return parent_id


if __name__ == "__main__":
    raise SystemExit(main())
