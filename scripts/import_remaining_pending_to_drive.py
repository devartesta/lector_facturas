from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import psycopg

from lector_facturas.google_drive import DriveConfig, GoogleDriveClient, GoogleOAuthConfig
from lector_facturas.parsers.artesta_income import parse_qhands_pdf, parse_rappel_pdf
from lector_facturas.parsers.artlink import parse_artlink_pdf
from lector_facturas.parsers.openai import parse_openai_pdf
from lector_facturas.parsers.partner_income_fr import parse_choose_pdf, parse_toasty_pdf
from lector_facturas.parsers.portclearance import parse_portclearance_pdf
from lector_facturas.parsers.producthero import parse_producthero_pdf
from lector_facturas.parsers.railway import parse_railway_pdf
from lector_facturas.provider_catalog import ProviderRecord
from lector_facturas.review_workflow import company_folder_name, get_provider


SCHEMA_NAME = "invoices"
FINANCE_ROOT = Path(r"C:\Users\ADRISE~1\OneDrive - Artesta\ARTESTA - 6. Finances")
COMPANY_CODES = {
    "ARTESTA STORE, S.L.": "SL",
    "ARTESTA STORES (UK) LTD": "LTD",
    "ARTESTA INC": "INC",
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload pending 2026 invoices to Google Drive and Postgres.")
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
            for spec in build_specs():
                parsed = spec["parser"](spec["file"])
                provider = get_provider(spec["company"], parsed.supplier_code)
                ensure_supplier_row(cur, provider)
                supplier_id = lookup_supplier_id(cur, COMPANY_CODES[provider.company], parsed.supplier_code)
                extension = spec["file"].suffix.lower() or ".pdf"
                filename = f"{parsed.supplier_code}_{parsed.invoice_date.strftime('%Y%m%d')}_{parsed.invoice_number.replace('/', '-').replace('_', '-')}{extension}"
                windows_path = build_windows_path(provider.company, parsed.period_yyyymm, provider.destination_path, filename)
                parent_id = ensure_drive_path(client, args.root_folder_id, windows_path)
                drive_file = client.ensure_file(
                    name=filename,
                    parent_id=parent_id,
                    content=spec["file"].read_bytes(),
                    mime_type="application/pdf",
                )
                upsert_document_row(
                    cur,
                    company=provider.company,
                    supplier_id=supplier_id,
                    parsed=parsed,
                    windows_path=windows_path,
                    drive_url=str(drive_file.get("webViewLink", "")),
                    drive_file_id=str(drive_file.get("id", "")),
                    local_source_file=str(spec["file"]),
                    document_type=getattr(parsed, "document_type", "invoice"),
                    division_invoice=getattr(parsed, "division_invoice", ""),
                    note=spec["note"],
                )
                uploaded.append((provider.company, parsed.supplier_code, parsed.invoice_number))
        conn.commit()

    print(f"Uploaded {len(uploaded)} pending invoices.")
    for row in uploaded:
        print(" | ".join(row))
    return 0


def build_specs() -> list[dict[str, object]]:
    sl_jan = FINANCE_ROOT / "Artesta Store, S.L" / "2026" / "1Q" / "202601"
    sl_feb = FINANCE_ROOT / "Artesta Store, S.L" / "2026" / "1Q" / "202602"
    sl_mar = FINANCE_ROOT / "Artesta Store, S.L" / "2026" / "1Q" / "202603"
    ltd_mar = FINANCE_ROOT / "Artesta Stores (UK) Ltd" / "2026" / "1Q" / "202603"
    return [
        {"company": "ARTESTA STORE, S.L.", "parser": parse_openai_pdf, "file": sl_jan / "Gastos" / "Proveedores" / "chatgpt" / "Invoice-7BSDV5AM-0001.pdf", "note": "Imported pending OpenAI invoice."},
        {"company": "ARTESTA STORE, S.L.", "parser": parse_openai_pdf, "file": sl_jan / "Gastos" / "Proveedores" / "chatgpt" / "Invoice-BZHJNTUB-0001.pdf", "note": "Imported pending OpenAI invoice."},
        {"company": "ARTESTA STORE, S.L.", "parser": parse_openai_pdf, "file": sl_feb / "Gastos" / "Proveedores" / "CHATGPT" / "Invoice-BZHJNTUB-0003.pdf", "note": "Imported pending OpenAI invoice."},
        {"company": "ARTESTA STORE, S.L.", "parser": parse_openai_pdf, "file": sl_feb / "Gastos" / "Proveedores" / "CHATGPT" / "Invoice-BZHJNTUB-0005.pdf", "note": "Imported pending OpenAI invoice."},
        {"company": "ARTESTA STORE, S.L.", "parser": parse_openai_pdf, "file": sl_feb / "Gastos" / "Proveedores" / "CHATGPT" / "Invoice-BZHJNTUB-0006.pdf", "note": "Imported pending OpenAI invoice."},
        {"company": "ARTESTA STORE, S.L.", "parser": parse_openai_pdf, "file": sl_feb / "Gastos" / "Proveedores" / "CHATGPT" / "Receipt-2699-8822-5500.pdf", "note": "Imported pending OpenAI receipt."},
        {"company": "ARTESTA STORE, S.L.", "parser": parse_railway_pdf, "file": sl_feb / "Gastos" / "Proveedores" / "RAILWAY" / "Invoice-1602C2F5-0016.pdf", "note": "Imported pending Railway invoice."},
        {"company": "ARTESTA STORE, S.L.", "parser": parse_producthero_pdf, "file": sl_feb / "Gastos" / "Proveedores" / "Product Hero" / "invoice_211723.pdf", "note": "Imported pending Producthero invoice."},
        {"company": "ARTESTA STORE, S.L.", "parser": parse_qhands_pdf, "file": sl_feb / "Ingresos" / "QHANDS" / "Factura_2026-0012.pdf", "note": "Imported pending outgoing invoice to QHANDS."},
        {"company": "ARTESTA STORE, S.L.", "parser": parse_toasty_pdf, "file": sl_feb / "Ingresos" / "TOASTY" / "invoice-AS-99158.pdf", "note": "Imported pending outgoing invoice to TOASTY."},
        {"company": "ARTESTA STORE, S.L.", "parser": parse_choose_pdf, "file": sl_mar / "Ingresos" / "CHOOSE" / "invoice-AS-101940.pdf", "note": "Imported pending outgoing invoice to CHOOSE."},
        {"company": "ARTESTA STORE, S.L.", "parser": parse_rappel_pdf, "file": sl_jan / "Ingresos" / "709. Rappels" / "Factura_A_2026-0006.pdf", "note": "Imported pending rappel invoice to Livitum."},
        {"company": "ARTESTA STORE, S.L.", "parser": parse_artlink_pdf, "file": sl_mar / "Gastos" / "Proveedores" / "Artlink" / "Artesta Store S.L invoice no 000203570 (Freight cost to order DCT 3001583).pdf", "note": "Imported pending Artlink freight invoice."},
        {"company": "ARTESTA STORES (UK) LTD", "parser": parse_portclearance_pdf, "file": ltd_mar / "Cost of sales" / "port clearence" / "INVOICE-PCSI2601529.pdf", "note": "Imported pending Port Clearance invoice."},
    ]


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


def upsert_document_row(
    cursor,
    *,
    company: str,
    supplier_id: str | None,
    parsed,
    windows_path: str,
    drive_url: str,
    drive_file_id: str,
    local_source_file: str,
    document_type: str,
    division_invoice: str,
    note: str,
) -> None:
    company_code = COMPANY_CODES[company]
    cursor.execute(
        f"SELECT id FROM {SCHEMA_NAME}.documents WHERE company_code = %s AND supplier_code = %s AND invoice_number = %s AND division_invoice = %s",
        (company_code, parsed.supplier_code, parsed.invoice_number, division_invoice),
    )
    existing = cursor.fetchone()
    extracted_raw = json.dumps({**parsed.extracted_raw, "local_source_file": local_source_file})
    payload = (
        parsed.invoice_number, parsed.invoice_date, parsed.issuer_company_name, parsed.billed_company_name, parsed.supplier_name,
        company_code, windows_path, drive_url, None, parsed.sender_email, parsed.original_filename, division_invoice,
        parsed.billing_period_start, parsed.billing_period_end, parsed.vat_percent, parsed.gross_amount, parsed.vat_amount, parsed.net_amount,
        supplier_id, parsed.supplier_code, parsed.currency_code, drive_file_id, "GOOGLE_DRIVE", document_type, "classified", "import", "", "",
        parsed.original_filename, parsed.parser_name, parsed.parser_confidence, extracted_raw, note,
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
    return "\\".join(["ARTESTA - 6. Finances", company_folder_name(company), period_yyyymm[:4], period_yyyymm, *Path(destination_path).parts, filename])


def ensure_drive_path(client: GoogleDriveClient, root_folder_id: str, windows_path: str) -> str:
    parts = windows_path.split("\\")
    parent_id = root_folder_id
    for folder_name in parts[1:-1]:
        folder = client.ensure_folder(name=folder_name, parent_id=parent_id)
        parent_id = str(folder["id"])
    return parent_id


if __name__ == "__main__":
    raise SystemExit(main())
