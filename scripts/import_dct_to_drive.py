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
from lector_facturas.parsers.dct import parse_dct_pdf
from lector_facturas.review_workflow import company_folder_name, get_provider


SCHEMA_NAME = "invoices"
FINANCE_ROOT = Path(r"C:\Users\ADRISE~1\OneDrive - Artesta\ARTESTA - 6. Finances")


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload DCT invoices and detail files to Google Drive and register them in Postgres.")
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

    bundles = discover_dct_bundles()
    uploaded: list[tuple[str, str, str]] = []
    with psycopg.connect(args.database_url) as conn:
        with conn.cursor() as cur:
            for bundle in bundles:
                parsed = parse_dct_pdf(bundle["invoice_pdf"])
                provider = get_provider("ARTESTA STORE, S.L.", parsed.supplier_code)
                base_filename = build_base_filename(parsed.supplier_code, parsed.invoice_date, parsed.invoice_number)
                pdf_filename = f"{base_filename}.pdf"
                windows_path = build_windows_path(provider.company, parsed.period_yyyymm, provider.destination_path, pdf_filename)
                parent_id = ensure_drive_path(client, args.root_folder_id, windows_path)
                pdf_drive = client.ensure_file(
                    name=pdf_filename,
                    parent_id=parent_id,
                    content=bundle["invoice_pdf"].read_bytes(),
                    mime_type="application/pdf",
                )
                detail_files = []
                for detail_suffix, detail_path in bundle["details"]:
                    detail_name = f"{base_filename}_{detail_suffix}{detail_path.suffix.lower()}"
                    detail_drive = client.ensure_file(
                        name=detail_name,
                        parent_id=parent_id,
                        content=detail_path.read_bytes(),
                        mime_type=mime_type_for(detail_path),
                    )
                    detail_files.append(
                        {
                            "kind": detail_suffix,
                            "original_filename": detail_path.name,
                            "stored_filename": detail_name,
                            "drive_file_id": str(detail_drive.get("id", "")),
                            "drive_url": str(detail_drive.get("webViewLink", "")),
                        }
                    )
                supplier_id = lookup_supplier_id(cur, "SL", parsed.supplier_code)
                cur.execute(
                    f"SELECT id FROM {SCHEMA_NAME}.documents WHERE company_code = %s AND supplier_code = %s AND invoice_number = %s",
                    ("SL", parsed.supplier_code, parsed.invoice_number),
                )
                existing = cur.fetchone()
                payload = (
                    parsed.invoice_number,
                    parsed.invoice_date,
                    parsed.issuer_company_name,
                    parsed.billed_company_name,
                    parsed.supplier_name,
                    "SL",
                    windows_path,
                    str(pdf_drive.get("webViewLink", "")),
                    None,
                    "",
                    parsed.original_filename,
                    parsed.billing_period_start,
                    parsed.billing_period_end,
                    parsed.vat_percent,
                    parsed.gross_amount,
                    parsed.vat_amount,
                    parsed.net_amount,
                    supplier_id,
                    parsed.supplier_code,
                    parsed.currency_code,
                    str(pdf_drive.get("id", "")),
                    "GOOGLE_DRIVE",
                    "invoice",
                    "classified",
                    "import",
                    "",
                    "",
                    parsed.original_filename,
                    parsed.parser_name,
                    parsed.parser_confidence,
                    json.dumps(
                        {
                            **parsed.extracted_raw,
                            "detail_files": detail_files,
                            "local_source_file": str(bundle["invoice_pdf"]),
                        }
                    ),
                    "Imported from historical DCT examples by parser.",
                    "",
                    parsed.original_filename,
                    parsed.period_yyyymm,
                )
                if existing:
                    cur.execute(
                        f"""
                        UPDATE {SCHEMA_NAME}.documents
                        SET invoice_date = %s,
                            issuer_company_name = %s,
                            billed_company_name = %s,
                            supplier_name = %s,
                            company_code = %s,
                            windows_path = %s,
                            drive_url = %s,
                            received_at = %s,
                            sender_email = %s,
                            original_filename = %s,
                            billing_period_start = %s,
                            billing_period_end = %s,
                            vat_percent = %s,
                            gross_amount = %s,
                            vat_amount = %s,
                            net_amount = %s,
                            supplier_id = %s,
                            supplier_code = %s,
                            currency_code = %s,
                            drive_file_id = %s,
                            storage_root = %s,
                            document_type = %s,
                            status = %s,
                            source_channel = %s,
                            email_message_id = %s,
                            email_thread_id = %s,
                            attachment_original_name = %s,
                            parser_name = %s,
                            parser_confidence = %s,
                            extracted_raw = %s::jsonb,
                            review_notes = %s,
                            source_sender = %s,
                            source_subject = %s,
                            period_yyyymm = %s,
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        payload + (existing[0],),
                    )
                else:
                    cur.execute(
                        f"""
                        INSERT INTO {SCHEMA_NAME}.documents (
                            id, invoice_number, invoice_date, issuer_company_name, billed_company_name,
                            supplier_name, company_code, windows_path, drive_url, received_at, sender_email,
                            original_filename, billing_period_start, billing_period_end, vat_percent, gross_amount,
                            vat_amount, net_amount, supplier_id, supplier_code, currency_code, drive_file_id,
                            storage_root, document_type, status, source_channel, email_message_id, email_thread_id,
                            attachment_original_name, parser_name, parser_confidence, extracted_raw, review_notes,
                            created_at, updated_at, source_sender, source_subject, period_yyyymm
                        ) VALUES (
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s::jsonb, %s,
                            NOW(), NOW(), %s, %s, %s
                        )
                        """,
                        (str(uuid.uuid4()),) + payload,
                    )
                uploaded.append((parsed.invoice_number, parsed.supplier_code, str(pdf_drive.get("webViewLink", ""))))
        conn.commit()

    print(f"Uploaded {len(uploaded)} DCT invoices to Google Drive.")
    for invoice_number, supplier_code, drive_url in uploaded:
        print(f"- {supplier_code} | {invoice_number} | {drive_url}")
    return 0


def discover_dct_bundles() -> list[dict[str, object]]:
    january = FINANCE_ROOT / "Artesta Store, S.L" / "2026" / "1Q" / "202601" / "Gastos" / "Proveedores" / "DCT"
    february = FINANCE_ROOT / "Artesta Store, S.L" / "2026" / "1Q" / "202602" / "Gastos" / "Proveedores" / "DCT"
    return [
        {
            "invoice_pdf": january / "RE_26-0045.pdf",
            "details": [("DETAIL", january / "ArtestaAT_03-2026.xls")],
        },
        {
            "invoice_pdf": january / "RE_26-0166.pdf",
            "details": [("DETAIL", january / "ArtestaAT_05-2026.xls")],
        },
        {
            "invoice_pdf": january / "RE_26-0167.pdf",
            "details": [("DETAIL_2NDSHIPPING", january / "ErneuterVersand 31.01.2026.xlsx")],
        },
        {
            "invoice_pdf": february / "RE_26-0281.pdf",
            "details": [("DETAIL", february / "ArtestaAT_07-2026.xls")],
        },
        {
            "invoice_pdf": february / "RE_26-0364.pdf",
            "details": [("DETAIL", february / "ArtestaAT_09-2026.xls")],
        },
        {
            "invoice_pdf": february / "RE_26-0365.pdf",
            "details": [("DETAIL_2NDSHIPPING", february / "ErneuterVersand 28.02.2026.xlsx")],
        },
    ]


def build_base_filename(supplier_code: str, invoice_date, invoice_number: str) -> str:
    safe_invoice_number = invoice_number.replace("/", "-").replace("\\", "-").replace(" ", "")
    return f"{supplier_code}_{invoice_date.strftime('%Y%m%d')}_{safe_invoice_number}"


def mime_type_for(path: Path) -> str:
    if path.suffix.lower() == ".xlsx":
        return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    return "application/vnd.ms-excel"


def lookup_supplier_id(cursor, company_code: str, supplier_code: str) -> str | None:
    row = cursor.execute(
        f"SELECT id FROM {SCHEMA_NAME}.suppliers WHERE company_code = %s AND supplier_code = %s",
        (company_code, supplier_code),
    ).fetchone()
    return str(row[0]) if row else None


def build_windows_path(company: str, period_yyyymm: str, destination_path: str, filename: str) -> str:
    parts = [
        "ARTESTA - 6. Finances",
        company_folder_name(company),
        period_yyyymm[:4],
        period_yyyymm,
        *Path(destination_path).parts,
        filename,
    ]
    return "\\".join(parts)


def ensure_drive_path(client: GoogleDriveClient, root_folder_id: str, windows_path: str) -> str:
    parts = windows_path.split("\\")
    folder_parts = parts[1:-1]
    parent_id = root_folder_id
    for folder_name in folder_parts:
        folder = client.ensure_folder(name=folder_name, parent_id=parent_id)
        parent_id = str(folder["id"])
    return parent_id


if __name__ == "__main__":
    raise SystemExit(main())
