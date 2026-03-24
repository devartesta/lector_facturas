from __future__ import annotations

import argparse
from dataclasses import dataclass
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
from lector_facturas.parsers.marketing_ads import (
    MarketingInvoiceDivision,
    parse_google_ads_pdf,
    parse_meta_ads_pdf,
    read_pdf_text,
)
from lector_facturas.review_workflow import company_folder_name, get_provider


SCHEMA_NAME = "invoices"
FINANCE_ROOT = Path(r"C:\Users\ADRISE~1\OneDrive - Artesta\ARTESTA - 6. Finances")


@dataclass(frozen=True)
class ImportBundle:
    provider_company: str
    supplier_code: str
    pdf_path: Path
    parsed_rows: list[MarketingInvoiceDivision]
    detail_files: list[tuple[str, Path]]


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload Google Ads and Meta Ads invoices to Google Drive and register them in Postgres.")
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

    bundles = discover_marketing_bundles()
    uploaded: list[tuple[str, str, str, str]] = []
    with psycopg.connect(args.database_url) as conn:
        with conn.cursor() as cur:
            for bundle in bundles:
                provider = get_provider(bundle.provider_company, bundle.supplier_code)
                first_row = bundle.parsed_rows[0]
                base_filename = build_base_filename(bundle.supplier_code, first_row.invoice_date, first_row.invoice_number)
                pdf_filename = f"{base_filename}.pdf"
                windows_path = build_windows_path(provider.company, first_row.period_yyyymm, provider.destination_path, pdf_filename)
                parent_id = ensure_drive_path(client, args.root_folder_id, windows_path)
                pdf_drive = client.ensure_file(
                    name=pdf_filename,
                    parent_id=parent_id,
                    content=bundle.pdf_path.read_bytes(),
                    mime_type="application/pdf",
                )
                detail_entries: list[dict[str, str]] = []
                for detail_name, detail_path in bundle.detail_files:
                    detail_filename = f"{base_filename}_{detail_name}{detail_path.suffix.lower()}"
                    detail_drive = client.ensure_file(
                        name=detail_filename,
                        parent_id=parent_id,
                        content=detail_path.read_bytes(),
                        mime_type="application/pdf",
                    )
                    detail_entries.append(
                        {
                            "name": detail_filename,
                            "drive_file_id": str(detail_drive.get("id", "")),
                            "drive_url": str(detail_drive.get("webViewLink", "")),
                        }
                    )
                supplier_id = lookup_supplier_id(cur, "SL", bundle.supplier_code)
                for row in bundle.parsed_rows:
                    upsert_document_row(
                        cur,
                        supplier_id=supplier_id,
                        parsed=row,
                        windows_path=windows_path,
                        drive_url=str(pdf_drive.get("webViewLink", "")),
                        drive_file_id=str(pdf_drive.get("id", "")),
                        local_source_file=str(bundle.pdf_path),
                        detail_entries=detail_entries,
                    )
                    uploaded.append((bundle.supplier_code, row.invoice_number, row.division_invoice, str(pdf_drive.get("webViewLink", ""))))
        conn.commit()

    print(f"Uploaded {len(uploaded)} marketing document rows to Google Drive/Postgres.")
    for supplier_code, invoice_number, division, drive_url in uploaded:
        print(f"- {supplier_code} | {invoice_number} | {division} | {drive_url}")
    return 0


def discover_marketing_bundles() -> list[ImportBundle]:
    bundles: list[ImportBundle] = []
    google_candidates = [
        FINANCE_ROOT / "Artesta Store, S.L" / "2026" / "1Q" / "202601" / "Gastos" / "Proveedores" / "Google" / "5489506571 (1).pdf",
        FINANCE_ROOT / "Artesta Store, S.L" / "2026" / "1Q" / "202602" / "Gastos" / "Proveedores" / "google" / "5509802536.pdf",
    ]
    for pdf_path in google_candidates:
        if not pdf_path.exists():
            continue
        parsed_rows = parse_google_ads_pdf(pdf_path)
        bundles.append(
            ImportBundle(
                provider_company="ARTESTA STORE, S.L.",
                supplier_code="GOOGLEADS",
                pdf_path=pdf_path,
                parsed_rows=parsed_rows,
                detail_files=[],
            )
        )

    meta_candidates = [
        (
            FINANCE_ROOT / "Artesta Store, S.L" / "2026" / "1Q" / "202601" / "Gastos" / "Proveedores" / "Meta" / "Transaction_249902419.pdf",
            FINANCE_ROOT / "Artesta Store, S.L" / "2026" / "1Q" / "202601" / "Gastos" / "Proveedores" / "Meta" / "249902419pay.pdf",
        ),
        (
            FINANCE_ROOT / "Artesta Store, S.L" / "2026" / "1Q" / "202603" / "Gastos" / "Proveedores" / "META" / "Transaction_251380763.pdf",
            None,
        ),
    ]
    for invoice_pdf, payment_pdf in meta_candidates:
        if not invoice_pdf.exists():
            continue
        parsed_rows = parse_meta_ads_pdf(invoice_pdf)
        period = parsed_rows[0].period_yyyymm
        if period not in {"202601", "202602"}:
            continue
        detail_files: list[tuple[str, Path]] = []
        if payment_pdf and payment_pdf.exists():
            detail_files.append(("PAYMENT", payment_pdf))
        bundles.append(
            ImportBundle(
                provider_company="ARTESTA STORE, S.L.",
                supplier_code="METAADS",
                pdf_path=invoice_pdf,
                parsed_rows=parsed_rows,
                detail_files=detail_files,
            )
        )
    return bundles


def build_base_filename(supplier_code: str, invoice_date, invoice_number: str) -> str:
    return f"{supplier_code}_{invoice_date.strftime('%Y%m%d')}_{invoice_number}"


def lookup_supplier_id(cursor, company_code: str, supplier_code: str) -> str | None:
    row = cursor.execute(
        f"SELECT id FROM {SCHEMA_NAME}.suppliers WHERE company_code = %s AND supplier_code = %s",
        (company_code, supplier_code),
    ).fetchone()
    return str(row[0]) if row else None


def upsert_document_row(
    cursor,
    *,
    supplier_id: str | None,
    parsed: MarketingInvoiceDivision,
    windows_path: str,
    drive_url: str,
    drive_file_id: str,
    local_source_file: str,
    detail_entries: list[dict[str, str]],
) -> None:
    cursor.execute(
        f"""
        SELECT id FROM {SCHEMA_NAME}.documents
        WHERE company_code = %s AND supplier_code = %s AND invoice_number = %s AND division_invoice = %s
        """,
        ("SL", parsed.supplier_code, parsed.invoice_number, parsed.division_invoice),
    )
    existing = cursor.fetchone()
    extracted_raw = json.dumps(
        {
            **parsed.extracted_raw,
            "local_source_file": local_source_file,
            "detail_files": detail_entries,
        }
    )
    payload = (
        parsed.invoice_number,
        parsed.invoice_date,
        parsed.issuer_company_name,
        parsed.billed_company_name,
        parsed.supplier_name,
        "SL",
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
        "import",
        "",
        "",
        parsed.original_filename,
        parsed.parser_name,
        parsed.parser_confidence,
        extracted_raw,
        "Imported from historical marketing examples by parser.",
        parsed.sender_email,
        parsed.original_filename,
        parsed.period_yyyymm,
    )
    update_payload = payload[1:]
    if existing:
        cursor.execute(
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
                division_invoice = %s,
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
            update_payload + (str(existing[0]),),
        )
        return
    cursor.execute(
        f"""
        INSERT INTO {SCHEMA_NAME}.documents (
            id, invoice_number, invoice_date, issuer_company_name, billed_company_name,
            supplier_name, company_code, windows_path, drive_url, received_at, sender_email,
            original_filename, division_invoice, billing_period_start, billing_period_end,
            vat_percent, gross_amount, vat_amount, net_amount,
            supplier_id, supplier_code, currency_code, drive_file_id,
            storage_root, document_type, status, source_channel, email_message_id, email_thread_id,
            attachment_original_name, parser_name, parser_confidence, extracted_raw, review_notes,
            created_at, updated_at, source_sender, source_subject, period_yyyymm
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s::jsonb, %s,
            NOW(), NOW(), %s, %s, %s
        )
        """,
        (str(uuid.uuid4()),) + payload,
    )


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
