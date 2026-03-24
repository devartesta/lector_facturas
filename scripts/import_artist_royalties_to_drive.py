from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import json
import shutil
import sys
import tempfile
import uuid
from zipfile import ZipFile

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import psycopg

from lector_facturas.google_drive import DriveConfig, GoogleDriveClient, GoogleOAuthConfig
from lector_facturas.parsers.artist_royalties import (
    ArtistRoyaltyDocument,
    ArtistRoyaltyMonthlySummary,
    parse_artist_royalty_pdf,
    parse_artist_royalties_summary_text,
)
from lector_facturas.review_workflow import company_folder_name


SCHEMA_NAME = "invoices"
FINANCE_ROOT = Path(r"C:\Users\AdriàSebastià\OneDrive - Artesta\ARTESTA - 6. Finances")


@dataclass(frozen=True)
class RoyaltyMonthBundle:
    period_yyyymm: str
    pdf_paths: tuple[Path, ...]
    summary_txt: Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload artist royalties PDFs and monthly summaries to Google Drive and register them in Postgres.")
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

    bundles = discover_bundles()
    with psycopg.connect(args.database_url) as conn:
        with conn.cursor() as cur:
            for bundle in bundles:
                import_month(cur, client, args.root_folder_id, bundle)
        conn.commit()

    print(f"Imported artist royalties for {len(bundles)} months.")
    return 0


def discover_bundles() -> list[RoyaltyMonthBundle]:
    jan_root = FINANCE_ROOT / "Artesta Store, S.L" / "2026" / "1Q" / "202601" / "Gastos" / "Artistas"
    feb_root = FINANCE_ROOT / "Artesta Store, S.L" / "2026" / "1Q" / "202602" / "Gastos" / "Artistas"

    zip_path = next(jan_root.glob("*.zip"))
    temp_root = Path(tempfile.gettempdir()) / "lector_facturas_artist_royalties_202601"
    if temp_root.exists():
        shutil.rmtree(temp_root)
    temp_root.mkdir(parents=True, exist_ok=True)
    with ZipFile(zip_path) as archive:
        archive.extractall(temp_root)
    jan_pdfs = tuple(sorted(temp_root.rglob("*.pdf")))

    feb_pdfs = tuple(sorted((feb_root / "facturas").glob("*.pdf")))
    return [
        RoyaltyMonthBundle(
            period_yyyymm="202601",
            pdf_paths=jan_pdfs,
            summary_txt=jan_root / "resumen_agregado_pagoartistas_202601.txt",
        ),
        RoyaltyMonthBundle(
            period_yyyymm="202602",
            pdf_paths=feb_pdfs,
            summary_txt=feb_root / "pagos" / "resumen_agregado_pagoartistas_202602 (1).txt",
        ),
    ]


def import_month(cursor, client: GoogleDriveClient, root_folder_id: str, bundle: RoyaltyMonthBundle) -> None:
    destination_folder = "\\".join(
        [
            "ARTESTA - 6. Finances",
            company_folder_name("ARTESTA STORE, S.L."),
            bundle.period_yyyymm[:4],
            bundle.period_yyyymm,
            "expenses",
            "cogs",
            "royalties",
        ]
    )
    parent_id = ensure_drive_folder_path(client, root_folder_id, destination_folder)
    for pdf_path in bundle.pdf_paths:
        parsed = parse_artist_royalty_pdf(pdf_path)
        filename = f"ROYALTIES_{parsed.invoice_date.strftime('%Y%m%d')}_{parsed.invoice_number}.pdf"
        drive_file = client.ensure_file(
            name=filename,
            parent_id=parent_id,
            content=pdf_path.read_bytes(),
            mime_type="application/pdf",
        )
        upsert_royalty_document(
            cursor,
            parsed=parsed,
            windows_path=destination_folder + "\\" + filename,
            drive_file_id=str(drive_file.get("id", "")),
            drive_url=str(drive_file.get("webViewLink", "")),
            local_source_file=str(pdf_path),
        )

    summary_name = f"ROYALTIES_SUMMARY_{bundle.period_yyyymm}.txt"
    summary_drive = client.ensure_file(
        name=summary_name,
        parent_id=parent_id,
        content=bundle.summary_txt.read_bytes(),
        mime_type="text/plain",
    )
    summaries = parse_artist_royalties_summary_text(
        bundle.summary_txt.read_text(encoding="utf-8"),
        source_filename=bundle.summary_txt.name,
    )
    for summary in summaries:
        upsert_royalty_summary(
            cursor,
            summary=summary,
            windows_path=destination_folder + "\\" + summary_name,
            drive_file_id=str(summary_drive.get("id", "")),
            drive_url=str(summary_drive.get("webViewLink", "")),
        )


def upsert_royalty_document(
    cursor,
    *,
    parsed: ArtistRoyaltyDocument,
    windows_path: str,
    drive_file_id: str,
    drive_url: str,
    local_source_file: str,
) -> None:
    cursor.execute(
        f"""
        SELECT id FROM {SCHEMA_NAME}.artist_royalties_documents
        WHERE company_code = %s AND period_yyyymm = %s AND invoice_number = %s
        """,
        (parsed.company_code, parsed.period_yyyymm, parsed.invoice_number),
    )
    existing = cursor.fetchone()
    extracted_raw = json.dumps(
        {
            **parsed.extracted_raw,
            "local_source_file": local_source_file,
        }
    )
    payload = (
        parsed.company_code,
        parsed.supplier_code,
        parsed.supplier_name,
        parsed.invoice_number,
        parsed.credit_note_number,
        parsed.invoice_date,
        parsed.billing_period_start,
        parsed.billing_period_end,
        parsed.period_yyyymm,
        parsed.artist_name,
        parsed.artist_tax_id,
        parsed.artist_email,
        parsed.artist_country,
        parsed.artist_region_code,
        parsed.payment_method,
        parsed.gross_amount,
        parsed.withholding_percent,
        parsed.withholding_amount,
        parsed.net_amount,
        parsed.currency_code,
        windows_path,
        drive_file_id,
        drive_url,
        parsed.original_filename,
        "manual",
        parsed.parser_name,
        parsed.parser_confidence,
        extracted_raw,
        "Imported from artist royalty credit note PDF.",
    )
    if existing:
        cursor.execute(
            f"""
            UPDATE {SCHEMA_NAME}.artist_royalties_documents
            SET company_code = %s,
                supplier_code = %s,
                supplier_name = %s,
                invoice_number = %s,
                credit_note_number = %s,
                invoice_date = %s,
                billing_period_start = %s,
                billing_period_end = %s,
                period_yyyymm = %s,
                artist_name = %s,
                artist_tax_id = %s,
                artist_email = %s,
                artist_country = %s,
                artist_region_code = %s,
                payment_method = %s,
                gross_amount = %s,
                withholding_percent = %s,
                withholding_amount = %s,
                net_amount = %s,
                currency_code = %s,
                windows_path = %s,
                drive_file_id = %s,
                drive_url = %s,
                original_filename = %s,
                source_channel = %s,
                parser_name = %s,
                parser_confidence = %s,
                extracted_raw = %s::jsonb,
                review_notes = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            payload + (str(existing[0]),),
        )
        return
    cursor.execute(
        f"""
        INSERT INTO {SCHEMA_NAME}.artist_royalties_documents (
            id, company_code, supplier_code, supplier_name, invoice_number, credit_note_number,
            invoice_date, billing_period_start, billing_period_end, period_yyyymm, artist_name,
            artist_tax_id, artist_email, artist_country, artist_region_code, payment_method,
            gross_amount, withholding_percent, withholding_amount, net_amount, currency_code,
            windows_path, drive_file_id, drive_url, original_filename, source_channel,
            parser_name, parser_confidence, extracted_raw, review_notes, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s::jsonb, %s, NOW(), NOW()
        )
        """,
        (str(uuid.uuid4()),) + payload,
    )


def upsert_royalty_summary(
    cursor,
    *,
    summary: ArtistRoyaltyMonthlySummary,
    windows_path: str,
    drive_file_id: str,
    drive_url: str,
) -> None:
    cursor.execute(
        f"""
        SELECT id FROM {SCHEMA_NAME}.artist_royalties_monthly_summary
        WHERE company_code = %s AND period_yyyymm = %s AND summary_scope = %s
        """,
        (summary.company_code, summary.period_yyyymm, summary.summary_scope),
    )
    existing = cursor.fetchone()
    extracted_raw = json.dumps(summary.extracted_raw)
    payload = (
        summary.company_code,
        summary.supplier_code,
        summary.summary_scope,
        summary.period_yyyymm,
        summary.posters_amount,
        summary.stationery_amount,
        summary.gross_amount,
        summary.withholding_amount,
        summary.withholding_percent,
        summary.net_amount,
        summary.paypal_amount,
        summary.bank_transfer_amount,
        summary.one_x_amount,
        summary.source_filename,
        windows_path,
        drive_file_id,
        drive_url,
        extracted_raw,
    )
    if existing:
        cursor.execute(
            f"""
            UPDATE {SCHEMA_NAME}.artist_royalties_monthly_summary
            SET company_code = %s,
                supplier_code = %s,
                summary_scope = %s,
                period_yyyymm = %s,
                posters_amount = %s,
                stationery_amount = %s,
                gross_amount = %s,
                withholding_amount = %s,
                withholding_percent = %s,
                net_amount = %s,
                paypal_amount = %s,
                bank_transfer_amount = %s,
                one_x_amount = %s,
                source_filename = %s,
                windows_path = %s,
                drive_file_id = %s,
                drive_url = %s,
                extracted_raw = %s::jsonb,
                updated_at = NOW()
            WHERE id = %s
            """,
            payload + (str(existing[0]),),
        )
        return
    cursor.execute(
        f"""
        INSERT INTO {SCHEMA_NAME}.artist_royalties_monthly_summary (
            id, company_code, supplier_code, summary_scope, period_yyyymm, posters_amount,
            stationery_amount, gross_amount, withholding_amount, withholding_percent, net_amount,
            paypal_amount, bank_transfer_amount, one_x_amount, source_filename, windows_path,
            drive_file_id, drive_url, extracted_raw, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s::jsonb, NOW(), NOW()
        )
        """,
        (str(uuid.uuid4()),) + payload,
    )


def ensure_drive_folder_path(client: GoogleDriveClient, root_folder_id: str, windows_folder_path: str) -> str:
    parent_id = root_folder_id
    for folder_name in windows_folder_path.split("\\")[1:]:
        folder = client.ensure_folder(name=folder_name, parent_id=parent_id)
        parent_id = str(folder["id"])
    return parent_id


if __name__ == "__main__":
    raise SystemExit(main())
