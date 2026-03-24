from __future__ import annotations

import argparse
from datetime import timezone
import json
from pathlib import Path
import shutil
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import psycopg

from lector_facturas.api.store import ReviewStore
from lector_facturas.parsers.apphoto import parse_apphoto_pdf
from lector_facturas.review_workflow import company_folder_name, get_provider


SHORT_FINANCE_ROOT = Path(r"C:\Users\ADRISE~1\OneDrive - Artesta\ARTESTA - 6. Finances")
SCHEMA_NAME = "invoices"


def main() -> int:
    parser = argparse.ArgumentParser(description="Import APPHOTO examples from 202601 and 202602 into invoices.documents.")
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--finance-root", default=str(SHORT_FINANCE_ROOT))
    args = parser.parse_args()

    finance_root = Path(args.finance_root)
    ReviewStore(database_url=args.database_url)
    source_files = discover_source_files(finance_root)
    if not source_files:
        raise SystemExit("No APPHOTO source files found in 202601/202602.")

    imported_rows: list[tuple[str, str, str]] = []
    with psycopg.connect(args.database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {SCHEMA_NAME}.review_items")
            cur.execute(f"DELETE FROM {SCHEMA_NAME}.documents")
            for source_file in source_files:
                parsed = parse_apphoto_pdf(source_file)
                provider = get_provider("ARTESTA STORE, S.L.", parsed.supplier_code)
                destination_dir = (
                    finance_root
                    / company_folder_name(provider.company)
                    / parsed.period_yyyymm[:4]
                    / parsed.period_yyyymm
                    / Path(provider.destination_path)
                )
                destination_dir.mkdir(parents=True, exist_ok=True)
                destination_name = build_destination_name(parsed.supplier_code, parsed.invoice_date.isoformat(), parsed.invoice_number, source_file.suffix)
                destination_file = safe_copy(source_file, destination_dir / destination_name)
                supplier_id = lookup_supplier_id(cur, "SL", parsed.supplier_code)
                windows_path = build_logical_windows_path(provider.company, parsed.period_yyyymm, provider.destination_path, destination_file.name)
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
                    (
                        str(uuid.uuid4()),
                        parsed.invoice_number,
                        parsed.invoice_date,
                        parsed.issuer_company_name,
                        parsed.billed_company_name,
                        parsed.supplier_name,
                        "SL",
                        windows_path,
                        "",
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
                        "",
                        "LOCAL_ONEDRIVE_COPY",
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
                                "local_source_file": str(source_file),
                                "local_destination_file": str(destination_file),
                            }
                        ),
                        "Imported from historical APPHOTO examples by parser.",
                        "",
                        parsed.original_filename,
                        parsed.period_yyyymm,
                    ),
                )
                imported_rows.append((parsed.invoice_number, parsed.supplier_code, windows_path))
        conn.commit()

    print(f"Imported {len(imported_rows)} APPHOTO invoices.")
    for invoice_number, supplier_code, windows_path in imported_rows:
        print(f"- {supplier_code} | {invoice_number} | {windows_path}")
    return 0


def discover_source_files(finance_root: Path) -> list[Path]:
    source_files: list[Path] = []
    for month in ("202601", "202602"):
        folder = finance_root / "Artesta Store, S.L" / "2026" / "1Q" / month / "Gastos" / "Proveedores" / "AP Photo"
        if not folder.exists():
            continue
        source_files.extend(sorted(path for path in folder.iterdir() if path.is_file() and path.suffix.lower() == ".pdf"))
    return source_files


def build_destination_name(supplier_code: str, invoice_date: str, invoice_number: str, suffix: str) -> str:
    compact_date = invoice_date.replace("-", "")
    safe_invoice_number = invoice_number.replace("/", "-").replace("\\", "-").replace(" ", "")
    return f"{supplier_code}_{compact_date}_{safe_invoice_number}{suffix.lower() or '.pdf'}"


def safe_copy(source_file: Path, destination_file: Path) -> Path:
    candidate = destination_file
    counter = 1
    while candidate.exists():
        counter += 1
        candidate = destination_file.with_name(f"{destination_file.stem}_new{counter}{destination_file.suffix}")
    shutil.copy2(source_file, candidate)
    return candidate


def lookup_supplier_id(cursor, company_code: str, supplier_code: str) -> str | None:
    row = cursor.execute(
        f"SELECT id FROM {SCHEMA_NAME}.suppliers WHERE company_code = %s AND supplier_code = %s",
        (company_code, supplier_code),
    ).fetchone()
    return str(row[0]) if row else None


def build_logical_windows_path(company: str, period_yyyymm: str, destination_path: str, filename: str) -> str:
    parts = [
        "ARTESTA - 6. Finances",
        company_folder_name(company),
        period_yyyymm[:4],
        period_yyyymm,
        *Path(destination_path).parts,
        filename,
    ]
    return "\\".join(parts)


if __name__ == "__main__":
    raise SystemExit(main())
