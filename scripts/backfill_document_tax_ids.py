from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path
import sys
from tempfile import NamedTemporaryFile

import psycopg
from pypdf import PdfReader

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from lector_facturas.document_tax_ids import extract_document_tax_ids  # noqa: E402
from lector_facturas.google_drive import GoogleDriveClient  # noqa: E402
from lector_facturas.settings import load_settings  # noqa: E402


@dataclass(frozen=True)
class DocumentRow:
    id: str
    original_filename: str
    issuer_company_name: str
    billed_company_name: str
    drive_file_id: str
    ocr_text: str


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill issuer_tax_id and billed_tax_id on invoices.documents.")
    parser.add_argument("--apply", action="store_true", help="Persist matches at or above --min-confidence.")
    parser.add_argument("--limit", type=int, default=0, help="Maximum number of documents to scan.")
    parser.add_argument(
        "--min-confidence",
        choices=("high", "medium"),
        default="high",
        help="Minimum confidence required for updates. Default: high.",
    )
    parser.add_argument("--ocr-empty", action="store_true", help="Use Google Drive OCR when PDF text extraction is empty.")
    parser.add_argument("--report", default="output/document_tax_id_backfill.csv", help="CSV report path.")
    args = parser.parse_args()

    settings = load_settings()
    database_url = _database_url()
    drive_client = GoogleDriveClient(settings.to_drive_config())
    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    rows = _load_pending_documents(database_url, limit=args.limit)
    scanned = updated = matched = failed = 0
    with psycopg.connect(database_url) as conn, report_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "document_id",
                "original_filename",
                "issuer_tax_id",
                "billed_tax_id",
                "confidence",
                "action",
                "notes",
            ],
        )
        writer.writeheader()
        for row in rows:
            scanned += 1
            try:
                text = row.ocr_text.strip()
                if not text:
                    content = drive_client.download_file_bytes(file_id=row.drive_file_id)
                    text = _extract_pdf_text(content, row.original_filename)
                    if not text.strip() and args.ocr_empty:
                        text = drive_client.ocr_pdf_to_text(name=row.original_filename, content=content)
                result = extract_document_tax_ids(
                    text,
                    issuer_company_name=row.issuer_company_name,
                    billed_company_name=row.billed_company_name,
                )
                allowed_confidence = {"high"} if args.min_confidence == "high" else {"high", "medium"}
                should_update = bool(result.issuer_tax_id or result.billed_tax_id) and result.confidence in allowed_confidence
                action = "matched"
                if should_update:
                    matched += 1
                    if args.apply:
                        _update_document(conn, row.id, result.issuer_tax_id, result.billed_tax_id)
                        updated += 1
                        action = "updated"
                else:
                    action = "no_match"
                writer.writerow(
                    {
                        "document_id": row.id,
                        "original_filename": row.original_filename,
                        "issuer_tax_id": result.issuer_tax_id or "",
                        "billed_tax_id": result.billed_tax_id or "",
                        "confidence": result.confidence,
                        "action": action,
                        "notes": result.notes,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                failed += 1
                writer.writerow(
                    {
                        "document_id": row.id,
                        "original_filename": row.original_filename,
                        "issuer_tax_id": "",
                        "billed_tax_id": "",
                        "confidence": "error",
                        "action": "error",
                        "notes": str(exc),
                    }
                )
        if args.apply:
            conn.commit()

    mode = "APPLY" if args.apply else "DRY_RUN"
    print(f"{mode}: scanned={scanned} matched={matched} updated={updated} failed={failed} report={report_path}")
    return 0


def _database_url() -> str:
    import os

    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url:
        raise RuntimeError("DATABASE_URL is required")
    return database_url


def _load_pending_documents(database_url: str, *, limit: int) -> list[DocumentRow]:
    limit_sql = "LIMIT %s" if limit else ""
    params: tuple[int, ...] = (limit,) if limit else ()
    query = f"""
        SELECT
            id::text,
            original_filename,
            issuer_company_name,
            billed_company_name,
            drive_file_id,
            COALESCE(extracted_raw->>'ocr_text', '') AS ocr_text
        FROM invoices.documents
        WHERE (issuer_tax_id IS NULL OR billed_tax_id IS NULL)
          AND drive_file_id IS NOT NULL
          AND drive_file_id <> ''
        ORDER BY invoice_date NULLS LAST, created_at
        {limit_sql}
    """
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            return [DocumentRow(*row) for row in cur.fetchall()]


def _extract_pdf_text(content: bytes, filename: str) -> str:
    suffix = Path(filename).suffix or ".pdf"
    with NamedTemporaryFile(delete=False, suffix=suffix) as handle:
        handle.write(content)
        temp_path = Path(handle.name)
    try:
        reader = PdfReader(str(temp_path))
        return "\n".join((page.extract_text() or "") for page in reader.pages)
    finally:
        temp_path.unlink(missing_ok=True)


def _update_document(conn, document_id: str, issuer_tax_id: str | None, billed_tax_id: str | None) -> None:
    conn.execute(
        """
        UPDATE invoices.documents
           SET issuer_tax_id = COALESCE(issuer_tax_id, %s),
               billed_tax_id = COALESCE(billed_tax_id, %s),
               updated_at = NOW()
         WHERE id = %s
        """,
        (issuer_tax_id, billed_tax_id, document_id),
    )


if __name__ == "__main__":
    raise SystemExit(main())
