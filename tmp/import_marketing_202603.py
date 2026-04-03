"""Direct import of Meta + Google Ads invoices for 202603 (SL).

Bypasses the pipeline (which doesn't handle multi-division returns).
Parses locally, uploads to correct Drive folder, inserts DB rows.
"""
import sys, os
sys.path.insert(0, "src")

from pathlib import Path
from lector_facturas.google_drive import GoogleDriveClient
from lector_facturas.settings import AppSettings
from lector_facturas.api.store import ReviewStore
from lector_facturas.parsers.marketing_ads import parse_google_ads_pdf, parse_meta_ads_pdf
from lector_facturas.invoice_ingestion import ensure_drive_path

settings = AppSettings(
    google_client_id=os.environ["GOOGLE_CLIENT_ID"],
    google_client_secret=os.environ["GOOGLE_CLIENT_SECRET"],
    google_refresh_token=os.environ["GOOGLE_REFRESH_TOKEN"],
    drive_shared_drive_id=os.environ.get("GOOGLE_DRIVE_SHARED_DRIVE_ID", ""),
    drive_root_folder_id=os.environ["GOOGLE_DRIVE_ROOT_FOLDER_ID"],
)

drive = GoogleDriveClient(settings.to_drive_config())
store = ReviewStore(database_url=os.environ["DATABASE_URL"])

company_code = "SL"
company_folder = "Artesta Store, S.L"
root_folder_id = settings.drive_root_folder_id

INVOICES = [
    {
        "pdf": Path(r"C:\Users\AdriàSebastià\Downloads\Transaction_251912684.pdf"),
        "parser": parse_meta_ads_pdf,
        "supplier_code": "METAADS",
    },
    {
        "pdf": Path(r"C:\Users\AdriàSebastià\Downloads\5538547928.pdf"),
        "parser": parse_google_ads_pdf,
        "supplier_code": "GOOGLEADS",
    },
]

for inv in INVOICES:
    pdf_path: Path = inv["pdf"]
    print(f"\n=== {pdf_path.name} ===")
    rows = inv["parser"](pdf_path)
    print(f"Parsed {len(rows)} divisions: {[r.division_invoice for r in rows]}")

    # Build final filename from first row (all share same invoice_number + date)
    first = rows[0]
    final_name = f"{first.supplier_code}_{first.invoice_date.strftime('%Y%m%d')}_{first.invoice_number}.pdf"
    period = first.period_yyyymm  # e.g. "202603"
    year = period[:4]
    destination_path = "expenses\\opex\\marketing"
    windows_path = f"ARTESTA - 6. Finances\\{company_folder}\\{year}\\{period}\\{destination_path}\\{final_name}"
    print(f"Destination: {windows_path}")

    # Ensure Drive folder exists and upload PDF
    final_parent_id = ensure_drive_path(drive, root_folder_id=root_folder_id, windows_path=windows_path)
    content = pdf_path.read_bytes()

    # Check if file already exists in destination folder
    existing = drive.list_files(parent_id=final_parent_id, name=final_name)
    if existing:
        drive_file_id = str(existing[0]["id"])
        drive_url = str(existing[0].get("webViewLink", ""))
        print(f"File already exists in Drive: {drive_url}")
    else:
        uploaded = drive.upload_file(
            name=final_name,
            parent_id=final_parent_id,
            content=content,
            mime_type="application/pdf",
        )
        drive_file_id = str(uploaded["id"])
        drive_url = str(uploaded.get("webViewLink", ""))
        print(f"Uploaded to Drive: {drive_url}")

    # Insert DB rows for each division
    for row in rows:
        exists = store.document_exists_by_business_key(
            company_code=company_code,
            supplier_code=row.supplier_code,
            invoice_number=row.invoice_number,
            division_invoice=row.division_invoice,
            document_type="invoice",
        )
        if exists:
            print(f"  [{row.division_invoice}] Already in DB, skipping")
            continue

        doc_id = store.insert_document_from_parsed(
            company_code=company_code,
            supplier_code=row.supplier_code,
            parsed=row,
            windows_path=windows_path,
            drive_url=drive_url,
            drive_file_id=drive_file_id,
            original_filename=pdf_path.name,
            source_channel="import",
            sender_email=row.sender_email,
            source_subject=f"Marketing import 202603",
            review_notes="Direct import - marketing split eu/uk/us",
        )
        print(f"  [{row.division_invoice}] Inserted doc_id={doc_id} amount={row.net_amount}")

print("\nDone.")
