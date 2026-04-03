"""Upload Proco PDF to validation/to-process and Excel to validation/proco-detail."""
import sys
sys.path.insert(0, "src")

from pathlib import Path
from lector_facturas.google_drive import GoogleDriveClient
from lector_facturas.invoice_ingestion import ensure_validation_folders
import os
from lector_facturas.settings import AppSettings

settings = AppSettings(
    google_client_id=os.environ["GOOGLE_CLIENT_ID"],
    google_client_secret=os.environ["GOOGLE_CLIENT_SECRET"],
    google_refresh_token=os.environ["GOOGLE_REFRESH_TOKEN"],
    drive_shared_drive_id=os.environ.get("GOOGLE_DRIVE_SHARED_DRIVE_ID", ""),
    drive_root_folder_id=os.environ["GOOGLE_DRIVE_ROOT_FOLDER_ID"],
)
drive_client = GoogleDriveClient(settings.to_drive_config())
root_folder_id = settings.drive_root_folder_id

validation_folders = ensure_validation_folders(drive_client, root_folder_id=root_folder_id)
to_process_id = str(validation_folders["to_process"]["id"])
proco_detail_id = str(validation_folders["proco_detail"]["id"])
print(f"to-process folder: {to_process_id}")
print(f"proco-detail folder: {proco_detail_id}")

pdf_path = Path(r"C:\Users\AdriàSebastià\Downloads\SI6033972.pdf")
xlsx_path = Path(r"C:\Users\AdriàSebastià\Downloads\Artesta_March_2026.xlsx")

# Upload PDF to to-process
pdf_result = drive_client.upload_file(
    name=pdf_path.name,
    parent_id=to_process_id,
    content=pdf_path.read_bytes(),
    mime_type="application/pdf",
)
print(f"PDF uploaded: {pdf_result.get('name')} — {pdf_result.get('id')}")

# Upload Excel to proco-detail
xlsx_result = drive_client.upload_file(
    name=xlsx_path.name,
    parent_id=proco_detail_id,
    content=xlsx_path.read_bytes(),
    mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)
print(f"Excel uploaded: {xlsx_result.get('name')} — {xlsx_result.get('id')}")
