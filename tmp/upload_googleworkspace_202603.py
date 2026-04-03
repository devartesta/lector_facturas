import sys, os
sys.path.insert(0, "src")
from pathlib import Path
from lector_facturas.google_drive import GoogleDriveClient
from lector_facturas.invoice_ingestion import ensure_validation_folders
from lector_facturas.settings import AppSettings

settings = AppSettings(
    google_client_id=os.environ["GOOGLE_CLIENT_ID"],
    google_client_secret=os.environ["GOOGLE_CLIENT_SECRET"],
    google_refresh_token=os.environ["GOOGLE_REFRESH_TOKEN"],
    drive_shared_drive_id=os.environ.get("GOOGLE_DRIVE_SHARED_DRIVE_ID", ""),
    drive_root_folder_id=os.environ["GOOGLE_DRIVE_ROOT_FOLDER_ID"],
)
drive = GoogleDriveClient(settings.to_drive_config())
validation_folders = ensure_validation_folders(drive, root_folder_id=settings.drive_root_folder_id)
to_process_id = str(validation_folders["to_process"]["id"])

f = Path(r"C:\Users\AdriàSebastià\Downloads\5528949048.pdf")
result = drive.upload_file(
    name=f.name,
    parent_id=to_process_id,
    content=f.read_bytes(),
    mime_type="application/pdf",
)
print(f"Uploaded: {result.get('name')} -> {result.get('id')}")
