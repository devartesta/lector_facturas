"""Map 2026 folder structure under Artesta Inc."""
import sys, os
sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, "src")

from lector_facturas.google_drive import GoogleDriveClient, DriveConfig, GoogleOAuthConfig

drive_client = GoogleDriveClient(DriveConfig(
    oauth=GoogleOAuthConfig(
        client_id=os.environ["GOOGLE_CLIENT_ID"],
        client_secret=os.environ["GOOGLE_CLIENT_SECRET"],
        refresh_token=os.environ["GOOGLE_REFRESH_TOKEN"],
    ),
    shared_drive_id=os.environ.get("GOOGLE_DRIVE_SHARED_DRIVE_ID", ""),
    root_folder_id=os.environ["GOOGLE_DRIVE_ROOT_FOLDER_ID"],
))

root_folder_id = os.environ["GOOGLE_DRIVE_ROOT_FOLDER_ID"]

def map_tree(folder_id, indent=0, max_depth=10):
    if indent > max_depth:
        return
    items = drive_client.list_files(parent_id=folder_id)
    folders = sorted([i for i in items if i.get("mimeType") == "application/vnd.google-apps.folder"], key=lambda x: x.get("name",""))
    files = sorted([i for i in items if i.get("mimeType") != "application/vnd.google-apps.folder"], key=lambda x: x.get("name",""))
    prefix = "  " * indent
    for folder in folders:
        print(f"{prefix}[DIR] {folder['name']}/")
        map_tree(folder["id"], indent + 1, max_depth)
    for f in files:
        print(f"{prefix}[FILE] {f['name']}")

root_items = drive_client.list_files(parent_id=root_folder_id)
artesta_inc = next((i for i in root_items if i.get("name") == "Artesta Inc" and i.get("mimeType") == "application/vnd.google-apps.folder"), None)

# Get 2026 subfolder
year_2026 = next((i for i in drive_client.list_files(parent_id=artesta_inc["id"]) if i.get("name") == "2026"), None)
if year_2026:
    print("[DIR] 2026/")
    map_tree(year_2026["id"], indent=1)
