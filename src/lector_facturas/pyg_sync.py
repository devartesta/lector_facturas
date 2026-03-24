from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from lector_facturas.pyg_consolidated_workbook import build_pyg_consolidated_workbook, collect_pyg_consolidated_data, default_output_path as default_output_path_consolidated
from lector_facturas.google_drive import GoogleDriveClient
from lector_facturas.pyg_inc_workbook import build_pyg_inc_workbook, collect_pyg_inc_data, default_output_path as default_output_path_inc
from lector_facturas.pyg_ltd_workbook import build_pyg_ltd_workbook, collect_pyg_ltd_data, default_output_path as default_output_path_ltd
from lector_facturas.pyg_sl_workbook import build_pyg_sl_workbook, collect_pyg_sl_data, default_output_path
from lector_facturas.settings import AppSettings


@dataclass(frozen=True)
class PygSlSyncResult:
    year: int
    drive_folder_id: str
    drive_file_id: str
    drive_file_name: str
    drive_file_url: str
    local_output_path: str
    replaced_file_ids: tuple[str, ...]


@dataclass(frozen=True)
class PygIncSyncResult:
    year: int
    drive_folder_id: str
    drive_file_id: str
    drive_file_name: str
    drive_file_url: str
    local_output_path: str
    replaced_file_ids: tuple[str, ...]


@dataclass(frozen=True)
class PygLtdSyncResult:
    year: int
    drive_folder_id: str
    drive_file_id: str
    drive_file_name: str
    drive_file_url: str
    local_output_path: str
    replaced_file_ids: tuple[str, ...]


@dataclass(frozen=True)
class PygConsolidatedSyncResult:
    year: int
    drive_folder_id: str
    drive_file_id: str
    drive_file_name: str
    drive_file_url: str
    local_output_path: str
    replaced_file_ids: tuple[str, ...]


def sync_pyg_sl_to_drive(
    *,
    settings: AppSettings,
    year: int,
    drive_folder_id: str | None = None,
    file_name: str | None = None,
    output_root: Path | None = None,
) -> PygSlSyncResult:
    if not settings.google_oauth_ready:
        raise RuntimeError("Google OAuth is not configured.")
    if not settings.drive_root_folder_id and not drive_folder_id:
        raise RuntimeError("Google Drive root folder is not configured.")
    database_url = _database_url()
    if not database_url:
        raise RuntimeError("DATABASE_URL is not configured.")

    root = output_root or Path(__file__).resolve().parents[2]
    output_path = default_output_path(root, year)
    bundle = collect_pyg_sl_data(year=year, database_url=database_url)
    build_pyg_sl_workbook(bundle, output_path)

    client = GoogleDriveClient(settings.to_drive_config())
    target_folder_id = drive_folder_id or settings.drive_root_folder_id
    target_name = file_name or f"pyg_sl_{year}.xlsx"
    existing = client.list_files(parent_id=target_folder_id, name=target_name)
    for item in existing:
        client.trash_file(file_id=str(item["id"]))
    created = client.upload_file(
        name=target_name,
        parent_id=target_folder_id,
        content=output_path.read_bytes(),
        mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    return PygSlSyncResult(
        year=year,
        drive_folder_id=target_folder_id,
        drive_file_id=str(created["id"]),
        drive_file_name=str(created["name"]),
        drive_file_url=str(created.get("webViewLink", "")),
        local_output_path=str(output_path),
        replaced_file_ids=tuple(str(item["id"]) for item in existing),
    )


def sync_pyg_inc_to_drive(
    *,
    settings: AppSettings,
    year: int,
    drive_folder_id: str | None = None,
    file_name: str | None = None,
    output_root: Path | None = None,
) -> PygIncSyncResult:
    if not settings.google_oauth_ready:
        raise RuntimeError("Google OAuth is not configured.")
    if not settings.drive_root_folder_id and not drive_folder_id:
        raise RuntimeError("Google Drive root folder is not configured.")
    database_url = _database_url()
    if not database_url:
        raise RuntimeError("DATABASE_URL is not configured.")

    root = output_root or Path(__file__).resolve().parents[2]
    output_path = default_output_path_inc(root, year)
    bundle = collect_pyg_inc_data(year=year, database_url=database_url)
    build_pyg_inc_workbook(bundle, output_path)

    client = GoogleDriveClient(settings.to_drive_config())
    target_folder_id = drive_folder_id or settings.drive_root_folder_id
    target_name = file_name or f"pyg_inc_{year}.xlsx"
    existing = client.list_files(parent_id=target_folder_id, name=target_name)
    for item in existing:
        client.trash_file(file_id=str(item["id"]))
    created = client.upload_file(
        name=target_name,
        parent_id=target_folder_id,
        content=output_path.read_bytes(),
        mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    return PygIncSyncResult(
        year=year,
        drive_folder_id=target_folder_id,
        drive_file_id=str(created["id"]),
        drive_file_name=str(created["name"]),
        drive_file_url=str(created.get("webViewLink", "")),
        local_output_path=str(output_path),
        replaced_file_ids=tuple(str(item["id"]) for item in existing),
    )


def sync_pyg_ltd_to_drive(
    *,
    settings: AppSettings,
    year: int,
    drive_folder_id: str | None = None,
    file_name: str | None = None,
    output_root: Path | None = None,
) -> PygLtdSyncResult:
    if not settings.google_oauth_ready:
        raise RuntimeError("Google OAuth is not configured.")
    if not settings.drive_root_folder_id and not drive_folder_id:
        raise RuntimeError("Google Drive root folder is not configured.")
    database_url = _database_url()
    if not database_url:
        raise RuntimeError("DATABASE_URL is not configured.")

    root = output_root or Path(__file__).resolve().parents[2]
    output_path = default_output_path_ltd(root, year)
    bundle = collect_pyg_ltd_data(year=year, database_url=database_url)
    build_pyg_ltd_workbook(bundle, output_path)

    client = GoogleDriveClient(settings.to_drive_config())
    target_folder_id = drive_folder_id or settings.drive_root_folder_id
    target_name = file_name or f"pyg_ltd_{year}.xlsx"
    existing = client.list_files(parent_id=target_folder_id, name=target_name)
    for item in existing:
        client.trash_file(file_id=str(item["id"]))
    created = client.upload_file(
        name=target_name,
        parent_id=target_folder_id,
        content=output_path.read_bytes(),
        mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    return PygLtdSyncResult(
        year=year,
        drive_folder_id=target_folder_id,
        drive_file_id=str(created["id"]),
        drive_file_name=str(created["name"]),
        drive_file_url=str(created.get("webViewLink", "")),
        local_output_path=str(output_path),
        replaced_file_ids=tuple(str(item["id"]) for item in existing),
    )


def sync_pyg_consolidated_to_drive(
    *,
    settings: AppSettings,
    year: int,
    drive_folder_id: str | None = None,
    file_name: str | None = None,
    output_root: Path | None = None,
) -> PygConsolidatedSyncResult:
    if not settings.google_oauth_ready:
        raise RuntimeError("Google OAuth is not configured.")
    if not settings.drive_root_folder_id and not drive_folder_id:
        raise RuntimeError("Google Drive root folder is not configured.")
    database_url = _database_url()
    if not database_url:
        raise RuntimeError("DATABASE_URL is not configured.")

    root = output_root or Path(__file__).resolve().parents[2]
    output_path = default_output_path_consolidated(root, year)
    bundle = collect_pyg_consolidated_data(year=year, database_url=database_url)
    build_pyg_consolidated_workbook(bundle, output_path)

    client = GoogleDriveClient(settings.to_drive_config())
    target_folder_id = drive_folder_id or settings.drive_root_folder_id
    target_name = file_name or f"pyg_consolidado_{year}.xlsx"
    existing = client.list_files(parent_id=target_folder_id, name=target_name)
    for item in existing:
        client.trash_file(file_id=str(item["id"]))
    created = client.upload_file(
        name=target_name,
        parent_id=target_folder_id,
        content=output_path.read_bytes(),
        mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    return PygConsolidatedSyncResult(
        year=year,
        drive_folder_id=target_folder_id,
        drive_file_id=str(created["id"]),
        drive_file_name=str(created["name"]),
        drive_file_url=str(created.get("webViewLink", "")),
        local_output_path=str(output_path),
        replaced_file_ids=tuple(str(item["id"]) for item in existing),
    )


def _database_url() -> str:
    import os

    return os.environ.get("DATABASE_URL", "").strip()
