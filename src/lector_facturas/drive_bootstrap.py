from __future__ import annotations

from dataclasses import dataclass

from lector_facturas.folder_structure import ENTITY_ALIASES, month_range
from lector_facturas.folder_templates import TEMPLATES
from lector_facturas.google_drive import GoogleDriveClient


FolderTree = dict[str, object]


@dataclass(frozen=True)
class DriveBootstrapResult:
    root_folder_id: str
    root_folder_name: str
    created_paths: tuple[str, ...]


def bootstrap_drive_structure(
    client: GoogleDriveClient,
    *,
    root_name: str,
    year: int,
    start_month: int,
    end_month: int,
    entities: tuple[str, ...] = ("SL", "Ltd", "Inc"),
    parent_id: str = "root",
) -> DriveBootstrapResult:
    root_folder = client.ensure_folder(name=root_name, parent_id=parent_id)
    created_paths: list[str] = []

    for entity in entities:
        template = TEMPLATES[entity]
        entity_name = ENTITY_ALIASES.get(entity, entity)
        entity_folder = client.ensure_folder(name=entity_name, parent_id=root_folder["id"])
        year_folder = client.ensure_folder(name=str(year), parent_id=entity_folder["id"])

        for yyyymm in month_range(year, start_month, end_month):
            month_folder = client.ensure_folder(name=yyyymm, parent_id=year_folder["id"])
            _ensure_tree(
                client,
                base_id=month_folder["id"],
                base_path=f"{root_name}/{entity_name}/{year}/{yyyymm}",
                tree=template,
                created_paths=created_paths,
            )

    return DriveBootstrapResult(
        root_folder_id=str(root_folder["id"]),
        root_folder_name=str(root_folder["name"]),
        created_paths=tuple(created_paths),
    )


def _ensure_tree(
    client: GoogleDriveClient,
    *,
    base_id: str,
    base_path: str,
    tree: FolderTree,
    created_paths: list[str],
) -> None:
    for name, child in tree.items():
        folder = client.ensure_folder(name=name, parent_id=base_id)
        child_path = f"{base_path}/{name}"
        created_paths.append(child_path)
        _ensure_tree(
            client,
            base_id=str(folder["id"]),
            base_path=child_path,
            tree=child,
            created_paths=created_paths,
        )
