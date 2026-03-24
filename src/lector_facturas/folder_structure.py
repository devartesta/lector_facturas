"""Folder structure bootstrap and maintenance helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Iterable

from lector_facturas.folder_templates import TEMPLATES


FolderTree = dict[str, object]


@dataclass(frozen=True)
class BootstrapConfig:
    root: Path
    entities: tuple[str, ...]
    year: int
    start_month: int
    end_month: int


ENTITY_ALIASES: dict[str, str] = {
    "SL": "Artesta Store, S.L",
    "Ltd": "Artesta Stores (UK) Ltd",
    "Inc": "Artesta Inc",
    "Artesta Store, S.L": "Artesta Store, S.L",
    "Artesta Stores (UK) Ltd": "Artesta Stores (UK) Ltd",
    "Artesta Inc": "Artesta Inc",
}


def validate_template(tree: FolderTree, *, path: str = "") -> None:
    if not isinstance(tree, dict):
        raise ValueError(f"Template node '{path or '<root>'}' must be a dict.")
    for name, child in tree.items():
        if not isinstance(name, str) or not name:
            raise ValueError(f"Invalid folder name at '{path or '<root>'}'.")
        if "/" in name or "\\" in name:
            raise ValueError(f"Folder name '{name}' at '{path or '<root>'}' contains a path separator.")
        if not isinstance(child, dict):
            raise ValueError(f"Template node '{_join_path(path, name)}' must be a dict.")
        validate_template(child, path=_join_path(path, name))


def month_range(year: int, start_month: int, end_month: int) -> Iterable[str]:
    if start_month < 1 or end_month > 12 or start_month > end_month:
        raise ValueError("Month range must be within 1..12 and start_month <= end_month.")
    return (f"{year}{month:02d}" for month in range(start_month, end_month + 1))


def bootstrap_structure(config: BootstrapConfig, *, templates: dict[str, FolderTree] | None = None) -> list[Path]:
    templates = templates or TEMPLATES
    created: list[Path] = []

    for entity in config.entities:
        template = templates.get(entity)
        if template is None:
            raise ValueError(f"Unknown entity template '{entity}'.")
        validate_template(template, path=entity)
        target_name = ENTITY_ALIASES.get(entity, entity)
        for yyyymm in month_range(config.year, config.start_month, config.end_month):
            year_root = config.root / target_name / str(config.year)
            base = _resolve_safe_month_path(year_root, yyyymm)
            _ensure_tree(base, template, created)

    return created


def ensure_next_month(root: Path, *, entities: tuple[str, ...], reference: date | None = None) -> list[Path]:
    reference = reference or date.today()
    year, month = _next_month(reference.year, reference.month)
    config = BootstrapConfig(
        root=root,
        entities=entities,
        year=year,
        start_month=month,
        end_month=month,
    )
    return bootstrap_structure(config)


def _ensure_tree(base: Path, tree: FolderTree, created: list[Path]) -> None:
    if not base.exists():
        base.mkdir(parents=True, exist_ok=True)
        created.append(base)
    for name, child in tree.items():
        child_path = base / name
        if not child_path.exists():
            child_path.mkdir(parents=True, exist_ok=True)
            created.append(child_path)
        _ensure_tree(child_path, child, created)


def _next_month(year: int, month: int) -> tuple[int, int]:
    if month == 12:
        return year + 1, 1
    return year, month + 1


def _join_path(base: str, leaf: str) -> str:
    return f"{base}/{leaf}" if base else leaf


def _resolve_safe_month_path(year_root: Path, yyyymm: str) -> Path:
    candidate = year_root / yyyymm
    if not candidate.exists():
        return candidate
    if candidate.is_dir():
        return candidate

    suffix = 1
    while True:
        alt = year_root / f"{yyyymm}_new" if suffix == 1 else year_root / f"{yyyymm}_new{suffix}"
        if not alt.exists():
            return alt
        if alt.is_dir():
            return alt
        suffix += 1
