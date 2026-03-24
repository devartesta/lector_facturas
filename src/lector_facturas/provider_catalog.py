"""Provider catalog and review routing for unmatched invoices."""

from __future__ import annotations

from csv import DictReader
from dataclasses import dataclass
from importlib.resources import files
from pathlib import Path


@dataclass(frozen=True)
class ProviderRecord:
    company: str
    current_folder: str
    provider_name: str
    supplier_code: str
    destination_path: str
    notes: str
    sender_emails: tuple[str, ...] = ()


@dataclass(frozen=True)
class ReviewLocation:
    company: str
    year: int
    period_yyyymm: str
    path: Path


def load_provider_catalog() -> list[ProviderRecord]:
    csv_path = files("lector_facturas.config").joinpath("providers_master.csv")
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        rows = DictReader(handle)
        return [
            ProviderRecord(
                company=row["company"],
                current_folder=row["current_folder"],
                provider_name=row["provider_name"],
                supplier_code=row["supplier_code"],
                destination_path=row["destination_path"],
                notes=row["notes"],
                sender_emails=tuple(
                    sender.strip().lower()
                    for sender in (row.get("sender_emails", "") or "").split(";")
                    if sender.strip()
                ),
            )
            for row in rows
        ]


def find_provider_match(company: str, folder_hint: str | None = None, sender_hint: str | None = None) -> list[ProviderRecord]:
    matches: list[ProviderRecord] = []
    normalized_company = _norm(company)
    folder_hint_norm = _norm(folder_hint or "")
    sender_hint_norm = _norm(sender_hint or "")

    for record in load_provider_catalog():
        if _norm(record.company) != normalized_company:
            continue
        if folder_hint_norm and _norm(record.current_folder) == folder_hint_norm:
            matches.append(record)
            continue
        if sender_hint_norm and any(sender_hint_norm == _norm(sender) for sender in record.sender_emails):
            matches.append(record)
            continue
        if sender_hint_norm and sender_hint_norm in _norm(record.provider_name):
            matches.append(record)
    return matches


def ensure_pending_supplier_review(root: Path, company_folder: str, year: int, period_yyyymm: str) -> ReviewLocation:
    review_path = (
        root
        / company_folder
        / str(year)
        / period_yyyymm
        / "validation"
        / "pending_supplier_review"
    )
    review_path.mkdir(parents=True, exist_ok=True)
    return ReviewLocation(
        company=company_folder,
        year=year,
        period_yyyymm=period_yyyymm,
        path=review_path,
    )


def _norm(value: str) -> str:
    return " ".join(value.lower().strip().split())
