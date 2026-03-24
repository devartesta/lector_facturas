"""Manual review actions for moving invoices from validation into final folders."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import shutil

from lector_facturas.provider_catalog import ProviderRecord, load_provider_catalog


COMPANY_FOLDER_NAMES = {
    "ARTESTA STORE, S.L.": "Artesta Store, S.L",
    "ARTESTA STORES (UK) LTD": "Artesta Stores (UK) Ltd",
    "ARTESTA INC": "Artesta Inc",
}

ENTITY_ALIASES = {
    "SL": "ARTESTA STORE, S.L.",
    "Ltd": "ARTESTA STORES (UK) LTD",
    "Inc": "ARTESTA INC",
    "Artesta Store, S.L": "ARTESTA STORE, S.L.",
    "Artesta Stores (UK) Ltd": "ARTESTA STORES (UK) LTD",
    "Artesta Inc": "ARTESTA INC",
}


@dataclass(frozen=True)
class ReviewDecision:
    root: Path
    source_file: Path
    company: str
    supplier_code: str
    invoice_date: str
    invoice_number: str


@dataclass(frozen=True)
class ReviewResult:
    source_file: Path
    destination_file: Path
    provider: ProviderRecord


def normalize_company(company: str) -> str:
    return ENTITY_ALIASES.get(company, company)


def company_folder_name(company: str) -> str:
    normalized_company = normalize_company(company)
    return COMPANY_FOLDER_NAMES.get(normalized_company, normalized_company)


def list_companies() -> list[str]:
    return [COMPANY_FOLDER_NAMES[company] for company in sorted(COMPANY_FOLDER_NAMES)]


def list_providers_for_company(company: str) -> list[ProviderRecord]:
    normalized_company = normalize_company(company)
    return sorted(
        [record for record in load_provider_catalog() if record.company == normalized_company],
        key=lambda record: (record.provider_name, record.supplier_code),
    )


def get_provider(company: str, supplier_code: str) -> ProviderRecord:
    normalized_company = normalize_company(company)
    for record in load_provider_catalog():
        if record.company == normalized_company and record.supplier_code == supplier_code:
            return record
    raise LookupError(f"Provider not found for company={company!r} supplier_code={supplier_code!r}")


def apply_review_decision(decision: ReviewDecision) -> ReviewResult:
    provider = get_provider(decision.company, decision.supplier_code)
    invoice_dt = datetime.strptime(decision.invoice_date, "%Y-%m-%d")
    year = str(invoice_dt.year)
    period = invoice_dt.strftime("%Y%m")
    extension = decision.source_file.suffix.lower() or ".pdf"
    filename = f"{provider.supplier_code}_{invoice_dt.strftime('%Y%m%d')}_{decision.invoice_number}{extension}"
    destination_dir = (
        decision.root / company_folder_name(provider.company) / year / period / Path(provider.destination_path)
    )
    destination_dir.mkdir(parents=True, exist_ok=True)
    destination_file = _safe_destination(destination_dir / filename)
    shutil.move(str(decision.source_file), str(destination_file))
    return ReviewResult(
        source_file=decision.source_file,
        destination_file=destination_file,
        provider=provider,
    )


def _safe_destination(path: Path) -> Path:
    if not path.exists():
        return path
    stem = path.stem
    suffix = path.suffix
    candidate = path.with_name(f"{stem}_new{suffix}")
    counter = 2
    while candidate.exists():
        candidate = path.with_name(f"{stem}_new{counter}{suffix}")
        counter += 1
    return candidate
