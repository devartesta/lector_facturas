from __future__ import annotations

from dataclasses import dataclass
import re
import unicodedata


TAX_ID_LABEL_RE = re.compile(
    r"(?i)\b(?:"
    r"n\.?\s*i\.?\s*f\.?|c\.?\s*i\.?\s*f\.?|vat(?:\s*(?:id|no|number|registration\s*no))?|"
    r"tax\s*(?:id|number)|tin|ein|iva|n(?:umero|o|º)\s*(?:de\s*)?(?:iva|identificacion\s*fiscal)"
    r")\b\s*(?:[:#/\-]|\s+)?\s*([A-Z]{0,2}\s*[A-Z0-9][A-Z0-9 .\-]{5,24})"
)
STANDALONE_TAX_ID_RE = re.compile(
    r"(?i)\b("
    r"ES\s*[A-Z]\s*\d{7}\s*[0-9A-Z]|"
    r"ES\s*\d{8}\s*[A-Z]|"
    r"GB\s*\d{9}(?:\s*\d{3})?|"
    r"IE\s*[0-9A-Z]{7,9}|"
    r"DE\s*\d{9}|"
    r"FR\s*[0-9A-Z]{2}\s*\d{9}|"
    r"IT\s*\d{11}|"
    r"NL\s*\d{9}\s*B\s*\d{2}|"
    r"[A-Z]\s*\d{7}\s*[0-9A-Z]|"
    r"\d{8}\s*[A-Z]|"
    r"\d{2}-\d{7}"
    r")\b"
)
VAT_COUNTRY_PREFIXES = {
    "AT",
    "BE",
    "BG",
    "CY",
    "CZ",
    "DE",
    "DK",
    "EE",
    "EL",
    "FI",
    "FR",
    "GB",
    "HR",
    "HU",
    "IE",
    "IT",
    "LT",
    "LU",
    "LV",
    "MT",
    "NL",
    "PL",
    "PT",
    "RO",
    "SE",
    "SI",
    "SK",
}


@dataclass(frozen=True)
class TaxIdBackfillResult:
    issuer_tax_id: str | None
    billed_tax_id: str | None
    confidence: str
    notes: str


@dataclass(frozen=True)
class _Candidate:
    value: str
    start: int
    end: int
    labelled: bool


def extract_document_tax_ids(
    text: str,
    *,
    issuer_company_name: str = "",
    billed_company_name: str = "",
) -> TaxIdBackfillResult:
    candidates = _find_tax_id_candidates(text)
    if not candidates:
        return TaxIdBackfillResult(None, None, "none", "no tax id candidates found")

    issuer = _candidate_near_company(text, candidates, issuer_company_name)
    billed = _candidate_near_company(text, candidates, billed_company_name)

    notes: list[str] = []
    if issuer:
        notes.append("issuer matched near issuer company name")
    if billed:
        notes.append("billed matched near billed company name")
    if (
        issuer
        and billed
        and issuer.value == billed.value
        and _normalize_match_text(issuer_company_name) != _normalize_match_text(billed_company_name)
    ):
        return TaxIdBackfillResult(
            None,
            None,
            "low",
            "same tax id matched issuer and billed company; left unset for manual review",
        )

    if issuer and billed:
        confidence = "high"
    elif issuer or billed:
        confidence = "medium"
    else:
        confidence = "low"
        notes.append("tax ids found but not close enough to company names")

    return TaxIdBackfillResult(
        issuer_tax_id=issuer.value if issuer else None,
        billed_tax_id=billed.value if billed else None,
        confidence=confidence,
        notes="; ".join(notes),
    )


def normalize_tax_id(value: str) -> str | None:
    cleaned = unicodedata.normalize("NFKD", value or "")
    cleaned = "".join(char for char in cleaned if not unicodedata.combining(char))
    cleaned = cleaned.upper()
    cleaned = re.sub(r"[^A-Z0-9-]", "", cleaned)
    cleaned = cleaned.strip("-")
    if not cleaned:
        return None
    if not any(char.isdigit() for char in cleaned):
        return None
    if re.fullmatch(r"\d{2}-\d{7}", cleaned):
        return cleaned
    cleaned = cleaned.replace("-", "")
    if cleaned.startswith("ES") and len(cleaned) in {11, 12}:
        maybe_es = cleaned[2:]
        if _is_spanish_tax_id(maybe_es):
            return maybe_es
    if _is_spanish_tax_id(cleaned):
        return cleaned
    if re.fullmatch(r"[A-Z]{2}[A-Z0-9]{7,12}", cleaned) and cleaned[:2] in VAT_COUNTRY_PREFIXES:
        return cleaned
    if re.fullmatch(r"\d{9,12}", cleaned):
        return cleaned
    return None


def _find_tax_id_candidates(text: str) -> list[_Candidate]:
    seen: set[tuple[str, int]] = set()
    candidates: list[_Candidate] = []
    for match in TAX_ID_LABEL_RE.finditer(text or ""):
        value = normalize_tax_id(match.group(1))
        if not value:
            continue
        key = (value, match.start(1))
        if key in seen:
            continue
        seen.add(key)
        candidates.append(_Candidate(value, match.start(1), match.end(1), labelled=True))
    for match in STANDALONE_TAX_ID_RE.finditer(text or ""):
        value = normalize_tax_id(match.group(1))
        if not value:
            continue
        if any(candidate.value == value and abs(candidate.start - match.start(1)) < 20 for candidate in candidates):
            continue
        key = (value, match.start(1))
        if key in seen:
            continue
        seen.add(key)
        candidates.append(_Candidate(value, match.start(1), match.end(1), labelled=False))
    return candidates


def _candidate_near_company(text: str, candidates: list[_Candidate], company_name: str) -> _Candidate | None:
    if not company_name.strip():
        return None
    normalized_text = _normalize_match_text(text)
    normalized_company = _normalize_match_text(company_name)
    if not normalized_company:
        return None
    spans = [(match.start(), match.end()) for match in re.finditer(re.escape(normalized_company), normalized_text)]
    if not spans:
        return None

    best: tuple[int, int, int, _Candidate] | None = None
    for candidate in candidates:
        for start, end in spans:
            if candidate.end < start:
                distance = start - candidate.end
                side = 1
            elif candidate.start > end:
                distance = candidate.start - end
                side = 0
            else:
                distance = 0
                side = 0
            if distance > 450:
                continue
            score = 0 if candidate.labelled else 1
            item = (score, side, distance, candidate)
            if best is None or item[:3] < best[:3]:
                best = item
    return best[3] if best else None


def _normalize_match_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    return normalized.lower()


def _is_spanish_tax_id(value: str) -> bool:
    return bool(
        re.fullmatch(r"[ABCDEFGHJNPQRSUVW]\d{7}[0-9A-J]", value)
        or re.fullmatch(r"\d{8}[A-Z]", value)
        or re.fullmatch(r"[XYZ]\d{7}[A-Z]", value)
    )
