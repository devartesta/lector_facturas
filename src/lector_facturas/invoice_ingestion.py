from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Callable
import re
import unicodedata
import uuid

from pypdf import PdfReader

from lector_facturas.google_drive import GoogleDriveClient
from lector_facturas.gmail_sync import GmailAttachmentStub, GmailMessageStub, classify_invoice_attachment
from lector_facturas.parsers.adobe import parse_adobe_pdf
from lector_facturas.parsers.adeplus import parse_adeplus_pdf
from lector_facturas.parsers.apphoto import parse_apphoto_pdf
from lector_facturas.parsers.artesta_income import parse_qhands_pdf, parse_rappel_pdf
from lector_facturas.parsers.artlink import parse_artlink_pdf
from lector_facturas.parsers.canva import parse_canva_pdf
from lector_facturas.parsers.claris import parse_claris_pdf
from lector_facturas.parsers.contasimple import parse_contasimple_pdf
from lector_facturas.parsers.continuum import parse_continuum_pdf
from lector_facturas.parsers.correos import parse_correos_pdf
from lector_facturas.parsers.delaware import parse_delaware_pdf
from lector_facturas.parsers.dct import parse_dct_pdf
from lector_facturas.parsers.godaddy import parse_godaddy_pdf
from lector_facturas.parsers.gls import parse_gls_ocr_text, parse_gls_pdf
from lector_facturas.parsers.googleworkspace import parse_googleworkspace_pdf
from lector_facturas.parsers.gorgias import parse_gorgias_pdf
from lector_facturas.parsers.hannun import parse_hannun_pdf
from lector_facturas.parsers.hetzner import parse_hetzner_pdf
from lector_facturas.parsers.hushed import parse_hushed_invoice_pdf, RECEIPT_FILENAME_RE as HUSHED_RECEIPT_RE
from lector_facturas.parsers.ipostal import parse_ipostal_pdf, parse_ipostal_text
from lector_facturas.parsers.jondo import parse_jondo_pdf
from lector_facturas.parsers.konvoai import parse_konvoai_pdf
from lector_facturas.parsers.lizenzero import parse_lizenzero_pdf
from lector_facturas.parsers.masmovil import parse_masmovil_pdf
from lector_facturas.parsers.marketing_ads import parse_google_ads_pdf, parse_meta_ads_pdf
from lector_facturas.parsers.microsoft import parse_microsoft_pdf
from lector_facturas.parsers.noda import parse_noda_pdf
from lector_facturas.parsers.openai import parse_openai_pdf
from lector_facturas.parsers.partner_income_fr import parse_choose_pdf, parse_toasty_pdf
from lector_facturas.parsers.portclearance import parse_portclearance_pdf
from lector_facturas.parsers.pressing import parse_pressing_pdf
from lector_facturas.parsers.proco import ProcoInvoice, parse_proco_bundle, parse_proco_pdf
from lector_facturas.parsers.producthero import parse_producthero_pdf
from lector_facturas.parsers.quickbooks import parse_quickbooks_pdf, parse_quickbooks_text
from lector_facturas.parsers.railway import parse_railway_pdf
from lector_facturas.parsers.regus import parse_regus_pdf
from lector_facturas.parsers.rever import parse_rever_pdf
from lector_facturas.parsers.shared_services import parse_shared_services_pdf
from lector_facturas.parsers.shopify import parse_shopify_pdf
from lector_facturas.parsers.spring import parse_spring_pdf
from lector_facturas.parsers.syncwith import parse_syncwith_pdf
from lector_facturas.parsers.tgi import parse_tgi_pdf
from lector_facturas.parsers.torras import parse_torras_pdf
from lector_facturas.parsers.ups import parse_ups_pdf
from lector_facturas.parsers.vitaly import parse_vitaly_pdf
from lector_facturas.parsers.yumaai import parse_yumaai_pdf
from lector_facturas.parsers.youraccountstaxes import parse_youraccountstaxes_pdf
from lector_facturas.review_workflow import company_folder_name, get_provider


VALIDATION_ROOT_PARTS = ("validation",)
VALIDATION_BUCKET_FOLDERS = {
    "no_invoice": ("validation", "no-invoice"),
    "to_check": ("validation", "to-check"),
    "to_process": ("validation", "to-process"),
    "proco_detail": ("validation", "proco-detail"),
}
COMPANY_CODES = {
    "ARTESTA STORE, S.L.": "SL",
    "ARTESTA STORES (UK) LTD": "LTD",
    "ARTESTA INC": "INC",
}


@dataclass(frozen=True)
class ParserRule:
    supplier_code: str
    parser_name: str
    parser: Callable[[Path], object]
    filename_contains: tuple[str, ...] = ()
    sender_contains: tuple[str, ...] = ()
    subject_contains: tuple[str, ...] = ()
    text_contains: tuple[str, ...] = ()
    filename_regexes: tuple[str, ...] = ()


@dataclass(frozen=True)
class IngestionResult:
    queue_item_id: str
    action: str
    summary_line: str
    validation_bucket: str = ""
    drive_file_id: str = ""
    drive_url: str = ""
    detected_supplier_code: str = ""
    detected_company_code: str = ""
    parser_name: str = ""
    document_id: str = ""
    error: str = ""


PARSER_RULES: tuple[ParserRule, ...] = (
    ParserRule("APPHOTOES", "apphoto", parse_apphoto_pdf, sender_contains=("apphoto.es",), filename_regexes=(r"^\d{4}-1-\d+\.pdf$",)),
    ParserRule("APPHOTOCAN", "apphoto", parse_apphoto_pdf, sender_contains=("apphoto.net",), filename_regexes=(r"^fra\.\d+\.pdf$",)),
    ParserRule("ADOBE", "adobe", parse_adobe_pdf, filename_contains=("iee",), sender_contains=("adobe",), text_contains=("adobe",)),
    ParserRule("ADEPLUS", "adeplus", parse_adeplus_pdf, filename_contains=("factura-",), text_contains=("adeplus consultores", "servicios integrales en proteccion de datos", "servicios integrales en protección de datos")),
    ParserRule(
        "ARTLINK",
        "artlink",
        parse_artlink_pdf,
        sender_contains=("artlink",),
        subject_contains=("artlink",),
        filename_contains=("freight cost", "invoice no 0002", "artesta stores ltd invoice no", "artesta store s.l invoice no"),
        text_contains=("artlink", "freight cost"),
    ),
    ParserRule(
        "CANVA",
        "canva",
        parse_canva_pdf,
        filename_contains=("canva",),
        filename_regexes=(r"^invoice-\d{5}-\d+.*\.pdf$",),
        sender_contains=("canva",),
        text_contains=("canva pty",),
    ),
    ParserRule("CHOOSE", "choose", parse_choose_pdf, sender_contains=("appchoose",), text_contains=("choose sas", "choose campaign")),
    ParserRule("CLARIS", "claris", parse_claris_pdf, filename_contains=("facturaf",), sender_contains=("claris",), text_contains=("claris gestio",)),
    ParserRule("CONTASIMPLE", "contasimple", parse_contasimple_pdf, filename_contains=("factura_es-",), text_contains=("cegid smb", "contasimple", "plan ultimate")),
    ParserRule("CONTINUUM", "continuum", parse_continuum_pdf, sender_contains=("continuum",), filename_contains=("continuum advisory", "from_continuum"), text_contains=("continuum advisory llc",)),
    ParserRule("CORREOSCAN", "correos", parse_correos_pdf, sender_contains=("correoscan",)),
    ParserRule("CORREOS", "correos", parse_correos_pdf, sender_contains=("correos",), filename_contains=("4004",), text_contains=("sociedad est. correos", "correos y tel")),
    ParserRule("DELAWARE", "delaware", parse_delaware_pdf, text_contains=("delaware corporate headquarters llc", "pennsylvania renewal filing",)),
    ParserRule("DCT", "dct", parse_dct_pdf, sender_contains=("dct.de",), filename_contains=("re_26-",)),
    ParserRule("GODADDY", "godaddy", parse_godaddy_pdf, sender_contains=("godaddy",), filename_contains=("godaddy",), text_contains=("godaddy", "go daddy")),
    ParserRule("GLS", "gls", parse_gls_pdf, sender_contains=("rgtmensajeros.com",), filename_contains=("escaneo",), text_contains=("rgt logistica", "rgt mensajeros")),
    ParserRule("GOOGLEADS", "google_ads", parse_google_ads_pdf, sender_contains=("collections@google.com",), text_contains=("google ads", "id de cuenta")),
    ParserRule("GOOGLEWORKSPACE", "googleworkspace", parse_googleworkspace_pdf, filename_contains=("gsuite", "google workspace", "googleworkspace"), sender_contains=("google",), text_contains=("google workspace", "google ireland limited")),
    ParserRule("GORGIAS", "gorgias", parse_gorgias_pdf, filename_contains=("gorgias", "invoice_inc-", "inc-12-"), sender_contains=("gorgias",), text_contains=("gorgias inc",)),
    ParserRule("QHANDS", "qhands", parse_qhands_pdf, filename_contains=("factura_2026-",), text_contains=("qhands design", "renting cnc")),
    ParserRule("YOURACCOUNTSTAXES", "youraccountstaxes", parse_youraccountstaxes_pdf, sender_contains=("youraccountsntaxes",), filename_contains=("inv-",), text_contains=("your accounts and taxes", "tax invoice", "total gbp")),
    ParserRule("HANNUN", "hannun_invoice", parse_hannun_pdf, filename_contains=("vta26-", "factura_h", "factura 2025-"), sender_contains=("hannun",), text_contains=("hannun",)),
    ParserRule("HETZNER", "hetzner", parse_hetzner_pdf, filename_contains=("hetzner",), text_contains=("hetzner online",)),
    ParserRule(
        "HUSHED", "hushed", parse_hushed_invoice_pdf,
        sender_contains=("hushed.com", "affinityclick"),
        subject_contains=("your receipt from hushed",),
        filename_contains=("receipt-",),
        text_contains=("hushed c/o affinityclick",),
    ),
    ParserRule("IPOSTAL", "ipostal", parse_ipostal_pdf, sender_contains=("ipostal",), filename_contains=("ipostal",), text_contains=("ipostal1", "factura para artesta inc", "identificaciones de correo propias")),
    ParserRule("JONDO", "jondo", parse_jondo_pdf, filename_regexes=(r"^as-\d+\.pdf$",), text_contains=("order invoice", "jondo uk", "po number: as-")),
    ParserRule("KONVOAI", "konvoai", parse_konvoai_pdf, filename_contains=("b5f7df3c",), sender_contains=("konvoai",), text_contains=("konvo ai",)),
    ParserRule("LIZENZERO", "lizenzero", parse_lizenzero_pdf, sender_contains=("lizenzero",), text_contains=("lizenzero", "interzero recycling alliance", "verpackungslizenz")),
    ParserRule("LIVITUM", "rappel", parse_rappel_pdf, filename_contains=("factura_a_",), text_contains=("home design labs", "rappel 2025")),
    ParserRule("MASMOVIL", "masmovil", parse_masmovil_pdf, sender_contains=("masmovil",), subject_contains=("masmovil",), text_contains=("xfera moviles", "masmovil negocios")),
    ParserRule("METAADS", "meta_ads", parse_meta_ads_pdf, sender_contains=("meta", "facebook"), text_contains=("meta platforms ireland", "facebook")),
    ParserRule("MICROSOFT", "microsoft", parse_microsoft_pdf, sender_contains=("microsoft",), filename_contains=("microsoft",), text_contains=("microsoft iberica", "numero de facturacion g")),
    ParserRule("NODA", "noda", parse_noda_pdf, sender_contains=("noda",), filename_contains=("020-26", "noda", "factura enero 2026"), text_contains=("asesoria fiscal noda", "noda y asociados")),
    ParserRule("OPENAI", "openai", parse_openai_pdf, filename_contains=("bzhjntub", "7bsdv5am", "invoice-bzhjntub", "invoice-7bsdv5am", "receipt-"), sender_contains=("openai",), text_contains=("openai", "chatgpt",)),
    ParserRule("PORTCLEARANCE", "portclearance", parse_portclearance_pdf, sender_contains=("port clearance", "portclearance"), filename_contains=("pcsi",), text_contains=("port clearance services",)),
    ParserRule("PRESSING", "pressing", parse_pressing_pdf, text_contains=("pressing impressi digital", "detalle en hoja excel adjunta")),
    ParserRule("PRODUCTHERO", "producthero", parse_producthero_pdf, sender_contains=("producthero",), filename_contains=("invoice_205588", "invoice_211723", "producthero"), text_contains=("producthero", "product hero")),
    ParserRule("PROCO", "proco", parse_proco_pdf, sender_contains=("precisionproco", "precision printing"), text_contains=("precision printing co. ltd", "direct mailing", "carriage", "postage")),
    ParserRule("RAILWAY", "railway", parse_railway_pdf, filename_contains=("1602c2f5",), sender_contains=("railway",)),
    ParserRule("QUICKBOOKS", "quickbooks", parse_quickbooks_pdf, sender_contains=("intuit", "quickbooks"), text_contains=("intuit inc.", "quickbooks online plus", "period for monthly fees")),
    ParserRule("REGUS", "regus", parse_regus_pdf, filename_contains=("3313-", "invoice("), sender_contains=("regus",)),
    ParserRule("REVER", "rever", parse_rever_pdf, filename_contains=("rvr-", "suppliednote", "invoice-rvr"), sender_contains=("rever",)),
    ParserRule("SHAREDSERVICESSL", "shared_services", parse_shared_services_pdf, filename_contains=("factura_202",), subject_contains=("factura_202",)),
    ParserRule("SHOPIFY", "shopify", parse_shopify_pdf, sender_contains=("shopify",), filename_contains=("shopify", "artesta_"), text_contains=("shopify",)),
    ParserRule("SYNCWITH", "syncwith", parse_syncwith_pdf, filename_contains=("invoice-tadgcdfs",), sender_contains=("syncwith",), text_contains=("syncwith inc", "hello@syncwith.com")),
    ParserRule("SPRINGGDS", "spring", parse_spring_pdf, filename_contains=("e260",), sender_contains=("spring",)),
    ParserRule("TGI", "tgi", parse_tgi_pdf, sender_contains=("tginc.com",), text_contains=("today's graphics inc", "tgi job")),
    ParserRule("TOASTY", "toasty", parse_toasty_pdf, sender_contains=("toasty",), text_contains=("toasty sas", "simon@toasty.family")),
    ParserRule("TORRAS", "torras", parse_torras_pdf, sender_contains=("torras",), filename_contains=("267f",), text_contains=("torras",), filename_regexes=(r"^7[-/]\d+.*\.pdf$",)),
    ParserRule("UPS", "ups", parse_ups_pdf, sender_contains=("ups.com",), subject_contains=("ups",), filename_contains=("rechnung",), text_contains=("united parcel service", "rechnungsnr.")),
    ParserRule("VITALY", "vitaly", parse_vitaly_pdf, sender_contains=("vitaly",), filename_contains=("ifc",), text_contains=("vitaly",)),
    ParserRule("YUMAAI", "yumaai", parse_yumaai_pdf, filename_contains=("oqxbyxmp",), sender_contains=("yumaai",), text_contains=("yuma",)),
)


def ensure_validation_folders(drive_client: GoogleDriveClient, *, root_folder_id: str) -> dict[str, dict[str, object]]:
    buckets: dict[str, dict[str, object]] = {}
    for bucket, parts in VALIDATION_BUCKET_FOLDERS.items():
        parent_id = root_folder_id
        current_folder: dict[str, object] | None = None
        for part in parts:
            current_folder = drive_client.ensure_folder(name=part, parent_id=parent_id)
            parent_id = str(current_folder["id"])
        buckets[bucket] = current_folder or {}
    return buckets


def build_review_filename(*, received_at: datetime, sender_email: str, original_filename: str) -> str:
    return (
        f"{received_at.astimezone(UTC).strftime('%Y%m%dT%H%M%SZ')}_"
        f"{safe_sender_fragment(sender_email)}_"
        f"{original_filename}"
    )


def safe_sender_fragment(sender_email: str) -> str:
    safe = re.sub(r"[^a-z0-9]+", "-", (sender_email or "unknown").strip().lower()).strip("-")
    return safe or "unknown"


def original_name_from_review_filename(stored_name: str) -> str:
    parts = stored_name.split("_", 2)
    return parts[2] if len(parts) == 3 else stored_name


def extract_pdf_text_for_detection(*, content: bytes, original_filename: str) -> str:
    suffix = Path(original_filename).suffix or ".pdf"
    with NamedTemporaryFile(delete=False, suffix=suffix) as handle:
        handle.write(content)
        temp_path = Path(handle.name)
    try:
        text_parts: list[str] = []
        reader = PdfReader(str(temp_path))
        for page in reader.pages[:2]:
            page_text = page.extract_text() or ""
            if page_text:
                text_parts.append(page_text)
        return "\n".join(text_parts)[:6000]
    except Exception:
        return ""
    finally:
        temp_path.unlink(missing_ok=True)


def detect_parser_rule(*, filename: str, sender_email: str, subject: str, pdf_text: str = "") -> ParserRule | None:
    filename_lower = _normalize_match_text(filename)
    sender_lower = _normalize_match_text(sender_email)
    subject_lower = _normalize_match_text(subject)
    pdf_text_lower = _normalize_match_text(pdf_text)
    for rule in PARSER_RULES:
        if any(_normalize_match_text(part) in sender_lower for part in rule.sender_contains):
            return rule
        if any(_normalize_match_text(part) in subject_lower for part in rule.subject_contains):
            return rule
        if any(_normalize_match_text(part) in pdf_text_lower for part in rule.text_contains):
            return rule
        if any(_normalize_match_text(part) in filename_lower for part in rule.filename_contains):
            return rule
        for pattern in rule.filename_regexes:
            if re.search(pattern, filename_lower, flags=re.IGNORECASE):
                return rule
    return None


def _normalize_match_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    return "".join(char for char in normalized if not unicodedata.combining(char)).lower()


def detect_parser_rule_with_ocr_fallback(
    *,
    drive_client: GoogleDriveClient,
    filename: str,
    sender_email: str,
    subject: str,
    content: bytes,
    pdf_text: str,
) -> tuple[ParserRule | None, str]:
    parser_rule = detect_parser_rule(
        filename=filename,
        sender_email=sender_email,
        subject=subject,
        pdf_text=pdf_text,
    )
    if parser_rule or pdf_text.strip():
        return parser_rule, pdf_text
    try:
        ocr_text = drive_client.ocr_pdf_to_text(name=filename, content=content)
    except Exception:
        return None, pdf_text
    parser_rule = detect_parser_rule(
        filename=filename,
        sender_email=sender_email,
        subject=subject,
        pdf_text=ocr_text,
    )
    return parser_rule, ocr_text


def parse_with_rule(
    rule: ParserRule,
    *,
    content: bytes,
    original_filename: str,
    drive_client: GoogleDriveClient | None = None,
    pdf_text: str = "",
):
    if rule.supplier_code == "GLS":
        ocr_text = pdf_text
        if not ocr_text.strip():
            if drive_client is None:
                raise ValueError("GLS OCR requires Google Drive client.")
            ocr_text = drive_client.ocr_pdf_to_text(name=original_filename, content=content)
        return parse_gls_ocr_text(ocr_text, original_filename=original_filename)
    if rule.supplier_code == "IPOSTAL":
        ocr_text = pdf_text
        if not ocr_text.strip():
            if drive_client is None:
                raise ValueError("iPostal OCR requires Google Drive client.")
            ocr_text = drive_client.ocr_pdf_to_text(name=original_filename, content=content)
        return parse_ipostal_text(ocr_text, original_filename=original_filename)
    if rule.supplier_code == "QUICKBOOKS":
        ocr_text = pdf_text
        if not ocr_text.strip():
            if drive_client is None:
                raise ValueError("QuickBooks OCR requires Google Drive client.")
            ocr_text = drive_client.ocr_pdf_to_text(name=original_filename, content=content)
        return parse_quickbooks_text(ocr_text, original_filename=original_filename)
    if rule.supplier_code == "TGI":
        ocr_text = pdf_text
        if not ocr_text.strip():
            if drive_client is None:
                raise ValueError("TGI OCR requires Google Drive client.")
            ocr_text = drive_client.ocr_pdf_to_text(name=original_filename, content=content)
        from lector_facturas.parsers.tgi import parse_tgi_text  # noqa: PLC0415

        return parse_tgi_text(ocr_text, original_filename=original_filename)

    if rule.supplier_code == "HUSHED":
        # Extract receipt number from original filename e.g. "Receipt-2260-8475.pdf"
        rcpt_m = HUSHED_RECEIPT_RE.match(original_filename)
        receipt_number = rcpt_m.group(1) if rcpt_m else ""
        suffix = Path(original_filename).suffix or ".pdf"
        with NamedTemporaryFile(delete=False, suffix=suffix) as handle:
            handle.write(content)
            temp_path = Path(handle.name)
        try:
            return parse_hushed_invoice_pdf(temp_path, receipt_number=receipt_number)
        finally:
            temp_path.unlink(missing_ok=True)

    suffix = Path(original_filename).suffix or ".pdf"
    with NamedTemporaryFile(delete=False, suffix=suffix) as handle:
        handle.write(content)
        temp_path = Path(handle.name)
    try:
        return rule.parser(temp_path)
    finally:
        temp_path.unlink(missing_ok=True)


def validate_parsed_invoice(parsed) -> str | None:
    if not getattr(parsed, "invoice_number", ""):
        return "missing invoice_number"
    if not getattr(parsed, "invoice_date", None):
        return "missing invoice_date"
    issuer_company_name = getattr(parsed, "issuer_company_name", "")
    billed_company_name = getattr(parsed, "billed_company_name", "")
    if not issuer_company_name:
        return "missing issuer_company_name"
    if billed_company_name not in COMPANY_CODES and issuer_company_name not in COMPANY_CODES:
        return f"unknown company routing: billed={billed_company_name}"
    if getattr(parsed, "gross_amount", None) is None or getattr(parsed, "net_amount", None) is None or getattr(parsed, "vat_amount", None) is None:
        return "missing amounts"
    parser_confidence = Decimal(str(getattr(parsed, "parser_confidence", "0")))
    if parser_confidence < Decimal("0.9850"):
        return f"low confidence: {parser_confidence}"
    return None


def effective_supplier_code(parsed, rule: ParserRule) -> str:
    if isinstance(parsed, list):
        return rule.supplier_code
    return str(getattr(parsed, "supplier_code", "") or rule.supplier_code)


def effective_company_name(parsed) -> str:
    billed_company_name = str(getattr(parsed, "billed_company_name", "") or "")
    issuer_company_name = str(getattr(parsed, "issuer_company_name", "") or "")
    if billed_company_name in COMPANY_CODES:
        return billed_company_name
    if issuer_company_name in COMPANY_CODES:
        return issuer_company_name
    return billed_company_name or issuer_company_name


def build_final_name(*, supplier_code: str, invoice_date, invoice_number: str, original_filename: str) -> str:
    extension = Path(original_filename).suffix.lower() or ".pdf"
    safe_invoice = invoice_number.replace("/", "-").replace("\\", "-").replace(" ", "")
    return f"{supplier_code}_{invoice_date.strftime('%Y%m%d')}_{safe_invoice}{extension}"


def build_windows_path(*, company_name: str, period_yyyymm: str, destination_path: str, filename: str) -> str:
    return "\\".join(
        [
            "ARTESTA - 6. Finances",
            company_folder_name(company_name),
            period_yyyymm[:4],
            period_yyyymm,
            *Path(destination_path).parts,
            filename,
        ]
    )


def ensure_drive_path(client: GoogleDriveClient, *, root_folder_id: str, windows_path: str) -> str:
    parent_id = root_folder_id
    parts = windows_path.split("\\")
    for folder_name in parts[1:-1]:
        folder = client.ensure_folder(name=folder_name, parent_id=parent_id)
        parent_id = str(folder["id"])
    return parent_id


def queue_item_id_for_email(*, message_id: str, attachment_id: str, original_filename: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"ingestion:email:{message_id}:{attachment_id}:{original_filename}"))


def queue_item_id_for_drive_file(file_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"ingestion:drive:{file_id}"))


def process_email_attachment(
    *,
    store,
    drive_client: GoogleDriveClient,
    root_folder_id: str,
    validation_folders: dict[str, dict[str, object]],
    message: GmailMessageStub,
    attachment: GmailAttachmentStub,
    content: bytes,
) -> IngestionResult:
    queue_item_id = queue_item_id_for_email(
        message_id=message.message_id,
        attachment_id=attachment.attachment_id,
        original_filename=attachment.filename,
    )
    pdf_text = extract_pdf_text_for_detection(content=content, original_filename=attachment.filename)
    heuristic = classify_invoice_attachment(message, attachment)
    if store.document_exists_exact(email_message_id=message.message_id, original_filename=attachment.filename):
        store.upsert_ingestion_queue_item(
            queue_item_id=queue_item_id,
            source="email_review",
            gmail_message_id=message.message_id,
            gmail_attachment_id=attachment.attachment_id,
            original_filename=attachment.filename,
            sender_email=message.sender_email,
            subject=message.subject,
            received_at=message.received_at,
            validation_bucket="ignored_duplicate",
            parse_status="not_attempted",
            heuristic_reason="exact duplicate by email_message_id + original_filename",
            mime_type=attachment.mime_type,
        )
        return IngestionResult(queue_item_id, "ignored_duplicate", _summary_line(message, attachment, "duplicada exacta"))

    if store.document_exists_by_original_filename(original_filename=attachment.filename) or store.document_exists_by_normalized_filename(original_filename=attachment.filename):
        store.upsert_ingestion_queue_item(
            queue_item_id=queue_item_id,
            source="email_review",
            gmail_message_id=message.message_id,
            gmail_attachment_id=attachment.attachment_id,
            original_filename=attachment.filename,
            sender_email=message.sender_email,
            subject=message.subject,
            received_at=message.received_at,
            validation_bucket="ignored_duplicate",
            parse_status="not_attempted",
            heuristic_reason="duplicate by filename",
            mime_type=attachment.mime_type,
        )
        return IngestionResult(queue_item_id, "ignored_duplicate", _summary_line(message, attachment, "duplicada por nombre"))

    parser_rule, pdf_text = detect_parser_rule_with_ocr_fallback(
        drive_client=drive_client,
        filename=attachment.filename,
        sender_email=message.sender_email,
        subject=message.subject,
        content=content,
        pdf_text=pdf_text,
    )
    if parser_rule:
        try:
            parsed = parse_with_rule(
                parser_rule,
                content=content,
                original_filename=attachment.filename,
                drive_client=drive_client,
                pdf_text=pdf_text,
            )
            detected_supplier_code = effective_supplier_code(parsed, parser_rule)

            # Multi-division invoices (Google Ads, Meta Ads)
            if isinstance(parsed, list) and parsed:
                first = parsed[0]
                company_name = effective_company_name(first)
                company_code = COMPANY_CODES.get(company_name, "")
                if not company_code:
                    raise ValueError(f"Unknown company for multi-division invoice: {company_name}")
                provider = get_provider(company_name, detected_supplier_code)
                final_name = build_final_name(
                    supplier_code=detected_supplier_code,
                    invoice_date=first.invoice_date,
                    invoice_number=first.invoice_number,
                    original_filename=attachment.filename,
                )
                windows_path = build_windows_path(
                    company_name=company_name,
                    period_yyyymm=first.period_yyyymm,
                    destination_path=provider.destination_path,
                    filename=final_name,
                )
                final_parent_id = ensure_drive_path(drive_client, root_folder_id=root_folder_id, windows_path=windows_path)
                drive_file = drive_client.upload_file(
                    name=final_name,
                    parent_id=final_parent_id,
                    content=content,
                    mime_type=attachment.mime_type or "application/pdf",
                )
                inserted_ids: list[str] = []
                for division_parsed in parsed:
                    if store.document_exists_by_business_key(
                        company_code=company_code,
                        supplier_code=detected_supplier_code,
                        invoice_number=division_parsed.invoice_number,
                        division_invoice=division_parsed.division_invoice,
                        document_type=getattr(division_parsed, "document_type", "invoice"),
                    ):
                        continue
                    doc_id = store.insert_document_from_parsed(
                        company_code=company_code,
                        supplier_code=detected_supplier_code,
                        parsed=division_parsed,
                        windows_path=windows_path,
                        drive_url=str(drive_file.get("webViewLink", "")),
                        drive_file_id=str(drive_file.get("id", "")),
                        original_filename=attachment.filename,
                        source_channel="email_review",
                        email_message_id=message.message_id,
                        email_thread_id=message.thread_id,
                        sender_email=message.sender_email,
                        source_subject=message.subject,
                        received_at=message.received_at,
                        review_notes="Auto-processed from email review (multi-division).",
                    )
                    inserted_ids.append(doc_id)
                store.upsert_ingestion_queue_item(
                    queue_item_id=queue_item_id,
                    source="email_review",
                    gmail_message_id=message.message_id,
                    gmail_attachment_id=attachment.attachment_id,
                    original_filename=attachment.filename,
                    sender_email=message.sender_email,
                    subject=message.subject,
                    received_at=message.received_at,
                    drive_file_id=str(drive_file.get("id", "")),
                    drive_url=str(drive_file.get("webViewLink", "")),
                    validation_bucket="auto_processed",
                    detected_supplier_code=detected_supplier_code,
                    detected_company_code=company_code,
                    parser_name=parser_rule.parser_name,
                    parse_status="parsed",
                    document_id=inserted_ids[0] if inserted_ids else "",
                    mime_type=attachment.mime_type,
                    heuristic_reason=f"auto-processed with parser {parser_rule.parser_name} (multi-division {len(parsed)} rows)",
                )
                divisions = ", ".join(p.division_invoice for p in parsed)
                return IngestionResult(
                    queue_item_id=queue_item_id,
                    action="auto_processed",
                    summary_line=_summary_line(message, attachment, f"procesada como {detected_supplier_code} [{divisions}]"),
                    detected_supplier_code=detected_supplier_code,
                    detected_company_code=company_code,
                    parser_name=parser_rule.parser_name,
                    drive_file_id=str(drive_file.get("id", "")),
                    drive_url=str(drive_file.get("webViewLink", "")),
                    document_id=inserted_ids[0] if inserted_ids else "",
                )

            validation_error = validate_parsed_invoice(parsed)
            company_name = effective_company_name(parsed)
            company_code = COMPANY_CODES.get(company_name, "")
            division_invoice = getattr(parsed, "division_invoice", "")
            document_type = getattr(parsed, "document_type", "invoice")
            if validation_error is None and company_code and not store.document_exists_by_business_key(
                company_code=company_code,
                supplier_code=detected_supplier_code,
                invoice_number=parsed.invoice_number,
                division_invoice=division_invoice,
                document_type=document_type,
            ):
                provider = get_provider(company_name, detected_supplier_code)
                final_name = build_final_name(
                    supplier_code=detected_supplier_code,
                    invoice_date=parsed.invoice_date,
                    invoice_number=parsed.invoice_number,
                    original_filename=attachment.filename,
                )
                windows_path = build_windows_path(
                    company_name=company_name,
                    period_yyyymm=parsed.period_yyyymm,
                    destination_path=provider.destination_path,
                    filename=final_name,
                )
                final_parent_id = ensure_drive_path(drive_client, root_folder_id=root_folder_id, windows_path=windows_path)
                drive_file = drive_client.upload_file(
                    name=final_name,
                    parent_id=final_parent_id,
                    content=content,
                    mime_type=attachment.mime_type or "application/pdf",
                )
                document_id = store.insert_document_from_parsed(
                    company_code=company_code,
                    supplier_code=detected_supplier_code,
                    parsed=parsed,
                    windows_path=windows_path,
                    drive_url=str(drive_file.get("webViewLink", "")),
                    drive_file_id=str(drive_file.get("id", "")),
                    original_filename=attachment.filename,
                    source_channel="email_review",
                    email_message_id=message.message_id,
                    email_thread_id=message.thread_id,
                    sender_email=message.sender_email,
                    source_subject=message.subject,
                    received_at=message.received_at,
                    review_notes="Auto-processed from email review.",
                )
                store.upsert_ingestion_queue_item(
                    queue_item_id=queue_item_id,
                    source="email_review",
                    gmail_message_id=message.message_id,
                    gmail_attachment_id=attachment.attachment_id,
                    original_filename=attachment.filename,
                    sender_email=message.sender_email,
                    subject=message.subject,
                    received_at=message.received_at,
                    drive_file_id=str(drive_file.get("id", "")),
                    drive_url=str(drive_file.get("webViewLink", "")),
                    validation_bucket="auto_processed",
                    detected_supplier_code=detected_supplier_code,
                    detected_company_code=company_code,
                    parser_name=parser_rule.parser_name,
                    parse_status="parsed",
                    document_id=document_id,
                    mime_type=attachment.mime_type,
                    heuristic_reason=f"auto-processed with parser {parser_rule.parser_name}",
                )
                return IngestionResult(
                    queue_item_id=queue_item_id,
                    action="auto_processed",
                    summary_line=_summary_line(message, attachment, f"procesada como {detected_supplier_code}"),
                    detected_supplier_code=detected_supplier_code,
                    detected_company_code=company_code,
                    parser_name=parser_rule.parser_name,
                    drive_file_id=str(drive_file.get("id", "")),
                    drive_url=str(drive_file.get("webViewLink", "")),
                    document_id=document_id,
                )
            if validation_error is None and company_code:
                store.upsert_ingestion_queue_item(
                    queue_item_id=queue_item_id,
                    source="email_review",
                    gmail_message_id=message.message_id,
                    gmail_attachment_id=attachment.attachment_id,
                    original_filename=attachment.filename,
                    sender_email=message.sender_email,
                    subject=message.subject,
                    received_at=message.received_at,
                    validation_bucket="ignored_duplicate",
                    detected_supplier_code=detected_supplier_code,
                    detected_company_code=company_code,
                    parser_name=parser_rule.parser_name,
                    parse_status="parsed",
                    mime_type=attachment.mime_type,
                    heuristic_reason="duplicate by business key",
                )
                return IngestionResult(queue_item_id, "ignored_duplicate", _summary_line(message, attachment, "duplicada por factura"))
            return _send_to_validation_bucket(
                store=store,
                drive_client=drive_client,
                folder_id=str(validation_folders["to_check"]["id"]),
                queue_item_id=queue_item_id,
                source="email_review",
                content=content,
                message=message,
                attachment=attachment,
                bucket="to_check",
                parse_status="failed" if validation_error else "manual_pending",
                heuristic_reason=validation_error or "parser detected but parse not reliable",
                detected_supplier_code=detected_supplier_code,
                parser_name=parser_rule.parser_name,
            )
        except Exception as exc:  # noqa: BLE001
            return _send_to_validation_bucket(
                store=store,
                drive_client=drive_client,
                folder_id=str(validation_folders["to_check"]["id"]),
                queue_item_id=queue_item_id,
                source="email_review",
                content=content,
                message=message,
                attachment=attachment,
                bucket="to_check",
                parse_status="failed",
                heuristic_reason=str(exc),
                detected_supplier_code=parser_rule.supplier_code,
                parser_name=parser_rule.parser_name,
            )

    target_bucket = "to_check" if heuristic.is_invoice_like else "no_invoice"
    parse_status = "manual_pending" if heuristic.is_invoice_like else "not_attempted"
    return _send_to_validation_bucket(
        store=store,
        drive_client=drive_client,
        folder_id=str(validation_folders[target_bucket]["id"]),
        queue_item_id=queue_item_id,
        source="email_review",
        content=content,
        message=message,
        attachment=attachment,
        bucket=target_bucket,
        parse_status=parse_status,
        heuristic_reason=heuristic.reason,
    )


def stage_email_attachment(
    *,
    store,
    drive_client: GoogleDriveClient,
    validation_folders: dict[str, dict[str, object]],
    message: GmailMessageStub,
    attachment: GmailAttachmentStub,
    content: bytes,
) -> IngestionResult:
    queue_item_id = queue_item_id_for_email(
        message_id=message.message_id,
        attachment_id=attachment.attachment_id,
        original_filename=attachment.filename,
    )
    if store.document_exists_exact(email_message_id=message.message_id, original_filename=attachment.filename):
        store.upsert_ingestion_queue_item(
            queue_item_id=queue_item_id,
            source="email_download",
            gmail_message_id=message.message_id,
            gmail_attachment_id=attachment.attachment_id,
            original_filename=attachment.filename,
            sender_email=message.sender_email,
            subject=message.subject,
            received_at=message.received_at,
            validation_bucket="ignored_duplicate",
            parse_status="not_attempted",
            heuristic_reason="exact duplicate by email_message_id + original_filename",
            mime_type=attachment.mime_type,
        )
        return IngestionResult(queue_item_id, "ignored_duplicate", _summary_line(message, attachment, "duplicada exacta"))

    if store.document_exists_by_original_filename(original_filename=attachment.filename) or store.document_exists_by_normalized_filename(original_filename=attachment.filename):
        store.upsert_ingestion_queue_item(
            queue_item_id=queue_item_id,
            source="email_download",
            gmail_message_id=message.message_id,
            gmail_attachment_id=attachment.attachment_id,
            original_filename=attachment.filename,
            sender_email=message.sender_email,
            subject=message.subject,
            received_at=message.received_at,
            validation_bucket="ignored_duplicate",
            parse_status="not_attempted",
            heuristic_reason="duplicate by filename",
            mime_type=attachment.mime_type,
        )
        return IngestionResult(queue_item_id, "ignored_duplicate", _summary_line(message, attachment, "duplicada por nombre"))

    heuristic = classify_invoice_attachment(message, attachment)
    target_bucket = "to_process" if heuristic.is_invoice_like else "no_invoice"
    return _send_to_validation_bucket(
        store=store,
        drive_client=drive_client,
        folder_id=str(validation_folders[target_bucket]["id"]),
        queue_item_id=queue_item_id,
        source="email_download",
        content=content,
        message=message,
        attachment=attachment,
        bucket=target_bucket,
        parse_status="not_attempted",
        heuristic_reason=heuristic.reason,
    )


def process_validation_drive_file(
    *,
    store,
    drive_client: GoogleDriveClient,
    root_folder_id: str,
    validation_folders: dict[str, dict[str, object]],
    file_item: dict[str, object],
) -> IngestionResult:
    file_id = str(file_item.get("id", ""))
    stored_name = str(file_item.get("name", ""))
    original_filename = original_name_from_review_filename(stored_name)
    queue_item_id = queue_item_id_for_drive_file(file_id)
    content = drive_client.download_file_bytes(file_id=file_id)
    pdf_text = extract_pdf_text_for_detection(content=content, original_filename=original_filename)
    parser_rule, pdf_text = detect_parser_rule_with_ocr_fallback(
        drive_client=drive_client,
        filename=original_filename,
        sender_email=stored_name,
        subject=stored_name,
        content=content,
        pdf_text=pdf_text,
    )
    if not parser_rule:
        drive_client.move_file(file_id=file_id, new_parent_id=str(validation_folders["to_check"]["id"]))
        store.upsert_ingestion_queue_item(
            queue_item_id=queue_item_id,
            source="manual_to_process",
            original_filename=original_filename,
            stored_filename=stored_name,
            drive_file_id=file_id,
            drive_url=str(file_item.get("webViewLink", "")),
            validation_bucket="to_check",
            parse_status="failed",
            parse_error="No parser detected from validation/to-process",
            heuristic_reason="No parser detected from validation/to-process",
            mime_type=str(file_item.get("mimeType", "")),
        )
        return IngestionResult(queue_item_id, "returned_to_to_check", f"{stored_name} | sin parser", validation_bucket="to_check", error="No parser detected")

    try:
        parsed = parse_with_rule(
            parser_rule,
            content=content,
            original_filename=original_filename,
            drive_client=drive_client,
            pdf_text=pdf_text,
        )
        detected_supplier_code = effective_supplier_code(parsed, parser_rule)

        # Multi-division invoices (e.g. marketing: Google Ads, Meta Ads)
        if isinstance(parsed, list) and parsed:
            first = parsed[0]
            company_name = effective_company_name(first)
            company_code = COMPANY_CODES.get(company_name, "")
            if not company_code:
                raise ValueError(f"Unknown company for multi-division invoice: {company_name}")
            document_type = getattr(first, "document_type", "invoice")
            provider = get_provider(company_name, detected_supplier_code)
            final_name = build_final_name(
                supplier_code=detected_supplier_code,
                invoice_date=first.invoice_date,
                invoice_number=first.invoice_number,
                original_filename=original_filename,
            )
            windows_path = build_windows_path(
                company_name=company_name,
                period_yyyymm=first.period_yyyymm,
                destination_path=provider.destination_path,
                filename=final_name,
            )
            final_parent_id = ensure_drive_path(drive_client, root_folder_id=root_folder_id, windows_path=windows_path)
            drive_client.update_file_name(file_id=file_id, name=final_name)
            moved = drive_client.move_file(file_id=file_id, new_parent_id=final_parent_id)
            drive_url_final = str(moved.get("webViewLink", ""))
            drive_file_id_final = str(moved.get("id", ""))
            inserted_ids: list[str] = []
            for division_parsed in parsed:
                if store.document_exists_by_business_key(
                    company_code=company_code,
                    supplier_code=detected_supplier_code,
                    invoice_number=division_parsed.invoice_number,
                    division_invoice=division_parsed.division_invoice,
                    document_type=document_type,
                ):
                    continue
                doc_id = store.insert_document_from_parsed(
                    company_code=company_code,
                    supplier_code=detected_supplier_code,
                    parsed=division_parsed,
                    windows_path=windows_path,
                    drive_url=drive_url_final,
                    drive_file_id=drive_file_id_final,
                    original_filename=original_filename,
                    source_channel="manual_to_process",
                    sender_email="",
                    source_subject="validation/to-process",
                    review_notes="Processed from validation/to-process (multi-division).",
                )
                inserted_ids.append(doc_id)
            store.upsert_ingestion_queue_item(
                queue_item_id=queue_item_id,
                source="manual_to_process",
                original_filename=original_filename,
                stored_filename=final_name,
                drive_file_id=drive_file_id_final,
                drive_url=drive_url_final,
                validation_bucket="auto_processed",
                detected_supplier_code=detected_supplier_code,
                detected_company_code=company_code,
                parser_name=parser_rule.parser_name,
                parse_status="parsed",
                document_id=inserted_ids[0] if inserted_ids else "",
                mime_type=str(file_item.get("mimeType", "")),
                heuristic_reason=f"processed from validation/to-process (multi-division {len(parsed)} rows)",
            )
            divisions = ", ".join(p.division_invoice for p in parsed)
            return IngestionResult(queue_item_id, "processed_from_to_process", f"{original_filename} | {detected_supplier_code} [{divisions}]", document_id=inserted_ids[0] if inserted_ids else "")

        validation_error = validate_parsed_invoice(parsed)
        company_name = effective_company_name(parsed)
        company_code = COMPANY_CODES.get(company_name, "")
        document_type = getattr(parsed, "document_type", "invoice")
        if validation_error or not company_code:
            raise ValueError(validation_error or "Unknown company")

        if isinstance(parsed, ProcoInvoice):
            # Attempt to upgrade parse with Excel detail if available
            proco_detail_folder_id = str(validation_folders.get("proco_detail", {}).get("id", ""))
            excel_file_id_to_trash: str | None = None
            if proco_detail_folder_id:
                excel_files = [
                    f for f in drive_client.list_files(parent_id=proco_detail_folder_id)
                    if Path(str(f.get("name", ""))).suffix.lower() in {".xlsx", ".xls"}
                ]
                if excel_files:
                    excel_file = excel_files[0]
                    excel_file_id_to_trash = str(excel_file.get("id", ""))
                    excel_content = drive_client.download_file_bytes(file_id=excel_file_id_to_trash)
                    with NamedTemporaryFile(delete=False, suffix=".pdf") as pdf_h:
                        pdf_h.write(content)
                        pdf_tmp = Path(pdf_h.name)
                    with NamedTemporaryFile(delete=False, suffix=".xlsx") as xlsx_h:
                        xlsx_h.write(excel_content)
                        xlsx_tmp = Path(xlsx_h.name)
                    try:
                        parsed = parse_proco_bundle(pdf_tmp, xlsx_tmp)
                    finally:
                        pdf_tmp.unlink(missing_ok=True)
                        xlsx_tmp.unlink(missing_ok=True)

            mfg_exists = store.document_exists_by_business_key(
                company_code=company_code,
                supplier_code=detected_supplier_code,
                invoice_number=parsed.invoice_number,
                division_invoice="manufacturing",
                document_type=document_type,
            )
            log_exists = store.document_exists_by_business_key(
                company_code=company_code,
                supplier_code=detected_supplier_code,
                invoice_number=parsed.invoice_number,
                division_invoice="logistics",
                document_type=document_type,
            )
            if mfg_exists and log_exists:
                drive_client.trash_file(file_id=file_id)
                if excel_file_id_to_trash:
                    drive_client.trash_file(file_id=excel_file_id_to_trash)
                store.upsert_ingestion_queue_item(
                    queue_item_id=queue_item_id,
                    source="manual_to_process",
                    original_filename=original_filename,
                    stored_filename=stored_name,
                    drive_file_id=file_id,
                    drive_url=str(file_item.get("webViewLink", "")),
                    validation_bucket="ignored_duplicate",
                    detected_supplier_code=detected_supplier_code,
                    detected_company_code=company_code,
                    parser_name=parser_rule.parser_name,
                    parse_status="parsed",
                    heuristic_reason="PROCO duplicate by business key from to-process",
                    mime_type=str(file_item.get("mimeType", "")),
                )
                return IngestionResult(queue_item_id, "ignored_duplicate", f"{stored_name} | PROCO duplicada", detected_supplier_code=detected_supplier_code)

            provider = get_provider(company_name, detected_supplier_code)
            final_name = build_final_name(
                supplier_code=detected_supplier_code,
                invoice_date=parsed.invoice_date,
                invoice_number=parsed.invoice_number,
                original_filename=original_filename,
            )
            windows_path = build_windows_path(
                company_name=company_name,
                period_yyyymm=parsed.period_yyyymm,
                destination_path=provider.destination_path,
                filename=final_name,
            )
            final_parent_id = ensure_drive_path(drive_client, root_folder_id=root_folder_id, windows_path=windows_path)
            drive_client.update_file_name(file_id=file_id, name=final_name)
            moved = drive_client.move_file(file_id=file_id, new_parent_id=final_parent_id)
            drive_url_final = str(moved.get("webViewLink", ""))
            drive_file_id_final = str(moved.get("id", ""))

            document_id: str | None = None
            if not mfg_exists:
                document_id = store.insert_document_from_parsed(
                    company_code=company_code,
                    supplier_code=detected_supplier_code,
                    parsed=parsed,
                    windows_path=windows_path,
                    drive_url=drive_url_final,
                    drive_file_id=drive_file_id_final,
                    original_filename=original_filename,
                    source_channel="manual_to_process",
                    source_subject="validation/to-process",
                    review_notes="Processed from validation/to-process (PROCO manufacturing).",
                    division_invoice_override="manufacturing",
                    gross_amount_override=parsed.manufacturing_gross_amount,
                    net_amount_override=parsed.manufacturing_net_amount,
                    vat_amount_override=parsed.manufacturing_vat_amount,
                )
            if not log_exists:
                log_doc_id = store.insert_document_from_parsed(
                    company_code=company_code,
                    supplier_code=detected_supplier_code,
                    parsed=parsed,
                    windows_path=windows_path,
                    drive_url=drive_url_final,
                    drive_file_id=drive_file_id_final,
                    original_filename=original_filename,
                    source_channel="manual_to_process",
                    source_subject="validation/to-process",
                    review_notes="Processed from validation/to-process (PROCO logistics).",
                    division_invoice_override="logistics",
                    gross_amount_override=parsed.logistics_gross_amount,
                    net_amount_override=parsed.logistics_net_amount,
                    vat_amount_override=parsed.logistics_vat_amount,
                )
                if document_id is None:
                    document_id = log_doc_id

            if excel_file_id_to_trash:
                drive_client.trash_file(file_id=excel_file_id_to_trash)

            store.upsert_ingestion_queue_item(
                queue_item_id=queue_item_id,
                source="manual_to_process",
                original_filename=original_filename,
                stored_filename=final_name,
                drive_file_id=drive_file_id_final,
                drive_url=drive_url_final,
                validation_bucket="auto_processed",
                detected_supplier_code=detected_supplier_code,
                detected_company_code=company_code,
                parser_name=parser_rule.parser_name,
                parse_status="parsed",
                document_id=document_id or "",
                mime_type=str(file_item.get("mimeType", "")),
                heuristic_reason="processed from validation/to-process (PROCO split)",
            )
            return IngestionResult(queue_item_id, "processed_from_to_process", f"{original_filename} | PROCO manufacturing+logistics", document_id=document_id or "")

        division_invoice = getattr(parsed, "division_invoice", "")
        if store.document_exists_by_business_key(
            company_code=company_code,
            supplier_code=detected_supplier_code,
            invoice_number=parsed.invoice_number,
            division_invoice=division_invoice,
            document_type=document_type,
        ):
            drive_client.trash_file(file_id=file_id)
            store.upsert_ingestion_queue_item(
                queue_item_id=queue_item_id,
                source="manual_to_process",
                original_filename=original_filename,
                stored_filename=stored_name,
                drive_file_id=file_id,
                drive_url=str(file_item.get("webViewLink", "")),
                validation_bucket="ignored_duplicate",
                detected_supplier_code=detected_supplier_code,
                detected_company_code=company_code,
                parser_name=parser_rule.parser_name,
                parse_status="parsed",
                heuristic_reason="duplicate by business key from to-process",
                mime_type=str(file_item.get("mimeType", "")),
            )
            return IngestionResult(queue_item_id, "ignored_duplicate", f"{stored_name} | duplicada", detected_supplier_code=detected_supplier_code)

        provider = get_provider(company_name, detected_supplier_code)
        final_name = build_final_name(
            supplier_code=detected_supplier_code,
            invoice_date=parsed.invoice_date,
            invoice_number=parsed.invoice_number,
            original_filename=original_filename,
        )
        windows_path = build_windows_path(
            company_name=company_name,
            period_yyyymm=parsed.period_yyyymm,
            destination_path=provider.destination_path,
            filename=final_name,
        )
        final_parent_id = ensure_drive_path(drive_client, root_folder_id=root_folder_id, windows_path=windows_path)
        drive_client.update_file_name(file_id=file_id, name=final_name)
        moved = drive_client.move_file(file_id=file_id, new_parent_id=final_parent_id)
        document_id = store.insert_document_from_parsed(
            company_code=company_code,
            supplier_code=detected_supplier_code,
            parsed=parsed,
            windows_path=windows_path,
            drive_url=str(moved.get("webViewLink", "")),
            drive_file_id=str(moved.get("id", "")),
            original_filename=original_filename,
            source_channel="manual_to_process",
            sender_email="",
            source_subject="validation/to-process",
            review_notes="Processed from validation/to-process.",
        )
        store.upsert_ingestion_queue_item(
            queue_item_id=queue_item_id,
            source="manual_to_process",
            original_filename=original_filename,
            stored_filename=final_name,
            drive_file_id=str(moved.get("id", "")),
            drive_url=str(moved.get("webViewLink", "")),
            validation_bucket="auto_processed",
            detected_supplier_code=detected_supplier_code,
            detected_company_code=company_code,
            parser_name=parser_rule.parser_name,
            parse_status="parsed",
            document_id=document_id,
            mime_type=str(file_item.get("mimeType", "")),
            heuristic_reason="processed from validation/to-process",
        )
        return IngestionResult(queue_item_id, "processed_from_to_process", f"{original_filename} | procesada como {detected_supplier_code}", document_id=document_id)
    except Exception as exc:  # noqa: BLE001
        drive_client.move_file(file_id=file_id, new_parent_id=str(validation_folders["to_check"]["id"]))
        store.upsert_ingestion_queue_item(
            queue_item_id=queue_item_id,
            source="manual_to_process",
            original_filename=original_filename,
            stored_filename=stored_name,
            drive_file_id=file_id,
            drive_url=str(file_item.get("webViewLink", "")),
            validation_bucket="to_check",
            detected_supplier_code=parser_rule.supplier_code,
            parser_name=parser_rule.parser_name,
            parse_status="failed",
            parse_error=str(exc),
            mime_type=str(file_item.get("mimeType", "")),
            heuristic_reason=str(exc),
        )
        return IngestionResult(queue_item_id, "returned_to_to_check", f"{original_filename} | error: {exc}", validation_bucket="to_check", error=str(exc))


def _send_to_validation_bucket(
    *,
    store,
    drive_client: GoogleDriveClient,
    folder_id: str,
    queue_item_id: str,
    source: str,
    content: bytes,
    message: GmailMessageStub,
    attachment: GmailAttachmentStub,
    bucket: str,
    parse_status: str,
    heuristic_reason: str,
    detected_supplier_code: str = "",
    parser_name: str = "",
) -> IngestionResult:
    stored_name = build_review_filename(
        received_at=message.received_at,
        sender_email=message.sender_email,
        original_filename=attachment.filename,
    )
    drive_file = drive_client.ensure_file(
        name=stored_name,
        parent_id=folder_id,
        content=content,
        mime_type=attachment.mime_type or "application/pdf",
    )
    store.upsert_ingestion_queue_item(
        queue_item_id=queue_item_id,
        source=source,
        gmail_message_id=message.message_id,
        gmail_attachment_id=attachment.attachment_id,
        original_filename=attachment.filename,
        stored_filename=stored_name,
        sender_email=message.sender_email,
        subject=message.subject,
        received_at=message.received_at,
        drive_file_id=str(drive_file.get("id", "")),
        drive_url=str(drive_file.get("webViewLink", "")),
        validation_bucket=bucket,
        detected_supplier_code=detected_supplier_code,
        parser_name=parser_name,
        parse_status=parse_status,
        parse_error=heuristic_reason if parse_status == "failed" else "",
        mime_type=attachment.mime_type,
        heuristic_reason=heuristic_reason,
    )
    return IngestionResult(
        queue_item_id=queue_item_id,
        action=bucket,
        summary_line=_summary_line(message, attachment, heuristic_reason),
        validation_bucket=bucket,
        drive_file_id=str(drive_file.get("id", "")),
        drive_url=str(drive_file.get("webViewLink", "")),
        detected_supplier_code=detected_supplier_code,
        parser_name=parser_name,
        error=heuristic_reason if parse_status == "failed" else "",
    )


PAYROLL_SENDER_KEYWORDS = ("dosconsulting",)


def _classify_payroll_document_type(filename: str) -> str:
    name = filename.lower()
    if "resumen" in name:
        return "resumen_nomina"
    if "nomina" in name or "nómina" in name:
        return "nominas"
    return "detalle"


def _build_payroll_stored_name(*, document_type: str, period_yyyymm: str, company_code: str, original_filename: str) -> str:
    suffix = Path(original_filename).suffix or ".pdf"
    type_map = {"nominas": "NOMINAS", "resumen_nomina": "NOMINAS_RESUMEN", "detalle": "NOMINAS_DETALLE"}
    label = type_map.get(document_type, "NOMINAS_DOC")
    return f"{label}_{period_yyyymm}_{company_code}{suffix}"


def stage_payroll_attachment(
    *,
    store,
    drive_client: GoogleDriveClient,
    payroll_folder_id: str,
    message: GmailMessageStub,
    attachment: GmailAttachmentStub,
    content: bytes,
    period_yyyymm: str,
    company_code: str,
) -> IngestionResult:
    queue_item_id = queue_item_id_for_email(
        message_id=message.message_id,
        attachment_id=attachment.attachment_id,
        original_filename=attachment.filename,
    )
    if store.payroll_document_exists(email_message_id=message.message_id, original_filename=attachment.filename):
        return IngestionResult(queue_item_id, "ignored_duplicate", f"payroll duplicate: {attachment.filename}")

    document_type = _classify_payroll_document_type(attachment.filename)
    stored_name = _build_payroll_stored_name(
        document_type=document_type,
        period_yyyymm=period_yyyymm,
        company_code=company_code,
        original_filename=attachment.filename,
    )

    uploaded = drive_client.upload_file(
        name=stored_name,
        parent_id=payroll_folder_id,
        content=content,
        mime_type="application/pdf",
    )

    windows_path = f"ARTESTA - 6. Finances\\Artesta Store, S.L\\{period_yyyymm[:4]}\\{period_yyyymm}\\expenses\\opex\\staff\\{stored_name}"

    store.insert_payroll_document(
        company_code=company_code,
        period_yyyymm=period_yyyymm,
        document_type=document_type,
        windows_path=windows_path,
        drive_url=str(uploaded.get("webViewLink", "")),
        drive_file_id=str(uploaded.get("id", "")),
        original_filename=attachment.filename,
        stored_filename=stored_name,
        sender_email=message.sender_email,
        source_channel="email_download",
        email_message_id=message.message_id,
    )
    return IngestionResult(queue_item_id, "payroll_archived", f"payroll: {stored_name}")


def _summary_line(message: GmailMessageStub, attachment: GmailAttachmentStub, suffix: str) -> str:
    return f"{message.received_at.isoformat()} | {attachment.filename} | {message.sender_email or '-'} | {message.subject or '-'} | {suffix}"
