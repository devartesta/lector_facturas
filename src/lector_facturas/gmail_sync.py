from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from email.utils import parseaddr
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import json

from lector_facturas.review_notifications import GmailConfig, refresh_access_token


@dataclass(frozen=True)
class GmailAttachmentStub:
    filename: str
    mime_type: str
    attachment_id: str = ""


@dataclass(frozen=True)
class GmailMessageStub:
    message_id: str
    thread_id: str
    sender_email: str
    sender_display_name: str
    subject: str
    received_at: datetime
    attachments: tuple[GmailAttachmentStub, ...]


@dataclass(frozen=True)
class AttachmentClassification:
    is_invoice_like: bool
    reason: str


INVOICE_FILE_EXTENSIONS = {".pdf"}
NON_INVOICE_FILENAME_KEYWORDS = (
    "pyg",
    "rnt",
    "management_report",
    "management report",
    "account_transactions",
    "account transactions",
    "mayor",
    "cuadro",
    "sales_",
    "stock",
    "resumen",
    "draft",
    "borrador",
    "copy of",
)
NON_INVOICE_SUBJECT_KEYWORDS = (
    "rnt",
    "management report",
    "cierre",
    "auditor",
    "draft",
    "resumen",
)
INVOICE_FILENAME_HINTS = (
    "invoice",
    "factura",
    "receipt",
    "hetzner_",
    "vta",
    "fra.",
    "iee",
)
INVOICE_SUBJECT_HINTS = (
    "invoice",
    "factura",
    "rechnung",
    "receipt",
)
INVOICE_SENDER_HINTS = (
    "rgtmensajeros.com",
)


def list_messages_in_window(
    config: GmailConfig,
    *,
    from_at: datetime,
    to_at: datetime,
    max_messages: int = 1000,
) -> list[GmailMessageStub]:
    if from_at.tzinfo is None or to_at.tzinfo is None:
        raise ValueError("from_at and to_at must be timezone-aware datetimes.")
    access_token = refresh_access_token(config)
    query = _build_gmail_query(from_at=from_at, to_at=to_at)
    message_refs = _list_message_refs(config, access_token=access_token, query=query, max_messages=max_messages)

    messages: list[GmailMessageStub] = []
    for ref in message_refs:
        payload = _get_message_metadata(config, access_token=access_token, message_id=ref["id"])
        message = _payload_to_message_stub(payload)
        if from_at <= message.received_at <= to_at:
            messages.append(message)
    messages.sort(key=lambda item: (item.received_at, item.message_id))
    return messages


def classify_invoice_attachment(message: GmailMessageStub, attachment: GmailAttachmentStub) -> AttachmentClassification:
    suffix = _suffix(attachment.filename)
    if suffix not in INVOICE_FILE_EXTENSIONS:
        return AttachmentClassification(False, f"unsupported extension {suffix or '<none>'}")
    filename = attachment.filename.strip().lower()
    subject = message.subject.strip().lower()
    sender = message.sender_email.strip().lower()
    if any(keyword in filename for keyword in NON_INVOICE_FILENAME_KEYWORDS):
        return AttachmentClassification(False, "filename matches known non-invoice pattern")
    if any(keyword in subject for keyword in NON_INVOICE_SUBJECT_KEYWORDS):
        return AttachmentClassification(False, "subject matches known non-invoice pattern")
    if any(hint in sender for hint in INVOICE_SENDER_HINTS):
        return AttachmentClassification(True, "sender matches known invoice source")
    if any(hint in subject for hint in INVOICE_SUBJECT_HINTS):
        return AttachmentClassification(True, "subject has invoice hint")
    if any(hint in filename for hint in INVOICE_FILENAME_HINTS):
        return AttachmentClassification(True, "filename has invoice hint")
    if _looks_like_structured_invoice_number(filename):
        return AttachmentClassification(True, "filename looks like structured invoice number")
    return AttachmentClassification(False, "no invoice hint detected")


def looks_like_invoice_attachment(message: GmailMessageStub, attachment: GmailAttachmentStub) -> bool:
    return classify_invoice_attachment(message, attachment).is_invoice_like


def _build_gmail_query(*, from_at: datetime, to_at: datetime) -> str:
    after = (from_at - timedelta(days=1)).astimezone(UTC).strftime("%Y/%m/%d")
    before = (to_at + timedelta(days=1)).astimezone(UTC).strftime("%Y/%m/%d")
    return f"has:attachment after:{after} before:{before}"


def _list_message_refs(
    config: GmailConfig,
    *,
    access_token: str,
    query: str,
    max_messages: int,
) -> list[dict[str, str]]:
    refs: list[dict[str, str]] = []
    page_token = ""
    while len(refs) < max_messages:
        params = {
            "q": query,
            "maxResults": min(500, max_messages - len(refs)),
            "includeSpamTrash": "false",
        }
        if page_token:
            params["pageToken"] = page_token
        endpoint = (
            f"https://gmail.googleapis.com/gmail/v1/users/{config.user_id}/messages?"
            f"{urlencode(params)}"
        )
        payload = _gmail_get_json(endpoint, access_token=access_token)
        refs.extend(payload.get("messages", []))
        page_token = str(payload.get("nextPageToken", ""))
        if not page_token:
            break
    return refs


def _get_message_metadata(config: GmailConfig, *, access_token: str, message_id: str) -> dict[str, object]:
    params = urlencode([("format", "full")])
    endpoint = f"https://gmail.googleapis.com/gmail/v1/users/{config.user_id}/messages/{message_id}?{params}"
    return _gmail_get_json(endpoint, access_token=access_token)


def _gmail_get_json(endpoint: str, *, access_token: str) -> dict[str, object]:
    request = Request(
        endpoint,
        headers={"Authorization": f"Bearer {access_token}"},
        method="GET",
    )
    with urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def _payload_to_message_stub(payload: dict[str, object]) -> GmailMessageStub:
    headers = {
        str(header.get("name", "")).lower(): str(header.get("value", ""))
        for header in (payload.get("payload", {}) or {}).get("headers", [])
    }
    sender_name, sender_email = parseaddr(headers.get("from", ""))
    subject = headers.get("subject", "")
    internal_date_ms = int(str(payload.get("internalDate", "0") or "0"))
    received_at = datetime.fromtimestamp(internal_date_ms / 1000, tz=UTC)
    attachments = tuple(_walk_attachments((payload.get("payload", {}) or {})))
    return GmailMessageStub(
        message_id=str(payload.get("id", "")),
        thread_id=str(payload.get("threadId", "")),
        sender_email=sender_email,
        sender_display_name=sender_name,
        subject=subject,
        received_at=received_at,
        attachments=attachments,
    )


def _walk_attachments(part: dict[str, object]) -> list[GmailAttachmentStub]:
    attachments: list[GmailAttachmentStub] = []
    filename = str(part.get("filename", "") or "").strip()
    if filename:
        body = part.get("body", {}) or {}
        attachments.append(
            GmailAttachmentStub(
                filename=filename,
                mime_type=str(part.get("mimeType", "") or ""),
                attachment_id=str(body.get("attachmentId", "") or ""),
            )
        )
    for child in part.get("parts", []) or []:
        attachments.extend(_walk_attachments(child))
    return attachments


def _suffix(filename: str) -> str:
    lower = filename.lower()
    dot = lower.rfind(".")
    return lower[dot:] if dot >= 0 else ""


def _looks_like_structured_invoice_number(filename: str) -> bool:
    stem = filename.rsplit(".", 1)[0].replace("_", "").replace("-", "").replace(" ", "")
    digits = sum(char.isdigit() for char in stem)
    letters = sum(char.isalpha() for char in stem)
    return digits >= 5 and (letters >= 1 or stem.isdigit())


def download_attachment_bytes(
    config: GmailConfig,
    *,
    message_id: str,
    attachment_id: str,
) -> bytes:
    if not attachment_id:
        raise ValueError("attachment_id is required.")
    access_token = refresh_access_token(config)
    endpoint = (
        f"https://gmail.googleapis.com/gmail/v1/users/{config.user_id}/messages/"
        f"{message_id}/attachments/{attachment_id}"
    )
    payload = _gmail_get_json(endpoint, access_token=access_token)
    data = str(payload.get("data", "") or "")
    if not data:
        return b""
    padding = "=" * (-len(data) % 4)
    import base64

    return base64.urlsafe_b64decode(data + padding)
