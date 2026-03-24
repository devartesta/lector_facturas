"""Gmail API notifications for invoice review workflows."""

from __future__ import annotations

from base64 import urlsafe_b64encode
from dataclasses import dataclass
from datetime import datetime
from email.message import EmailMessage
from html import escape
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import json
import os


@dataclass(frozen=True)
class GmailConfig:
    client_id: str
    client_secret: str
    refresh_token: str
    sender: str
    recipients: tuple[str, ...]
    user_id: str = "me"


@dataclass(frozen=True)
class UnmatchedInvoiceNotice:
    company: str
    period_yyyymm: str
    source_sender: str
    source_subject: str
    attachment_names: tuple[str, ...]
    review_path: Path
    extracted_text: str = ""
    suggested_provider: str = ""
    review_url: str = ""


@dataclass(frozen=True)
class HistoricalInvoiceNotice:
    company: str
    invoice_year: int
    expected_year_from: int
    source_sender: str
    source_subject: str
    attachment_names: tuple[str, ...]
    review_path: Path
    invoice_number: str = ""
    invoice_date: str = ""
    extracted_text: str = ""
    review_url: str = ""


@dataclass(frozen=True)
class MissingExpectedInvoicesNotice:
    company: str
    period_yyyymm: str
    missing_items: tuple[str, ...]
    notes: str = ""


@dataclass(frozen=True)
class ProcessedInvoiceItem:
    filename: str
    supplier_code: str
    drive_url: str
    windows_path: str
    gross_amount: float | None
    currency_code: str
    invoice_number: str
    updated_at: datetime


@dataclass(frozen=True)
class NightlyReviewDigest:
    company: str
    period_yyyymm: str
    unmatched_supplier_items: tuple[UnmatchedInvoiceNotice, ...] = ()
    historical_invoice_items: tuple[HistoricalInvoiceNotice, ...] = ()
    missing_expected_items: tuple[str, ...] = ()
    loaded_invoice_items: tuple[str, ...] = ()
    pending_load_items: tuple[str, ...] = ()
    duplicate_items: tuple[str, ...] = ()
    to_check_items: tuple[str, ...] = ()
    no_invoice_items: tuple[str, ...] = ()
    processed_from_to_process_items: tuple[str, ...] = ()
    returned_to_to_check_items: tuple[str, ...] = ()
    processed_items: tuple[ProcessedInvoiceItem, ...] = ()
    notes: str = ""


def _build_message(subject: str, config: GmailConfig, lines: list[str]) -> EmailMessage:
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = config.sender
    message["To"] = ", ".join(config.recipients)
    message.set_content("\n".join(lines))
    return message


def _display_review_path(path: Path) -> str:
    parts = list(path.parts)
    try:
        idx = parts.index("ARTESTA - 6. Finances")
        return "\\".join(parts[idx:])
    except ValueError:
        return path.name


def _build_html_message(
    *,
    subject: str,
    config: GmailConfig,
    intro: str,
    badge: str,
    summary: list[tuple[str, str]],
    details: list[str],
    excerpt: str = "",
) -> EmailMessage:
    text_lines = [intro, ""]
    text_lines.extend(f"{label}: {value}" for label, value in summary)
    if details:
        text_lines.extend(["", *details])
    if excerpt:
        text_lines.extend(["", "Texto extraido:", excerpt[:1200]])

    message = _build_message(subject, config, text_lines)

    summary_html = "".join(
        f"<tr><td style='padding:4px 12px 4px 0;color:#6b7280;white-space:nowrap'>{escape(label)}</td>"
        f"<td style='padding:4px 0;color:#111827'><strong>{escape(value)}</strong></td></tr>"
        for label, value in summary
    )
    details_html = "".join(f"<li style='margin:0 0 6px'>{escape(item)}</li>" for item in details)
    excerpt_html = ""
    if excerpt:
        excerpt_html = (
            "<div style='margin-top:14px'>"
            "<div style='font-size:12px;font-weight:700;color:#6b7280;margin-bottom:6px'>Texto extraido</div>"
            f"<div style='background:#f8fafc;border:1px solid #e5e7eb;border-radius:8px;padding:10px;"
            f"font-family:Consolas, monospace;font-size:12px;color:#374151;white-space:pre-wrap'>"
            f"{escape(excerpt[:1200])}</div></div>"
        )

    html_body = f"""\
<html>
  <body style="margin:0;padding:24px;background:#f3f4f6;font-family:Segoe UI, Arial, sans-serif;color:#111827">
    <div style="max-width:640px;margin:0 auto;background:#ffffff;border:1px solid #e5e7eb;border-radius:14px;overflow:hidden">
      <div style="padding:18px 22px;border-bottom:1px solid #e5e7eb;background:#fafaf9">
        <div style="font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:#6b7280;margin-bottom:8px">Lector Facturas - Ejecucion nocturna</div>
        <div style="display:inline-block;padding:4px 10px;border-radius:999px;background:#111827;color:#ffffff;font-size:12px;font-weight:700">{escape(badge)}</div>
        <div style="margin-top:12px;font-size:20px;font-weight:700;line-height:1.3">{escape(intro)}</div>
      </div>
      <div style="padding:20px 22px">
        <table style="border-collapse:collapse;font-size:14px;margin-bottom:14px">{summary_html}</table>
        {"<ul style='padding-left:18px;margin:10px 0 0'>" + details_html + "</ul>" if details else ""}
        {excerpt_html}
      </div>
    </div>
  </body>
</html>
"""
    message.add_alternative(html_body, subtype="html")
    return message


def build_unmatched_supplier_email(notice: UnmatchedInvoiceNotice, config: GmailConfig) -> EmailMessage:
    details = [
        f"Adjuntos: {', '.join(notice.attachment_names) if notice.attachment_names else 'ninguno'}",
        f"Ubicacion temporal: {_display_review_path(notice.review_path)}",
    ]
    if notice.review_url:
        details.insert(0, f"Revisar: {notice.review_url}")
    if notice.suggested_provider:
        details.insert(0, f"Proveedor sugerido: {notice.suggested_provider}")
    return _build_html_message(
        subject=f"[LF] Revisar proveedor - {notice.company} - {notice.period_yyyymm}",
        config=config,
        intro="Factura sin proveedor reconocido",
        badge="Proveedor",
        summary=[
            ("Empresa", notice.company),
            ("Periodo", notice.period_yyyymm),
            ("Remitente", notice.source_sender or "-"),
            ("Asunto", notice.source_subject or "-"),
        ],
        details=details,
        excerpt=notice.extracted_text,
    )


def build_historical_invoice_email(notice: HistoricalInvoiceNotice, config: GmailConfig) -> EmailMessage:
    details = [
        f"Adjuntos: {', '.join(notice.attachment_names) if notice.attachment_names else 'ninguno'}",
        f"Ubicacion temporal: {_display_review_path(notice.review_path)}",
    ]
    if notice.review_url:
        details.insert(0, f"Revisar: {notice.review_url}")
    if notice.invoice_number:
        details.insert(0, f"Numero de factura: {notice.invoice_number}")
    if notice.invoice_date:
        details.insert(1 if notice.invoice_number else 0, f"Fecha de factura: {notice.invoice_date}")
    return _build_html_message(
        subject=f"[LF] Revisar factura historica - {notice.company} - {notice.invoice_year}",
        config=config,
        intro="Factura fuera del periodo automatico",
        badge="Historica",
        summary=[
            ("Empresa", notice.company),
            ("Anyo factura", str(notice.invoice_year)),
            ("Anyo minimo", str(notice.expected_year_from)),
            ("Remitente", notice.source_sender or "-"),
            ("Asunto", notice.source_subject or "-"),
        ],
        details=details,
        excerpt=notice.extracted_text,
    )


def build_missing_expected_invoices_email(
    notice: MissingExpectedInvoicesNotice,
    config: GmailConfig,
) -> EmailMessage:
    details = list(notice.missing_items)
    if notice.notes:
        details.append(f"Notas: {notice.notes}")
    return _build_html_message(
        subject=f"[LF] Faltantes diarios - {notice.company} - {notice.period_yyyymm}",
        config=config,
        intro="Facturas esperadas pendientes de recibir",
        badge="Faltantes",
        summary=[
            ("Empresa", notice.company),
            ("Periodo", notice.period_yyyymm),
            ("Pendientes", str(len(notice.missing_items))),
        ],
        details=details,
    )


def _format_amount(amount: float | None, currency_code: str) -> str:
    if amount is None:
        return ""
    symbol = {"EUR": "€", "USD": "$", "GBP": "£"}.get(currency_code.upper(), currency_code)
    return f"{amount:,.2f} {symbol}".replace(",", "X").replace(".", ",").replace("X", ".")


def _short_path(windows_path: str) -> str:
    if not windows_path:
        return ""
    parts = windows_path.replace("\\", "/").split("/")
    try:
        idx = next(i for i, p in enumerate(parts) if "Finances" in p or "Finance" in p)
        return "/".join(parts[idx:])
    except StopIteration:
        return "/".join(parts[-3:]) if len(parts) > 3 else windows_path


def build_nightly_review_digest_email(
    digest: NightlyReviewDigest,
    config: GmailConfig,
) -> EmailMessage:
    n_processed = len(digest.processed_items)
    n_to_check = len(digest.to_check_items)
    n_no_invoice = len(digest.no_invoice_items)
    n_rebotes = len(digest.returned_to_to_check_items)
    n_duplicates = len(digest.duplicate_items)

    # --- plain text ---
    plain_lines = [
        "Resumen diario del lector de facturas.",
        "",
        f"Empresa: {digest.company}",
        f"Periodo: {digest.period_yyyymm}",
        f"Procesadas: {n_processed} | To-check: {n_to_check} | No-facturas: {n_no_invoice} | Rebotes: {n_rebotes}",
    ]
    if digest.processed_items:
        plain_lines.extend(["", f"Facturas procesadas ({n_processed}):"])
        for item in digest.processed_items:
            amount_str = f" | {_format_amount(item.gross_amount, item.currency_code)}" if item.gross_amount else ""
            plain_lines.append(f"- [{item.supplier_code}] {item.filename}{amount_str}")
            if item.drive_url:
                plain_lines.append(f"  Drive: {item.drive_url}")
    if digest.to_check_items:
        plain_lines.extend(["", f"To-check ({n_to_check}):"])
        plain_lines.extend(f"- {item}" for item in digest.to_check_items)
    if digest.no_invoice_items:
        plain_lines.extend(["", f"No-facturas ({n_no_invoice}):"])
        plain_lines.extend(f"- {item}" for item in digest.no_invoice_items)
    if digest.returned_to_to_check_items:
        plain_lines.extend(["", f"Rebotes ({n_rebotes}):"])
        plain_lines.extend(f"- {item}" for item in digest.returned_to_to_check_items)
    if n_to_check > 0 or n_rebotes > 0:
        plain_lines.extend(["", "Acciones pendientes:", "- Revisa los items en to-check y rebotes."])

    message = _build_message(
        f"[LF] Resumen diario - {digest.company} - {digest.period_yyyymm}",
        config,
        plain_lines,
    )

    # --- summary table (only relevant rows) ---
    summary_rows = [
        ("Empresa", digest.company),
        ("Periodo", digest.period_yyyymm),
        ("Procesadas", str(n_processed)),
        ("To-check", str(n_to_check)),
        ("No-facturas", str(n_no_invoice)),
        ("Rebotes", str(n_rebotes)),
    ]
    if n_duplicates:
        summary_rows.append(("Duplicadas", str(n_duplicates)))

    def _row_color(label: str, value: str) -> str:
        if label in ("To-check", "Rebotes") and value != "0":
            return "#dc2626"
        if label == "Procesadas" and value != "0":
            return "#16a34a"
        return "#111827"

    summary_html = "".join(
        f"<tr>"
        f"<td style='padding:5px 16px 5px 0;color:#6b7280;white-space:nowrap;font-size:13px'>{escape(label)}</td>"
        f"<td style='padding:5px 0;font-size:13px'>"
        f"<strong style='color:{_row_color(label, value)}'>{escape(value)}</strong>"
        f"</td></tr>"
        for label, value in summary_rows
    )

    # --- processed invoices section ---
    processed_html = ""
    if digest.processed_items:
        cards = []
        for item in digest.processed_items:
            amount_html = ""
            if item.gross_amount is not None:
                amount_html = (
                    f"<span style='background:#f0fdf4;color:#15803d;border:1px solid #bbf7d0;"
                    f"border-radius:6px;padding:2px 8px;font-size:12px;font-weight:600'>"
                    f"{escape(_format_amount(item.gross_amount, item.currency_code))}</span>"
                )
            supplier_badge = (
                f"<span style='background:#eff6ff;color:#1d4ed8;border:1px solid #bfdbfe;"
                f"border-radius:6px;padding:2px 8px;font-size:11px;font-weight:700'>"
                f"{escape(item.supplier_code or '-')}</span>"
            )
            drive_btn = ""
            if item.drive_url:
                drive_btn = (
                    f"<a href='{escape(item.drive_url)}' target='_blank' "
                    f"style='display:inline-block;margin-top:6px;padding:5px 12px;background:#111827;"
                    f"color:#ffffff;border-radius:6px;font-size:12px;font-weight:600;"
                    f"text-decoration:none'>Abrir en Drive</a>"
                )
            path_html = ""
            short = _short_path(item.windows_path)
            if short:
                path_html = (
                    f"<div style='margin-top:4px;font-size:11px;color:#9ca3af;"
                    f"font-family:Consolas,monospace;word-break:break-all'>{escape(short)}</div>"
                )
            cards.append(
                "<div style='padding:12px 14px;border:1px solid #e5e7eb;border-radius:10px;"
                "margin-bottom:10px;background:#fafafa'>"
                f"<div style='display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:4px'>"
                f"{supplier_badge}"
                f"<span style='font-size:13px;font-weight:600;color:#111827'>{escape(item.filename)}</span>"
                f"</div>"
                f"<div style='display:flex;align-items:center;gap:8px;flex-wrap:wrap'>"
                f"{amount_html}"
                f"{drive_btn}"
                f"</div>"
                f"{path_html}"
                "</div>"
            )
        processed_html = (
            "<div style='margin-top:20px'>"
            "<div style='font-size:14px;font-weight:700;color:#111827;margin-bottom:10px'>"
            f"Facturas procesadas ({n_processed})</div>"
            + "".join(cards)
            + "</div>"
        )

    # --- to-check section ---
    to_check_html = ""
    if digest.to_check_items:
        items_li = "".join(
            f"<li style='margin:0 0 6px;font-size:13px'>{escape(item)}</li>"
            for item in digest.to_check_items
        )
        to_check_html = (
            "<div style='margin-top:20px'>"
            "<div style='font-size:14px;font-weight:700;color:#dc2626;margin-bottom:10px'>"
            f"To-check — requieren revision ({n_to_check})</div>"
            f"<ul style='padding-left:18px;margin:0'>{items_li}</ul>"
            "</div>"
        )

    # --- no-invoice section (informativa, compacta) ---
    no_invoice_html = ""
    if digest.no_invoice_items:
        items_li = "".join(
            f"<li style='margin:0 0 4px;font-size:12px;color:#6b7280'>{escape(item)}</li>"
            for item in digest.no_invoice_items
        )
        no_invoice_html = (
            "<div style='margin-top:20px'>"
            "<div style='font-size:13px;font-weight:700;color:#6b7280;margin-bottom:8px'>"
            f"No-facturas ({n_no_invoice})</div>"
            f"<ul style='padding-left:18px;margin:0'>{items_li}</ul>"
            "</div>"
        )

    # --- rebotes section ---
    rebotes_html = ""
    if digest.returned_to_to_check_items:
        items_li = "".join(
            f"<li style='margin:0 0 6px;font-size:13px'>{escape(item)}</li>"
            for item in digest.returned_to_to_check_items
        )
        rebotes_html = (
            "<div style='margin-top:20px'>"
            "<div style='font-size:14px;font-weight:700;color:#dc2626;margin-bottom:10px'>"
            f"Rebotes — error al procesar ({n_rebotes})</div>"
            f"<ul style='padding-left:18px;margin:0'>{items_li}</ul>"
            "</div>"
        )

    # --- acciones pendientes ---
    acciones_html = ""
    if n_to_check > 0 or n_rebotes > 0:
        acciones_html = (
            "<div style='margin-top:20px;padding:12px 14px;background:#fff7ed;"
            "border:1px solid #fdba74;border-radius:10px;color:#7c2d12'>"
            "<strong>Acciones pendientes</strong>"
            "<ul style='padding-left:18px;margin:8px 0 0;font-size:13px'>"
        )
        if n_to_check > 0:
            acciones_html += "<li>Hay facturas en to-check que requieren revision manual.</li>"
        if n_rebotes > 0:
            acciones_html += "<li>Hay rebotes con errores — comprueba permisos de Drive.</li>"
        acciones_html += "</ul></div>"

    html_body = f"""\
<html>
  <body style="margin:0;padding:24px;background:#f3f4f6;font-family:Segoe UI,Arial,sans-serif;color:#111827">
    <div style="max-width:720px;margin:0 auto;background:#ffffff;border:1px solid #e5e7eb;border-radius:14px;overflow:hidden">
      <div style="padding:18px 22px;border-bottom:1px solid #e5e7eb;background:#fafaf9">
        <div style="font-size:11px;letter-spacing:.08em;text-transform:uppercase;color:#6b7280;margin-bottom:8px">Lector Facturas</div>
        <div style="display:inline-block;padding:4px 10px;border-radius:999px;background:#111827;color:#fff;font-size:12px;font-weight:700">Resumen diario</div>
      </div>
      <div style="padding:20px 22px">
        <table style="border-collapse:collapse;margin-bottom:4px">{summary_html}</table>
        {processed_html}
        {to_check_html}
        {rebotes_html}
        {no_invoice_html}
        {acciones_html}
      </div>
    </div>
  </body>
</html>
"""
    message.add_alternative(html_body, subtype="html")
    return message


def _section_html(title: str, items_html: list[str]) -> str:
    return (
        "<div style='margin-top:16px'>"
        f"<div style='font-size:13px;font-weight:700;color:#111827;margin-bottom:8px'>{escape(title)}</div>"
        "<ul style='padding-left:18px;margin:0'>"
        f"{''.join(items_html)}"
        "</ul>"
        "</div>"
    )


def send_message_via_gmail(message: EmailMessage, config: GmailConfig) -> dict[str, object]:
    access_token = refresh_access_token(config)
    raw_message = urlsafe_b64encode(message.as_bytes()).decode("ascii")
    payload = json.dumps({"raw": raw_message}).encode("utf-8")
    endpoint = f"https://gmail.googleapis.com/gmail/v1/users/{config.user_id}/messages/send"
    request = Request(
        endpoint,
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Gmail API notification failed: {exc.code} {detail}") from exc


def send_unmatched_supplier_email(notice: UnmatchedInvoiceNotice, config: GmailConfig) -> dict[str, object]:
    message = build_unmatched_supplier_email(notice, config)
    return send_message_via_gmail(message, config)


def build_worker_failure_email(
    *,
    worker_name: str,
    consecutive_failures: int,
    last_error: str = "",
    config: GmailConfig,
) -> EmailMessage:
    from datetime import datetime, timezone as tz
    now = datetime.now(tz=tz.utc).strftime("%Y-%m-%d %H:%M UTC")
    summary = [
        ("Worker", worker_name),
        ("Fallos consecutivos", str(consecutive_failures)),
        ("Detectado", now),
    ]
    details = ["Revisa los logs en Railway para ver el error concreto."]
    if last_error:
        details.insert(0, f"Ultimo error: {last_error}")
    return _build_html_message(
        subject=f"[LF] ALERTA: {worker_name} — {consecutive_failures} fallos consecutivos",
        config=config,
        intro=f"El worker '{worker_name}' lleva {consecutive_failures} ejecuciones fallidas seguidas.",
        badge="Worker caido",
        summary=summary,
        details=details,
    )


def send_worker_failure_alert(*, worker_name: str, consecutive_failures: int, last_error: str = "") -> None:
    """Envia alerta por email cuando un worker acumula fallos consecutivos. No lanza excepciones."""
    client_id = os.environ.get("GOOGLE_CLIENT_ID", "").strip()
    client_secret = os.environ.get("GOOGLE_CLIENT_SECRET", "").strip()
    refresh_token = os.environ.get("GOOGLE_REFRESH_TOKEN", "").strip()
    sender = os.environ.get("GMAIL_SENDER", "").strip()
    recipients_raw = os.environ.get("GMAIL_RECIPIENTS", "").strip()
    if not all([client_id, client_secret, refresh_token, sender, recipients_raw]):
        return
    recipients = tuple(r.strip() for r in recipients_raw.split(",") if r.strip())
    config = GmailConfig(
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
        sender=sender,
        recipients=recipients,
    )
    try:
        message = build_worker_failure_email(
            worker_name=worker_name,
            consecutive_failures=consecutive_failures,
            last_error=last_error,
            config=config,
        )
        send_message_via_gmail(message, config)
    except Exception:  # noqa: BLE001
        pass  # nunca crashear el worker por un fallo de alerta


def refresh_access_token(config: GmailConfig) -> str:
    payload = urlencode(
        {
            "client_id": config.client_id,
            "client_secret": config.client_secret,
            "refresh_token": config.refresh_token,
            "grant_type": "refresh_token",
        }
    ).encode("utf-8")
    request = Request(
        "https://oauth2.googleapis.com/token",
        data=payload,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urlopen(request, timeout=30) as response:
            data = json.loads(response.read().decode("utf-8"))
            return data["access_token"]
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Token refresh failed: {exc.code} {detail}") from exc
