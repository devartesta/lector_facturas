from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any
import ntpath
import uuid

import psycopg

from lector_facturas.api.store import COMPANY_NAMES, SCHEMA_NAME, ReviewStore
from lector_facturas.google_drive import GoogleDriveClient
from lector_facturas.invoice_ingestion import build_windows_path, ensure_drive_path
from lector_facturas.review_workflow import get_provider
from lector_facturas.settings import AppSettings


def _normalize_decimal(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return Decimal(str(value)).quantize(Decimal("0.01"))


def _safe_invoice_number(invoice_number: str) -> str:
    return invoice_number.replace("/", "-").replace("\\", "-").replace(" ", "")


def _filename_for_document(*, supplier_code: str, invoice_date: date, invoice_number: str, original_filename: str) -> str:
    extension = Path(original_filename or "document.pdf").suffix.lower() or ".pdf"
    return f"{supplier_code}_{invoice_date.strftime('%Y%m%d')}_{_safe_invoice_number(invoice_number)}{extension}"


def _fetch_supplier(conn: psycopg.Connection[Any], *, company_code: str, supplier_code: str) -> dict[str, str]:
    row = conn.execute(
        f"""
        SELECT supplier_code, supplier_name, destination_path
        FROM {SCHEMA_NAME}.suppliers
        WHERE company_code = %s AND supplier_code = %s
        """,
        (company_code, supplier_code),
    ).fetchone()
    if row is None:
        provider = get_provider(COMPANY_NAMES[company_code], supplier_code)
        return {
            "supplier_code": provider.supplier_code,
            "supplier_name": provider.provider_name,
            "destination_path": provider.destination_path,
        }
    return {
        "supplier_code": str(row[0] or ""),
        "supplier_name": str(row[1] or ""),
        "destination_path": str(row[2] or ""),
    }


def _resolve_accounting_fields(
    *,
    accounting_category: str,
    accounting_subcategory: str,
    accounting_detail: str,
    destination_path: str,
    fallback_detail: str = "",
) -> tuple[str, str, str]:
    if accounting_category and accounting_subcategory:
        return accounting_category, accounting_subcategory, accounting_detail or fallback_detail
    path = (destination_path or "").strip().strip("/")
    if "/" in path:
        parts = path.split("/")
        if len(parts) >= 3:
            return parts[1], parts[2], accounting_detail or fallback_detail
    return accounting_category or "", accounting_subcategory or "", accounting_detail or fallback_detail


def _document_detail_query() -> str:
    return f"""
        SELECT
            d.id, d.company_code, d.supplier_code, COALESCE(s.supplier_name, d.supplier_name),
            d.invoice_number, d.invoice_date, d.period_yyyymm, d.billing_period_start, d.billing_period_end,
            d.currency_code, d.gross_amount, d.net_amount, d.vat_amount, d.vat_percent,
            d.accounting_category, d.accounting_subcategory, d.accounting_detail,
            d.document_type, d.status, d.payment_status, d.payment_date, d.payment_method,
            d.payment_amount, d.payment_due_date, d.windows_path, d.drive_file_id, d.drive_url,
            d.original_filename, d.source_channel, d.parser_name, d.parser_confidence, d.review_notes,
            d.division_invoice, d.is_periodified_root, d.periodified_parent_id,
            (SELECT COUNT(*) FROM {SCHEMA_NAME}.documents child WHERE child.periodified_parent_id = d.id),
            COALESCE(pm.provision_id::text, ''),
            COALESCE(p.status, '')
        FROM {SCHEMA_NAME}.documents d
        LEFT JOIN {SCHEMA_NAME}.suppliers s ON s.id = d.supplier_id
        LEFT JOIN {SCHEMA_NAME}.provision_matches pm ON pm.document_id = d.id
        LEFT JOIN {SCHEMA_NAME}.provisions p ON p.id = pm.provision_id
    """


def _document_detail_from_row(row: Any) -> dict[str, Any]:
    return {
        "id": str(row[0]),
        "company_code": str(row[1] or ""),
        "supplier_code": str(row[2] or ""),
        "supplier_name": str(row[3] or ""),
        "invoice_number": str(row[4] or ""),
        "invoice_date": row[5],
        "period_yyyymm": str(row[6] or ""),
        "billing_period_start": row[7],
        "billing_period_end": row[8],
        "currency_code": str(row[9] or ""),
        "gross_amount": row[10],
        "net_amount": row[11],
        "vat_amount": row[12],
        "vat_percent": row[13],
        "accounting_category": str(row[14] or ""),
        "accounting_subcategory": str(row[15] or ""),
        "accounting_detail": str(row[16] or ""),
        "document_type": str(row[17] or "invoice"),
        "status": str(row[18] or ""),
        "payment_status": str(row[19] or ""),
        "payment_date": row[20],
        "payment_method": str(row[21] or ""),
        "payment_amount": row[22],
        "payment_due_date": row[23],
        "windows_path": str(row[24] or ""),
        "drive_file_id": str(row[25] or ""),
        "drive_url": str(row[26] or ""),
        "original_filename": str(row[27] or ""),
        "source_channel": str(row[28] or ""),
        "parser_name": str(row[29] or ""),
        "parser_confidence": row[30],
        "review_notes": str(row[31] or ""),
        "division_invoice": str(row[32] or ""),
        "is_periodified_root": bool(row[33]) if row[33] is not None else False,
        "periodified_parent_id": str(row[34] or ""),
        "periodified_children_count": int(row[35] or 0),
        "linked_provision_id": str(row[36] or ""),
        "linked_provision_status": str(row[37] or ""),
    }


def get_document_detail(*, database_url: str, document_id: str) -> dict[str, Any] | None:
    with psycopg.connect(database_url) as conn:
        row = conn.execute(_document_detail_query() + " WHERE d.id = %s", (document_id,)).fetchone()
    return None if row is None else _document_detail_from_row(row)


def list_documents(
    *,
    database_url: str,
    company_code: str | None = None,
    period_yyyymm: str | None = None,
    supplier_code: str | None = None,
    query: str | None = None,
    document_type: str | None = None,
    payment_status: str | None = None,
    accounting_category: str | None = None,
    accounting_subcategory: str | None = None,
    periodified: str | None = None,
) -> list[dict[str, Any]]:
    sql = f"""
        SELECT
            d.id, d.document_type, d.company_code, d.supplier_code, COALESCE(s.supplier_name, d.supplier_name),
            d.invoice_number, d.invoice_date, d.period_yyyymm, d.currency_code,
            d.gross_amount, d.net_amount, d.vat_amount,
            d.accounting_category, d.accounting_subcategory, d.accounting_detail,
            d.payment_status, d.payment_due_date, d.drive_url,
            (d.drive_file_id <> '' OR d.drive_url <> '') AS has_pdf,
            d.is_periodified_root,
            (SELECT COUNT(*) FROM {SCHEMA_NAME}.documents child WHERE child.periodified_parent_id = d.id),
            d.review_notes
        FROM {SCHEMA_NAME}.documents d
        LEFT JOIN {SCHEMA_NAME}.suppliers s ON s.id = d.supplier_id
        WHERE 1=1
    """
    params: list[Any] = []
    if company_code:
        sql += " AND d.company_code = %s"
        params.append(company_code)
    if period_yyyymm:
        sql += " AND d.period_yyyymm = %s"
        params.append(period_yyyymm)
    if supplier_code:
        sql += " AND d.supplier_code = %s"
        params.append(supplier_code)
    if document_type:
        sql += " AND d.document_type = %s"
        params.append(document_type)
    if payment_status:
        sql += " AND d.payment_status = %s"
        params.append(payment_status)
    if accounting_category:
        sql += " AND d.accounting_category = %s"
        params.append(accounting_category)
    if accounting_subcategory:
        sql += " AND d.accounting_subcategory = %s"
        params.append(accounting_subcategory)
    if periodified == "yes":
        sql += " AND (d.is_periodified_root = TRUE OR d.periodified_parent_id IS NOT NULL)"
    elif periodified == "no":
        sql += " AND d.is_periodified_root = FALSE AND d.periodified_parent_id IS NULL"
    if query:
        pattern = f"%{query}%"
        sql += " AND (d.invoice_number ILIKE %s OR d.supplier_code ILIKE %s OR COALESCE(s.supplier_name, d.supplier_name) ILIKE %s OR d.review_notes ILIKE %s)"
        params.extend([pattern, pattern, pattern, pattern])
    sql += " ORDER BY d.period_yyyymm DESC, d.company_code, d.supplier_code, d.invoice_number"
    with psycopg.connect(database_url) as conn:
        rows = conn.execute(sql, params).fetchall()
    return [
        {
            "id": str(row[0]),
            "kind": "document",
            "document_type": str(row[1] or "invoice"),
            "company_code": str(row[2] or ""),
            "supplier_code": str(row[3] or ""),
            "supplier_name": str(row[4] or ""),
            "invoice_number": str(row[5] or ""),
            "invoice_date": row[6],
            "period_yyyymm": str(row[7] or ""),
            "currency_code": str(row[8] or ""),
            "gross_amount": row[9],
            "net_amount": row[10],
            "vat_amount": row[11],
            "accounting_category": str(row[12] or ""),
            "accounting_subcategory": str(row[13] or ""),
            "accounting_detail": str(row[14] or ""),
            "payment_status": str(row[15] or ""),
            "payment_due_date": row[16],
            "drive_url": str(row[17] or ""),
            "has_pdf": bool(row[18]),
            "is_periodified_root": bool(row[19]),
            "periodified_children_count": int(row[20] or 0),
            "notes": str(row[21] or ""),
        }
        for row in rows
    ]


def _document_windows_path(
    *,
    company_code: str,
    supplier_code: str,
    invoice_date: date,
    period_yyyymm: str,
    invoice_number: str,
    destination_path: str,
    original_filename: str,
) -> str:
    return build_windows_path(
        company_name=COMPANY_NAMES[company_code],
        period_yyyymm=period_yyyymm,
        destination_path=destination_path,
        filename=_filename_for_document(
            supplier_code=supplier_code,
            invoice_date=invoice_date,
            invoice_number=invoice_number,
            original_filename=original_filename,
        ),
    )


def _sync_drive_location(
    *,
    settings: AppSettings,
    drive_file_id: str,
    windows_path: str,
) -> tuple[str, str]:
    if not drive_file_id or not settings.google_oauth_ready or not settings.drive_root_folder_id:
        return "", drive_file_id
    client = GoogleDriveClient(settings.to_drive_config())
    parent_id = ensure_drive_path(client, root_folder_id=settings.drive_root_folder_id, windows_path=windows_path)
    moved = client.move_file(file_id=drive_file_id, new_parent_id=parent_id)
    renamed = client.update_file_name(file_id=drive_file_id, name=ntpath.basename(windows_path))
    return str(renamed.get("webViewLink") or moved.get("webViewLink") or ""), str(renamed.get("id") or moved.get("id") or drive_file_id)


def update_document(
    *,
    database_url: str,
    settings: AppSettings | None,
    document_id: str,
    payload: dict[str, Any],
) -> dict[str, Any] | None:
    current = get_document_detail(database_url=database_url, document_id=document_id)
    if current is None:
        return None
    with psycopg.connect(database_url) as conn:
        supplier = _fetch_supplier(conn, company_code=payload["company_code"], supplier_code=payload["supplier_code"])
        supplier_id = conn.execute(
            f"SELECT id FROM {SCHEMA_NAME}.suppliers WHERE company_code = %s AND supplier_code = %s",
            (payload["company_code"], payload["supplier_code"]),
        ).fetchone()
        category, subcategory, detail = _resolve_accounting_fields(
            accounting_category=str(payload.get("accounting_category", "")),
            accounting_subcategory=str(payload.get("accounting_subcategory", "")),
            accounting_detail=str(payload.get("accounting_detail", "")),
            destination_path=supplier["destination_path"],
            fallback_detail=str(payload.get("division_invoice", "")),
        )
        windows_path = current["windows_path"]
        drive_url = current["drive_url"]
        drive_file_id = current["drive_file_id"]
        invoice_date = payload.get("invoice_date") or current["invoice_date"]
        if invoice_date:
            windows_path = _document_windows_path(
                company_code=payload["company_code"],
                supplier_code=payload["supplier_code"],
                invoice_date=invoice_date,
                period_yyyymm=payload["period_yyyymm"],
                invoice_number=payload["invoice_number"],
                destination_path=supplier["destination_path"],
                original_filename=current["original_filename"] or "document.pdf",
            )
            if settings and drive_file_id:
                maybe_drive_url, maybe_drive_file_id = _sync_drive_location(settings=settings, drive_file_id=drive_file_id, windows_path=windows_path)
                drive_url = maybe_drive_url or drive_url
                drive_file_id = maybe_drive_file_id or drive_file_id
        result = conn.execute(
            f"""
            UPDATE {SCHEMA_NAME}.documents
            SET company_code = %s,
                supplier_id = %s,
                supplier_code = %s,
                supplier_name = %s,
                invoice_number = %s,
                invoice_date = %s,
                period_yyyymm = %s,
                billing_period_start = %s,
                billing_period_end = %s,
                currency_code = %s,
                gross_amount = %s,
                net_amount = %s,
                vat_amount = %s,
                vat_percent = %s,
                accounting_category = %s,
                accounting_subcategory = %s,
                accounting_detail = %s,
                document_type = %s,
                status = %s,
                payment_status = %s,
                payment_date = %s,
                payment_method = %s,
                payment_amount = %s,
                payment_due_date = %s,
                review_notes = %s,
                division_invoice = %s,
                windows_path = %s,
                drive_url = %s,
                drive_file_id = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            (
                payload["company_code"],
                supplier_id[0] if supplier_id else None,
                payload["supplier_code"],
                supplier["supplier_name"],
                payload["invoice_number"],
                invoice_date,
                payload["period_yyyymm"],
                payload.get("billing_period_start"),
                payload.get("billing_period_end"),
                payload["currency_code"],
                payload.get("gross_amount"),
                payload.get("net_amount"),
                payload.get("vat_amount"),
                payload.get("vat_percent"),
                category,
                subcategory,
                detail,
                payload.get("document_type", "invoice"),
                payload.get("status", "classified"),
                payload.get("payment_status", "pending"),
                payload.get("payment_date"),
                payload.get("payment_method", ""),
                payload.get("payment_amount"),
                payload.get("payment_due_date"),
                payload.get("review_notes", ""),
                payload.get("division_invoice", ""),
                windows_path,
                drive_url,
                drive_file_id,
                document_id,
            ),
        )
        conn.commit()
        if (result.rowcount or 0) <= 0:
            return None
    return get_document_detail(database_url=database_url, document_id=document_id)


def create_manual_document(
    *,
    database_url: str,
    settings: AppSettings | None,
    payload: dict[str, Any],
    pdf_bytes: bytes,
    original_filename: str,
) -> dict[str, Any]:
    store = ReviewStore(database_url=database_url)
    with psycopg.connect(database_url) as conn:
        supplier = _fetch_supplier(conn, company_code=payload["company_code"], supplier_code=payload["supplier_code"])
    windows_path = _document_windows_path(
        company_code=payload["company_code"],
        supplier_code=payload["supplier_code"],
        invoice_date=payload["invoice_date"],
        period_yyyymm=payload["period_yyyymm"],
        invoice_number=payload["invoice_number"],
        destination_path=supplier["destination_path"],
        original_filename=original_filename,
    )
    drive_url = ""
    drive_file_id = ""
    if settings and settings.google_oauth_ready and settings.drive_root_folder_id:
        client = GoogleDriveClient(settings.to_drive_config())
        parent_id = ensure_drive_path(client, root_folder_id=settings.drive_root_folder_id, windows_path=windows_path)
        uploaded = client.upload_file(name=ntpath.basename(windows_path), parent_id=parent_id, content=pdf_bytes, mime_type="application/pdf")
        drive_url = str(uploaded.get("webViewLink", ""))
        drive_file_id = str(uploaded.get("id", ""))
    parsed = store.build_manual_parsed_document(
        company_code=payload["company_code"],
        supplier_code=payload["supplier_code"],
        invoice_number=payload["invoice_number"],
        invoice_date=payload["invoice_date"],
        period_yyyymm=payload["period_yyyymm"],
        billing_period_start=payload.get("billing_period_start"),
        billing_period_end=payload.get("billing_period_end"),
        currency_code=payload["currency_code"],
        gross_amount=payload.get("gross_amount"),
        net_amount=payload.get("net_amount"),
        vat_amount=payload.get("vat_amount"),
        vat_percent=payload.get("vat_percent"),
        document_type=payload.get("document_type", "invoice"),
        division_invoice=payload.get("division_invoice", ""),
        source_subject=payload.get("source_subject", ""),
    )
    document_id = store.insert_document_from_parsed(
        parsed=parsed,
        company_code=payload["company_code"],
        supplier_code=payload["supplier_code"],
        windows_path=windows_path,
        drive_url=drive_url,
        drive_file_id=drive_file_id,
        original_filename=original_filename,
        source_channel=payload.get("source_channel", "manual_upload"),
        sender_email=payload.get("sender_email", ""),
        source_subject=payload.get("source_subject", ""),
        review_notes=payload.get("review_notes", ""),
        gross_amount_override=payload.get("gross_amount"),
        net_amount_override=payload.get("net_amount"),
        vat_amount_override=payload.get("vat_amount"),
        payment_due_date_override=payload.get("payment_due_date"),
        division_invoice_override=payload.get("division_invoice", ""),
    )
    updated = update_document(database_url=database_url, settings=None, document_id=document_id, payload=payload)
    if updated is None:
        raise KeyError(document_id)
    return updated


def delete_document(*, database_url: str, document_id: str) -> dict[str, Any] | None:
    store = ReviewStore(database_url=database_url)
    return store.delete_document(document_id=document_id)


def periodify_document(
    *,
    database_url: str,
    settings: AppSettings | None,
    document_id: str,
    months: list[dict[str, Any]],
) -> dict[str, Any] | None:
    root = get_document_detail(database_url=database_url, document_id=document_id)
    if root is None:
        return None
    pdf_bytes = b""
    if settings and root["drive_file_id"]:
        client = GoogleDriveClient(settings.to_drive_config())
        pdf_bytes = client.download_file_bytes(file_id=root["drive_file_id"])
    child_document_ids: list[str] = []
    total_parts = len(months)
    for index, month in enumerate(months, start=1):
        child = create_manual_document(
            database_url=database_url,
            settings=settings if pdf_bytes else None,
            payload={
                "company_code": root["company_code"],
                "supplier_code": root["supplier_code"],
                "invoice_number": f'{root["invoice_number"]}_PERIODIFICADA_{index}_{total_parts}',
                "invoice_date": root["invoice_date"],
                "period_yyyymm": month["period_yyyymm"],
                "billing_period_start": month.get("billing_period_start"),
                "billing_period_end": month.get("billing_period_end"),
                "currency_code": root["currency_code"],
                "gross_amount": _normalize_decimal(month.get("gross_amount")),
                "net_amount": _normalize_decimal(month.get("net_amount")),
                "vat_amount": _normalize_decimal(month.get("vat_amount")),
                "vat_percent": root["vat_percent"],
                "accounting_category": root["accounting_category"],
                "accounting_subcategory": root["accounting_subcategory"],
                "accounting_detail": root["accounting_detail"],
                "document_type": root["document_type"],
                "payment_status": root["payment_status"],
                "payment_date": root["payment_date"],
                "payment_method": root["payment_method"],
                "payment_amount": root["payment_amount"],
                "payment_due_date": root["payment_due_date"],
                "review_notes": root["review_notes"],
                "division_invoice": root["division_invoice"],
                "source_channel": "manual_periodificada",
                "source_subject": f'Periodified from {root["invoice_number"]}',
            },
            pdf_bytes=pdf_bytes or b"",
            original_filename=root["original_filename"] or "document.pdf",
        )
        child_document_ids.append(child["id"])
        with psycopg.connect(database_url) as conn:
            conn.execute(
                f"UPDATE {SCHEMA_NAME}.documents SET periodified_parent_id = %s, parser_name = 'manual_periodificada', updated_at = NOW() WHERE id = %s",
                (document_id, child["id"]),
            )
            conn.commit()
    with psycopg.connect(database_url) as conn:
        conn.execute(
            f"UPDATE {SCHEMA_NAME}.documents SET is_periodified_root = TRUE, updated_at = NOW() WHERE id = %s",
            (document_id,),
        )
        conn.commit()
    return {"root_document_id": document_id, "child_document_ids": child_document_ids}


def list_provisions(
    *,
    database_url: str,
    company_code: str | None = None,
    period_yyyymm: str | None = None,
    supplier_code: str | None = None,
    status: str | None = None,
    query: str | None = None,
) -> list[dict[str, Any]]:
    sql = f"""
        SELECT
            p.id, p.company_code, p.supplier_code, COALESCE(s.supplier_name, p.supplier_code),
            p.invoice_date_expected, p.period_yyyymm, p.currency_code,
            p.gross_amount, p.net_amount, p.vat_amount,
            p.accounting_category, p.accounting_subcategory, p.accounting_detail,
            p.notes, p.status, COALESCE(p.matched_document_id::text, ''),
            pa.amount, pa.currency_code
        FROM {SCHEMA_NAME}.provisions p
        LEFT JOIN {SCHEMA_NAME}.suppliers s ON s.company_code = p.company_code AND s.supplier_code = p.supplier_code
        LEFT JOIN LATERAL (
            SELECT amount, currency_code
            FROM {SCHEMA_NAME}.provision_adjustments
            WHERE provision_id = p.id
            ORDER BY created_at DESC
            LIMIT 1
        ) pa ON TRUE
        WHERE 1=1
    """
    params: list[Any] = []
    if company_code:
        sql += " AND p.company_code = %s"
        params.append(company_code)
    if period_yyyymm:
        sql += " AND p.period_yyyymm = %s"
        params.append(period_yyyymm)
    if supplier_code:
        sql += " AND p.supplier_code = %s"
        params.append(supplier_code)
    if status:
        sql += " AND p.status = %s"
        params.append(status)
    if query:
        pattern = f"%{query}%"
        sql += " AND (p.supplier_code ILIKE %s OR COALESCE(s.supplier_name, p.supplier_code) ILIKE %s OR p.notes ILIKE %s)"
        params.extend([pattern, pattern, pattern])
    sql += " ORDER BY p.period_yyyymm DESC, p.company_code, p.supplier_code"
    with psycopg.connect(database_url) as conn:
        rows = conn.execute(sql, params).fetchall()
    return [
        {
            "id": str(row[0]),
            "kind": "provision",
            "document_type": "provision",
            "company_code": str(row[1] or ""),
            "supplier_code": str(row[2] or ""),
            "supplier_name": str(row[3] or ""),
            "invoice_date_expected": row[4],
            "period_yyyymm": str(row[5] or ""),
            "currency_code": str(row[6] or ""),
            "gross_amount": row[7],
            "net_amount": row[8],
            "vat_amount": row[9],
            "accounting_category": str(row[10] or ""),
            "accounting_subcategory": str(row[11] or ""),
            "accounting_detail": str(row[12] or ""),
            "notes": str(row[13] or ""),
            "provision_status": str(row[14] or "open"),
            "matched_document_id": str(row[15] or ""),
            "adjustment_amount": row[16],
            "adjustment_currency": str(row[17] or ""),
            "has_pdf": False,
            "drive_url": "",
            "payment_status": "",
            "payment_due_date": None,
            "is_periodified_root": False,
            "periodified_children_count": 0,
        }
        for row in rows
    ]


def get_provision(*, database_url: str, provision_id: str) -> dict[str, Any] | None:
    for item in list_provisions(database_url=database_url):
        if item["id"] == provision_id:
            return item
    return None


def create_provision(*, database_url: str, payload: dict[str, Any]) -> dict[str, Any]:
    provision_id = str(uuid.uuid4())
    with psycopg.connect(database_url) as conn:
        conn.execute(
            f"""
            INSERT INTO {SCHEMA_NAME}.provisions (
                id, company_code, supplier_code, invoice_date_expected, period_yyyymm, currency_code,
                gross_amount, net_amount, vat_amount, accounting_category, accounting_subcategory,
                accounting_detail, notes, status, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, NOW(), NOW()
            )
            """,
            (
                provision_id,
                payload["company_code"],
                payload["supplier_code"],
                payload.get("invoice_date_expected"),
                payload["period_yyyymm"],
                payload["currency_code"],
                payload.get("gross_amount"),
                payload.get("net_amount"),
                payload.get("vat_amount"),
                payload.get("accounting_category", ""),
                payload.get("accounting_subcategory", ""),
                payload.get("accounting_detail", ""),
                payload.get("notes", ""),
                payload.get("status", "open"),
            ),
        )
        conn.commit()
    return get_provision(database_url=database_url, provision_id=provision_id) or {}


def update_provision(*, database_url: str, provision_id: str, payload: dict[str, Any]) -> dict[str, Any] | None:
    with psycopg.connect(database_url) as conn:
        result = conn.execute(
            f"""
            UPDATE {SCHEMA_NAME}.provisions
            SET company_code = %s,
                supplier_code = %s,
                invoice_date_expected = %s,
                period_yyyymm = %s,
                currency_code = %s,
                gross_amount = %s,
                net_amount = %s,
                vat_amount = %s,
                accounting_category = %s,
                accounting_subcategory = %s,
                accounting_detail = %s,
                notes = %s,
                status = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            (
                payload["company_code"],
                payload["supplier_code"],
                payload.get("invoice_date_expected"),
                payload["period_yyyymm"],
                payload["currency_code"],
                payload.get("gross_amount"),
                payload.get("net_amount"),
                payload.get("vat_amount"),
                payload.get("accounting_category", ""),
                payload.get("accounting_subcategory", ""),
                payload.get("accounting_detail", ""),
                payload.get("notes", ""),
                payload.get("status", "open"),
                provision_id,
            ),
        )
        conn.commit()
        if (result.rowcount or 0) <= 0:
            return None
    return get_provision(database_url=database_url, provision_id=provision_id)


def match_provision(*, database_url: str, provision_id: str, document_id: str) -> dict[str, Any] | None:
    provision = get_provision(database_url=database_url, provision_id=provision_id)
    document = get_document_detail(database_url=database_url, document_id=document_id)
    if provision is None or document is None:
        return None
    adjustment_amount = (Decimal(str(document["net_amount"] or 0)) - Decimal(str(provision["net_amount"] or 0))).quantize(Decimal("0.01"))
    with psycopg.connect(database_url) as conn:
        conn.execute(
            f"""
            INSERT INTO {SCHEMA_NAME}.provision_matches (id, provision_id, document_id, matched_at, created_at, updated_at)
            VALUES (%s, %s, %s, NOW(), NOW(), NOW())
            ON CONFLICT (provision_id, document_id) DO NOTHING
            """,
            (str(uuid.uuid4()), provision_id, document_id),
        )
        conn.execute(
            f"""
            UPDATE {SCHEMA_NAME}.provisions
            SET matched_document_id = %s,
                status = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            (document_id, "adjusted" if adjustment_amount != Decimal("0.00") else "matched", provision_id),
        )
        if adjustment_amount != Decimal("0.00"):
            conn.execute(
                f"""
                INSERT INTO {SCHEMA_NAME}.provision_adjustments (
                    id, provision_id, document_id, company_code, period_yyyymm, amount, currency_code, notes, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()
                )
                """,
                (
                    str(uuid.uuid4()),
                    provision_id,
                    document_id,
                    document["company_code"],
                    document["period_yyyymm"],
                    adjustment_amount,
                    document["currency_code"],
                    "Automatic adjustment generated from provision match.",
                ),
            )
        conn.commit()
    return get_provision(database_url=database_url, provision_id=provision_id)
