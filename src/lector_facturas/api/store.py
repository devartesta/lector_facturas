from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any
import json
import ntpath
from pathlib import Path
import uuid

from lector_facturas.payment_fees import (
    PaymentFeeSummaryRow,
    PaymentOrderTransaction,
    parse_decimal,
    summarize_payment_transactions,
)
from lector_facturas.provider_catalog import ProviderRecord, load_provider_catalog
from lector_facturas.review_notifications import NightlyReviewDigest, UnmatchedInvoiceNotice
from lector_facturas.review_workflow import ReviewDecision, apply_review_decision, company_folder_name, get_provider

try:
    import psycopg
except ImportError:  # pragma: no cover
    psycopg = None


SCHEMA_NAME = "invoices"
COMPANY_CODES = {
    "ARTESTA STORE, S.L.": "SL",
    "ARTESTA STORES (UK) LTD": "LTD",
    "ARTESTA INC": "INC",
}
COMPANY_NAMES = {value: key for key, value in COMPANY_CODES.items()}

DOCUMENTS_COLUMN_DEFINITIONS = (
    ("received_at", "TIMESTAMPTZ NULL"),
    ("sender_email", "TEXT NOT NULL DEFAULT ''"),
    ("original_filename", "TEXT NOT NULL DEFAULT ''"),
    ("division_invoice", "TEXT NOT NULL DEFAULT ''"),
    ("billing_period_start", "DATE NULL"),
    ("billing_period_end", "DATE NULL"),
)

SUPPLIERS_COLUMN_DEFINITIONS = (
    ("sender_emails", "TEXT NOT NULL DEFAULT '[]'"),
)


@dataclass
class StoredReviewItem:
    id: str
    kind: str
    status: str
    company: str
    period_yyyymm: str
    source_sender: str = ""
    source_subject: str = ""
    attachment_names: list[str] | None = None
    review_path: str = ""
    source_file: str = ""
    suggested_provider: str = ""
    suggested_supplier_code: str = ""
    notes: str = ""
    destination_file: str = ""
    drive_file_id: str = ""
    drive_view_url: str = ""


@dataclass
class IngestionQueueItem:
    id: str
    source: str
    gmail_message_id: str = ""
    gmail_attachment_id: str = ""
    original_filename: str = ""
    stored_filename: str = ""
    sender_email: str = ""
    subject: str = ""
    received_at: datetime | None = None
    drive_file_id: str = ""
    drive_url: str = ""
    validation_bucket: str = ""
    detected_supplier_code: str = ""
    detected_company_code: str = ""
    parser_name: str = ""
    parse_status: str = ""
    parse_error: str = ""
    document_id: str = ""
    mime_type: str = ""
    heuristic_reason: str = ""
    created_at: datetime | None = None
    updated_at: datetime | None = None


class ReviewStore:
    def __init__(
        self,
        storage_path: Path | None = None,
        finance_root: Path | None = None,
        database_url: str | None = None,
    ) -> None:
        self.storage_path = storage_path or Path(__file__).resolve().parents[3] / "data" / "review_items.json"
        self.payment_transactions_path = self.storage_path.with_name("payment_order_transactions.json")
        self.payment_summary_path = self.storage_path.with_name("payment_fee_monthly_summary.json")
        self.shopify_payout_transactions_path = self.storage_path.with_name("shopify_payout_transactions.json")
        self.paypal_transactions_raw_path = self.storage_path.with_name("paypal_transactions_raw.json")
        self.finance_root = finance_root
        self.database_url = database_url
        if self.database_url:
            if psycopg is None:
                raise RuntimeError("psycopg is required when DATABASE_URL is configured.")
            self._init_db()
            return
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.storage_path.exists():
            self._write_items(self._seed_items())
        if not self.payment_transactions_path.exists():
            self._write_payment_transactions([])
        if not self.payment_summary_path.exists():
            self._write_payment_summaries([])
        if not self.shopify_payout_transactions_path.exists():
            self._write_shopify_payout_transactions([])
        if not self.paypal_transactions_raw_path.exists():
            self._write_paypal_transactions_raw([])

    def list_companies(self) -> list[dict[str, str]]:
        if self.database_url:
            with self._connect() as conn:
                rows = conn.execute(
                    f"""
                    SELECT DISTINCT company_code
                    FROM {SCHEMA_NAME}.suppliers
                    WHERE is_active = TRUE
                    ORDER BY company_code
                    """
                ).fetchall()
            codes = [row[0] for row in rows]
        else:
            codes = sorted({self._company_code(record.company) for record in load_provider_catalog()})
        return [
            {
                "code": code,
                "name": company_folder_name(self._company_name(code)),
            }
            for code in codes
        ]

    def list_suppliers(self, company: str | None = None) -> list[dict[str, str]]:
        if self.database_url:
            return self._list_suppliers_db(company=company)
        suppliers = load_provider_catalog()
        if company:
            normalized_company = self._company_name(self._company_code(company))
            suppliers = [record for record in suppliers if record.company == normalized_company]
        return [self._provider_record_to_dict(record) for record in suppliers]

    def list_review_items(self, status: str | None = None) -> list[StoredReviewItem]:
        if self.database_url:
            return self._list_review_items_db(status=status)
        items = self._read_items()
        if status:
            items = [item for item in items if item.status == status]
        return items

    def build_nightly_digest(self) -> NightlyReviewDigest:
        open_items = self.list_review_items(status="open")
        unmatched_items = tuple(
            UnmatchedInvoiceNotice(
                company=item.company,
                period_yyyymm=item.period_yyyymm,
                source_sender=item.source_sender,
                source_subject=item.source_subject,
                attachment_names=tuple(item.attachment_names or []),
                review_path=Path(item.review_path or "."),
                suggested_provider=item.suggested_provider,
                review_url="",
            )
            for item in open_items
            if item.kind == "unmatched_supplier"
        )
        return NightlyReviewDigest(
            company="ARTESTA REVIEW",
            period_yyyymm=open_items[0].period_yyyymm if open_items else "",
            unmatched_supplier_items=unmatched_items,
            notes="Resumen generado desde la API de lector_facturas.",
        )

    def get_review_item(self, review_item_id: str) -> StoredReviewItem:
        if self.database_url:
            return self._get_review_item_db(review_item_id)
        for item in self._read_items():
            if item.id == review_item_id:
                return item
        raise KeyError(review_item_id)

    def get_mail_sync_state(self, *, mailbox: str, sync_name: str) -> dict[str, object]:
        if not self.database_url:
            return {
                "mailbox": mailbox,
                "sync_name": sync_name,
                "last_processed_at": None,
                "last_processed_message_id": "",
            }
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT mailbox, sync_name, last_processed_at, last_processed_message_id
                FROM {SCHEMA_NAME}.mail_sync_state
                WHERE mailbox = %s AND sync_name = %s
                """,
                (mailbox, sync_name),
            ).fetchone()
        if row is None:
            return {
                "mailbox": mailbox,
                "sync_name": sync_name,
                "last_processed_at": None,
                "last_processed_message_id": "",
            }
        return {
            "mailbox": row[0],
            "sync_name": row[1],
            "last_processed_at": row[2],
            "last_processed_message_id": row[3] or "",
        }

    def upsert_mail_sync_state(
        self,
        *,
        mailbox: str,
        sync_name: str,
        last_processed_at,
        last_processed_message_id: str,
    ) -> dict[str, object]:
        if not self.database_url:
            return {
                "mailbox": mailbox,
                "sync_name": sync_name,
                "last_processed_at": last_processed_at,
                "last_processed_message_id": last_processed_message_id,
            }
        with self._connect() as conn:
            conn.execute(
                f"""
                INSERT INTO {SCHEMA_NAME}.mail_sync_state (
                    id, mailbox, sync_name, last_processed_at, last_processed_message_id, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, NOW(), NOW()
                )
                ON CONFLICT (mailbox, sync_name) DO UPDATE SET
                    last_processed_at = EXCLUDED.last_processed_at,
                    last_processed_message_id = EXCLUDED.last_processed_message_id,
                    updated_at = NOW()
                """,
                (
                    str(uuid.uuid5(uuid.NAMESPACE_URL, f"mail-sync:{mailbox}:{sync_name}")),
                    mailbox,
                    sync_name,
                    last_processed_at,
                    last_processed_message_id,
                ),
            )
            conn.commit()
        return self.get_mail_sync_state(mailbox=mailbox, sync_name=sync_name)

    def list_ingestion_queue(self, *, bucket: str | None = None) -> list[IngestionQueueItem]:
        if not self.database_url:
            return []
        query = f"""
            SELECT id, source, gmail_message_id, gmail_attachment_id, original_filename, stored_filename,
                   sender_email, subject, received_at, drive_file_id, drive_url, validation_bucket,
                   detected_supplier_code, detected_company_code, parser_name, parse_status, parse_error,
                   document_id, mime_type, heuristic_reason, created_at, updated_at
            FROM {SCHEMA_NAME}.ingestion_queue
        """
        params: list[object] = []
        if bucket:
            query += " WHERE validation_bucket = %s"
            params.append(bucket)
        query += " ORDER BY received_at NULLS LAST, created_at, id"
        with self._connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        return [self._row_to_ingestion_queue_item(row) for row in rows]

    def get_documents_by_ids(self, document_ids: list[str]) -> dict[str, dict[str, Any]]:
        """Return {document_id: {gross_amount, currency_code, windows_path, drive_url, invoice_number, invoice_date}} for given IDs."""
        if not self.database_url or not document_ids:
            return {}
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT id, gross_amount, currency_code, windows_path, drive_url, invoice_number, invoice_date
                FROM {SCHEMA_NAME}.documents
                WHERE id = ANY(%s)
                """,
                (document_ids,),
            ).fetchall()
        return {
            str(row[0]): {
                "gross_amount": float(row[1]) if row[1] is not None else None,
                "currency_code": str(row[2] or ""),
                "windows_path": str(row[3] or ""),
                "drive_url": str(row[4] or ""),
                "invoice_number": str(row[5] or ""),
                "invoice_date": str(row[6]) if row[6] is not None else "",
            }
            for row in rows
        }

    def get_ingestion_queue_item(self, queue_item_id: str) -> IngestionQueueItem:
        if not self.database_url:
            raise KeyError(queue_item_id)
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT id, source, gmail_message_id, gmail_attachment_id, original_filename, stored_filename,
                       sender_email, subject, received_at, drive_file_id, drive_url, validation_bucket,
                       detected_supplier_code, detected_company_code, parser_name, parse_status, parse_error,
                       document_id, mime_type, heuristic_reason, created_at, updated_at
                FROM {SCHEMA_NAME}.ingestion_queue
                WHERE id = %s
                """,
                (queue_item_id,),
            ).fetchone()
        if row is None:
            raise KeyError(queue_item_id)
        return self._row_to_ingestion_queue_item(row)

    def upsert_ingestion_queue_item(
        self,
        *,
        queue_item_id: str,
        source: str,
        gmail_message_id: str = "",
        gmail_attachment_id: str = "",
        original_filename: str = "",
        stored_filename: str = "",
        sender_email: str = "",
        subject: str = "",
        received_at=None,
        drive_file_id: str = "",
        drive_url: str = "",
        validation_bucket: str = "",
        detected_supplier_code: str = "",
        detected_company_code: str = "",
        parser_name: str = "",
        parse_status: str = "",
        parse_error: str = "",
        document_id: str = "",
        mime_type: str = "",
        heuristic_reason: str = "",
    ) -> IngestionQueueItem:
        if not self.database_url:
            raise RuntimeError("ingestion_queue requires DATABASE_URL")
        with self._connect() as conn:
            conn.execute(
                f"""
                INSERT INTO {SCHEMA_NAME}.ingestion_queue (
                    id, source, gmail_message_id, gmail_attachment_id, original_filename, stored_filename,
                    sender_email, subject, received_at, drive_file_id, drive_url, validation_bucket,
                    detected_supplier_code, detected_company_code, parser_name, parse_status, parse_error,
                    document_id, mime_type, heuristic_reason, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, NOW(), NOW()
                )
                ON CONFLICT (id) DO UPDATE SET
                    source = EXCLUDED.source,
                    gmail_message_id = EXCLUDED.gmail_message_id,
                    gmail_attachment_id = EXCLUDED.gmail_attachment_id,
                    original_filename = EXCLUDED.original_filename,
                    stored_filename = EXCLUDED.stored_filename,
                    sender_email = EXCLUDED.sender_email,
                    subject = EXCLUDED.subject,
                    received_at = EXCLUDED.received_at,
                    drive_file_id = EXCLUDED.drive_file_id,
                    drive_url = EXCLUDED.drive_url,
                    validation_bucket = EXCLUDED.validation_bucket,
                    detected_supplier_code = EXCLUDED.detected_supplier_code,
                    detected_company_code = EXCLUDED.detected_company_code,
                    parser_name = EXCLUDED.parser_name,
                    parse_status = EXCLUDED.parse_status,
                    parse_error = EXCLUDED.parse_error,
                    document_id = EXCLUDED.document_id,
                    mime_type = EXCLUDED.mime_type,
                    heuristic_reason = EXCLUDED.heuristic_reason,
                    updated_at = NOW()
                """,
                (
                    queue_item_id,
                    source,
                    gmail_message_id,
                    gmail_attachment_id,
                    original_filename,
                    stored_filename,
                    sender_email,
                    subject,
                    received_at,
                    drive_file_id,
                    drive_url,
                    validation_bucket,
                    detected_supplier_code,
                    detected_company_code,
                    parser_name,
                    parse_status,
                    parse_error,
                    document_id or None,
                    mime_type,
                    heuristic_reason,
                ),
            )
            conn.commit()
        return self.get_ingestion_queue_item(queue_item_id)

    def document_exists_by_business_key(
        self,
        *,
        company_code: str,
        supplier_code: str,
        invoice_number: str,
        division_invoice: str = "",
        document_type: str = "invoice",
    ) -> bool:
        if not self.database_url:
            return False
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT 1
                FROM {SCHEMA_NAME}.documents
                WHERE company_code = %s
                  AND supplier_code = %s
                  AND invoice_number = %s
                  AND division_invoice = %s
                  AND document_type = %s
                LIMIT 1
                """,
                (company_code, supplier_code, invoice_number, division_invoice, document_type),
            ).fetchone()
        return row is not None

    def insert_document_from_parsed(
        self,
        *,
        company_code: str,
        supplier_code: str,
        parsed,
        windows_path: str,
        drive_url: str,
        drive_file_id: str,
        original_filename: str,
        source_channel: str,
        email_message_id: str = "",
        email_thread_id: str = "",
        sender_email: str = "",
        source_subject: str = "",
        received_at=None,
        review_notes: str = "",
        division_invoice_override: str | None = None,
        gross_amount_override=None,
        net_amount_override=None,
        vat_amount_override=None,
    ) -> str:
        if not self.database_url:
            raise RuntimeError("documents require DATABASE_URL")
        document_id = str(uuid.uuid4())
        division_invoice = division_invoice_override if division_invoice_override is not None else getattr(parsed, "division_invoice", "")
        document_type = getattr(parsed, "document_type", "invoice")
        gross_amount = gross_amount_override if gross_amount_override is not None else parsed.gross_amount
        net_amount = net_amount_override if net_amount_override is not None else parsed.net_amount
        vat_amount = vat_amount_override if vat_amount_override is not None else parsed.vat_amount
        with self._connect() as conn:
            supplier_id = self._find_supplier_id(conn, company_code, supplier_code)
            conn.execute(
                f"""
                INSERT INTO {SCHEMA_NAME}.documents (
                    id, invoice_number, invoice_date, issuer_company_name, billed_company_name, supplier_name, company_code, windows_path, drive_url,
                    received_at, sender_email, original_filename, division_invoice, billing_period_start, billing_period_end, vat_percent, gross_amount,
                    vat_amount, net_amount, supplier_id, supplier_code, currency_code, drive_file_id, storage_root, document_type, status, source_channel,
                    email_message_id, email_thread_id, attachment_original_name, parser_name, parser_confidence, extracted_raw, review_notes,
                    created_at, updated_at, source_sender, source_subject, period_yyyymm
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s::jsonb, %s, NOW(), NOW(), %s, %s, %s
                )
                """,
                (
                    document_id,
                    parsed.invoice_number,
                    parsed.invoice_date,
                    parsed.issuer_company_name,
                    parsed.billed_company_name,
                    parsed.supplier_name,
                    company_code,
                    windows_path,
                    drive_url,
                    received_at,
                    sender_email or getattr(parsed, "sender_email", ""),
                    original_filename,
                    division_invoice,
                    parsed.billing_period_start,
                    parsed.billing_period_end,
                    parsed.vat_percent,
                    gross_amount,
                    vat_amount,
                    net_amount,
                    supplier_id,
                    supplier_code,
                    parsed.currency_code,
                    drive_file_id,
                    "GOOGLE_DRIVE",
                    document_type,
                    "classified",
                    source_channel,
                    email_message_id,
                    email_thread_id,
                    original_filename,
                    parsed.parser_name,
                    parsed.parser_confidence,
                    json.dumps(parsed.extracted_raw),
                    review_notes,
                    sender_email or getattr(parsed, "sender_email", ""),
                    source_subject,
                    parsed.period_yyyymm,
                ),
            )
            conn.commit()
        return document_id

    def delete_document(self, *, document_id: str) -> dict[str, Any]:
        if not self.database_url:
            raise RuntimeError("documents require DATABASE_URL")
        with self._connect() as conn:
            row = conn.execute(
                f"SELECT id, drive_file_id FROM {SCHEMA_NAME}.documents WHERE id = %s",
                (document_id,),
            ).fetchone()
            if not row:
                return {}
            conn.execute(f"DELETE FROM {SCHEMA_NAME}.documents WHERE id = %s", (document_id,))
            conn.commit()
        return {"id": str(row[0]), "drive_file_id": str(row[1] or "")}

    def document_exists_exact(self, *, email_message_id: str, original_filename: str) -> bool:
        if not self.database_url:
            return False
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT 1
                FROM {SCHEMA_NAME}.documents
                WHERE email_message_id = %s
                  AND original_filename = %s
                LIMIT 1
                """,
                (email_message_id, original_filename),
            ).fetchone()
        return row is not None

    def payroll_document_exists(self, *, email_message_id: str, original_filename: str) -> bool:
        if not self.database_url:
            return False
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT 1
                FROM {SCHEMA_NAME}.payroll_documents
                WHERE email_message_id = %s
                  AND original_filename = %s
                LIMIT 1
                """,
                (email_message_id, original_filename),
            ).fetchone()
        return row is not None

    def insert_payroll_document(
        self,
        *,
        company_code: str,
        period_yyyymm: str,
        document_type: str,
        windows_path: str,
        drive_url: str,
        drive_file_id: str,
        original_filename: str,
        stored_filename: str,
        sender_email: str,
        source_channel: str,
        email_message_id: str,
    ) -> str:
        if not self.database_url:
            raise RuntimeError("payroll_documents require DATABASE_URL")
        document_id = str(uuid.uuid4())
        with self._connect() as conn:
            conn.execute(
                f"""
                INSERT INTO {SCHEMA_NAME}.payroll_documents (
                    id, company_code, period_yyyymm, document_type,
                    windows_path, drive_url, drive_file_id,
                    original_filename, stored_filename,
                    source_sender, source_channel, email_message_id,
                    created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    NOW(), NOW()
                )
                """,
                (
                    document_id,
                    company_code,
                    period_yyyymm,
                    document_type,
                    windows_path,
                    drive_url,
                    drive_file_id,
                    original_filename,
                    stored_filename,
                    sender_email,
                    source_channel,
                    email_message_id,
                ),
            )
            conn.commit()
        return document_id

    def document_exists_by_original_filename(self, *, original_filename: str) -> bool:
        if not self.database_url:
            return False
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT 1
                FROM {SCHEMA_NAME}.documents
                WHERE original_filename = %s
                   OR attachment_original_name = %s
                LIMIT 1
                """,
                (original_filename, original_filename),
            ).fetchone()
        return row is not None

    def document_exists_by_normalized_filename(self, *, original_filename: str) -> bool:
        if not self.database_url:
            return False
        normalized = "".join(char.lower() for char in original_filename if char.isalnum())
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT 1
                FROM {SCHEMA_NAME}.documents
                WHERE regexp_replace(lower(original_filename), '[^a-z0-9]+', '', 'g') = %s
                   OR regexp_replace(lower(attachment_original_name), '[^a-z0-9]+', '', 'g') = %s
                LIMIT 1
                """,
                (normalized, normalized),
            ).fetchone()
        return row is not None

    def resolve_review_item(
        self,
        review_item_id: str,
        *,
        company: str,
        supplier_code: str,
        invoice_date: str,
        invoice_number: str,
    ) -> StoredReviewItem:
        if self.database_url:
            return self._resolve_review_item_db(
                review_item_id,
                company=company,
                supplier_code=supplier_code,
                invoice_date=invoice_date,
                invoice_number=invoice_number,
            )
        items = self._read_items()
        for item in items:
            if item.id != review_item_id:
                continue
            destination_file = ""
            if self.finance_root and item.source_file and Path(item.source_file).exists():
                result = apply_review_decision(
                    ReviewDecision(
                        root=self.finance_root,
                        source_file=Path(item.source_file),
                        company=company,
                        supplier_code=supplier_code,
                        invoice_date=invoice_date,
                        invoice_number=invoice_number,
                    )
                )
                destination_file = str(result.destination_file)
            item.status = "resolved"
            item.company = self._company_name(self._company_code(company))
            item.suggested_supplier_code = supplier_code
            item.destination_file = destination_file
            self._write_items(items)
            return item
        raise KeyError(review_item_id)

    def upsert_payment_order_transactions(self, transactions: list[PaymentOrderTransaction]) -> int:
        if self.database_url:
            return self._upsert_payment_order_transactions_db(transactions)
        existing = {item.external_transaction_id: item for item in self._read_payment_transactions()}
        for transaction in transactions:
            existing[transaction.external_transaction_id] = transaction
        self._write_payment_transactions(list(existing.values()))
        return len(transactions)

    def upsert_shopify_payout_transactions(self, records: list[dict[str, Any]]) -> int:
        if self.database_url:
            return self._upsert_shopify_payout_transactions_db(records)
        existing = {str(item.get("source_record_id", "")): item for item in self._read_shopify_payout_transactions()}
        for record in records:
            existing[str(record.get("source_record_id", ""))] = record
        self._write_shopify_payout_transactions(list(existing.values()))
        return len(records)

    def delete_shopify_payout_transactions_range(self, *, date_from: str, date_to: str) -> int:
        if self.database_url:
            return self._delete_shopify_payout_transactions_range_db(date_from=date_from, date_to=date_to)
        items = self._read_shopify_payout_transactions()
        kept = [
            item
            for item in items
            if not (date_from <= str(item.get("transaction_date", ""))[:10] <= date_to)
        ]
        deleted = len(items) - len(kept)
        self._write_shopify_payout_transactions(kept)
        return deleted

    def list_shopify_payout_transactions(
        self,
        *,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[dict[str, Any]]:
        if self.database_url:
            return self._list_shopify_payout_transactions_db(date_from=date_from, date_to=date_to)
        items = self._read_shopify_payout_transactions()
        if date_from:
            items = [item for item in items if str(item.get("transaction_date", ""))[:10] >= date_from]
        if date_to:
            items = [item for item in items if str(item.get("transaction_date", ""))[:10] <= date_to]
        return sorted(items, key=lambda item: (str(item.get("transaction_date", "")), str(item.get("source_record_id", ""))))

    def upsert_paypal_transactions_raw(self, records: list[dict[str, Any]]) -> int:
        if self.database_url:
            return self._upsert_paypal_transactions_raw_db(records)
        existing = {str(item.get("source_record_id", "")): item for item in self._read_paypal_transactions_raw()}
        for record in records:
            existing[str(record.get("source_record_id", ""))] = record
        self._write_paypal_transactions_raw(list(existing.values()))
        return len(records)

    def delete_paypal_transactions_range(self, *, date_from: str, date_to: str) -> int:
        if self.database_url:
            return self._delete_paypal_transactions_range_db(date_from=date_from, date_to=date_to)
        items = self._read_paypal_transactions_raw()
        kept = [
            item
            for item in items
            if not (date_from <= str(item.get("transaction_date", ""))[:10] <= date_to)
        ]
        deleted = len(items) - len(kept)
        self._write_paypal_transactions_raw(kept)
        return deleted

    def list_paypal_transactions_raw(
        self,
        *,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[dict[str, Any]]:
        if self.database_url:
            return self._list_paypal_transactions_raw_db(date_from=date_from, date_to=date_to)
        items = self._read_paypal_transactions_raw()
        if date_from:
            items = [item for item in items if str(item.get("transaction_date", ""))[:10] >= date_from]
        if date_to:
            items = [item for item in items if str(item.get("transaction_date", ""))[:10] <= date_to]
        return sorted(items, key=lambda item: (str(item.get("transaction_date", "")), str(item.get("source_record_id", ""))))

    def delete_payment_order_transactions_range(
        self,
        *,
        platform: str,
        date_from: str,
        date_to: str,
    ) -> int:
        if self.database_url:
            return self._delete_payment_order_transactions_range_db(
                platform=platform,
                date_from=date_from,
                date_to=date_to,
            )
        items = self._read_payment_transactions()
        kept = [
            item
            for item in items
            if not (
                item.platform == platform
                and date_from <= item.transaction_date[:10] <= date_to
            )
        ]
        deleted = len(items) - len(kept)
        self._write_payment_transactions(kept)
        return deleted

    def rebuild_payment_fee_monthly_summary(self, *, company_code: str, platform: str | None = None) -> int:
        summaries = summarize_payment_transactions(
            [
                item
                for item in self.list_payment_order_transactions(
                    company_code=company_code,
                    platform=platform,
                    include_unpaid_shopify=True,
                )
                if item.company_code == company_code
            ]
        )
        summaries = self._apply_manual_fee_summary_overrides(
            company_code=company_code,
            platform=platform,
            summaries=summaries,
        )
        if self.database_url:
            self._replace_payment_fee_monthly_summary_db(company_code=company_code, platform=platform, summaries=summaries)
        else:
            existing = self._read_payment_fee_summaries()
            if platform:
                existing = [item for item in existing if item.platform != platform]
            else:
                existing = []
            self._write_payment_summaries(existing + summaries)
        return len(summaries)

    def list_payment_order_transactions(
        self,
        *,
        company_code: str | None = None,
        platform: str | None = None,
        period_yyyymm: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        market_code: str | None = None,
        is_chargeback: bool | None = None,
        payout_id: str | None = None,
        include_unpaid_shopify: bool = False,
    ) -> list[PaymentOrderTransaction]:
        if self.database_url:
            return self._list_payment_order_transactions_db(
                company_code=company_code,
                platform=platform,
                period_yyyymm=period_yyyymm,
                date_from=date_from,
                date_to=date_to,
                market_code=market_code,
                is_chargeback=is_chargeback,
                payout_id=payout_id,
                include_unpaid_shopify=include_unpaid_shopify,
            )
        items = self._read_payment_transactions()
        if not include_unpaid_shopify:
            items = [
                item
                for item in items
                if item.platform != "shopify" or bool(item.payout_date)
            ]
        if company_code:
            items = [item for item in items if item.company_code == company_code]
        if platform:
            items = [item for item in items if item.platform == platform]
        if period_yyyymm:
            items = [item for item in items if item.period_yyyymm == period_yyyymm]
        if date_from:
            items = [item for item in items if item.transaction_date[:10] >= date_from]
        if date_to:
            items = [item for item in items if item.transaction_date[:10] <= date_to]
        if market_code:
            items = [item for item in items if item.market_code == market_code]
        if is_chargeback is not None:
            items = [item for item in items if item.is_chargeback is is_chargeback]
        if payout_id:
            items = [item for item in items if item.external_payout_id == payout_id]
        return sorted(items, key=lambda item: (item.transaction_date, item.external_transaction_id))

    def list_payment_fee_monthly_summary(
        self,
        *,
        company_code: str | None = None,
        platform: str | None = None,
        period_yyyymm: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> list[PaymentFeeSummaryRow]:
        if self.database_url:
            return self._list_payment_fee_monthly_summary_db(
                company_code=company_code,
                platform=platform,
                period_yyyymm=period_yyyymm,
                date_from=date_from,
                date_to=date_to,
            )
        items = self._read_payment_fee_summaries()
        if company_code:
            items = [item for item in items if item.company_code == company_code]
        if platform:
            items = [item for item in items if item.platform == platform]
        if period_yyyymm:
            items = [item for item in items if item.period_yyyymm == period_yyyymm]
        if date_from:
            from_period = date_from[:7].replace("-", "")
            items = [item for item in items if item.period_yyyymm >= from_period]
        if date_to:
            to_period = date_to[:7].replace("-", "")
            items = [item for item in items if item.period_yyyymm <= to_period]
        return sorted(items, key=lambda item: (item.period_yyyymm, item.platform, item.market_code))

    def _read_items(self) -> list[StoredReviewItem]:
        with self.storage_path.open("r", encoding="utf-8") as handle:
            raw_items = json.load(handle)
        return [StoredReviewItem(**raw_item) for raw_item in raw_items]

    def _write_items(self, items: list[StoredReviewItem]) -> None:
        with self.storage_path.open("w", encoding="utf-8") as handle:
            json.dump([asdict(item) for item in items], handle, ensure_ascii=False, indent=2)

    def _read_payment_transactions(self) -> list[PaymentOrderTransaction]:
        with self.payment_transactions_path.open("r", encoding="utf-8") as handle:
            raw_items = json.load(handle)
        return [PaymentOrderTransaction.from_json_dict(raw_item) for raw_item in raw_items]

    def _write_payment_transactions(self, items: list[PaymentOrderTransaction]) -> None:
        with self.payment_transactions_path.open("w", encoding="utf-8") as handle:
            json.dump([item.to_json_dict() for item in items], handle, ensure_ascii=False, indent=2)

    def _read_shopify_payout_transactions(self) -> list[dict[str, Any]]:
        with self.shopify_payout_transactions_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def _write_shopify_payout_transactions(self, items: list[dict[str, Any]]) -> None:
        with self.shopify_payout_transactions_path.open("w", encoding="utf-8") as handle:
            json.dump(items, handle, ensure_ascii=False, indent=2)

    def _read_paypal_transactions_raw(self) -> list[dict[str, Any]]:
        with self.paypal_transactions_raw_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def _write_paypal_transactions_raw(self, items: list[dict[str, Any]]) -> None:
        with self.paypal_transactions_raw_path.open("w", encoding="utf-8") as handle:
            json.dump(items, handle, ensure_ascii=False, indent=2)

    def _read_payment_fee_summaries(self) -> list[PaymentFeeSummaryRow]:
        with self.payment_summary_path.open("r", encoding="utf-8") as handle:
            raw_items = json.load(handle)
        return [PaymentFeeSummaryRow.from_json_dict(raw_item) for raw_item in raw_items]

    def _write_payment_summaries(self, items: list[PaymentFeeSummaryRow]) -> None:
        with self.payment_summary_path.open("w", encoding="utf-8") as handle:
            json.dump([item.to_json_dict() for item in items], handle, ensure_ascii=False, indent=2)

    def _apply_manual_fee_summary_overrides(
        self,
        *,
        company_code: str,
        platform: str | None,
        summaries: list[PaymentFeeSummaryRow],
    ) -> list[PaymentFeeSummaryRow]:
        if platform not in (None, "shopify", "paypal"):
            return summaries
        shopify_fee_map = self._shopify_excel_fee_totals(company_code=company_code) if platform in (None, "shopify") else {}
        paypal_fee_map = self._paypal_raw_fee_totals(company_code=company_code) if platform in (None, "paypal") else {}
        updated: list[PaymentFeeSummaryRow] = []
        for summary in summaries:
            if summary.platform == "shopify":
                fee_amount = shopify_fee_map.get((summary.period_yyyymm, summary.market_code, summary.currency_code), summary.fee_amount)
                total_cost_amount = fee_amount + summary.chargeback_amount + summary.chargeback_fee_amount
            elif summary.platform == "paypal":
                fee_amount = paypal_fee_map.get((summary.period_yyyymm, summary.market_code, summary.currency_code), summary.fee_amount)
                total_cost_amount = fee_amount - summary.chargeback_amount - summary.chargeback_fee_amount
            else:
                updated.append(summary)
                continue
            updated.append(
                PaymentFeeSummaryRow(
                    company_code=summary.company_code,
                    period_yyyymm=summary.period_yyyymm,
                    platform=summary.platform,
                    market_code=summary.market_code,
                    currency_code=summary.currency_code,
                    orders_count=summary.orders_count,
                    transactions_count=summary.transactions_count,
                    gross_amount=summary.gross_amount,
                    fee_amount=fee_amount,
                    chargeback_amount=summary.chargeback_amount,
                    chargeback_fee_amount=summary.chargeback_fee_amount,
                    total_cost_amount=total_cost_amount,
                    net_amount=summary.net_amount,
                    payout_count=summary.payout_count,
                )
            )
        return updated

    def _paypal_raw_fee_totals(self, *, company_code: str) -> dict[tuple[str, str, str], object]:
        if self.database_url:
            with self._connect() as conn:
                rows = conn.execute(
                    f"""
                    SELECT to_char(transaction_date AT TIME ZONE 'Europe/Madrid', 'YYYYMM') AS period_yyyymm,
                           market_code,
                           divisa,
                           COALESCE(SUM(tarifa), 0) AS fee_amount
                    FROM {SCHEMA_NAME}.paypal_transactions_raw
                    WHERE company_code = %s
                    GROUP BY 1, 2, 3
                    """,
                    (company_code,),
                ).fetchall()
            return {(row[0], row[1], row[2]): row[3] for row in rows}
        totals: dict[tuple[str, str, str], object] = {}
        for item in self._read_paypal_transactions_raw():
            if item.get("company_code") != company_code:
                continue
            transaction_date = str(item.get("transaction_date", ""))
            if not transaction_date:
                continue
            period_yyyymm = transaction_date[:7].replace("-", "")
            key = (period_yyyymm, str(item.get("market_code", "")), str(item.get("divisa", "")))
            current = totals.get(key, parse_decimal("0"))
            totals[key] = current + parse_decimal(item.get("tarifa"))
        return totals

    def _shopify_excel_fee_totals(self, *, company_code: str) -> dict[tuple[str, str, str], object]:
        if self.database_url:
            with self._connect() as conn:
                rows = conn.execute(
                    f"""
                    SELECT to_char(transaction_date AT TIME ZONE 'Europe/Madrid', 'YYYYMM') AS period_yyyymm,
                           market_code,
                           currency,
                           COALESCE(SUM(fee), 0) AS fee_amount
                    FROM {SCHEMA_NAME}.shopify_payout_transactions
                    WHERE company_code = %s
                    GROUP BY 1, 2, 3
                    """,
                    (company_code,),
                ).fetchall()
            return {(row[0], row[1], row[2]): row[3] for row in rows}
        totals: dict[tuple[str, str, str], object] = {}
        for item in self._read_shopify_payout_transactions():
            if item.get("company_code") != company_code:
                continue
            transaction_date = str(item.get("transaction_date", ""))
            if not transaction_date:
                continue
            period_yyyymm = transaction_date[:7].replace("-", "")
            key = (period_yyyymm, str(item.get("market_code", "")), str(item.get("currency", "")))
            current = totals.get(key, parse_decimal("0"))
            totals[key] = current + parse_decimal(item.get("fee"))
        return totals

    def _seed_items(self) -> list[StoredReviewItem]:
        return [
            StoredReviewItem(
                id=str(uuid.uuid4()),
                kind="unmatched_supplier",
                status="open",
                company="ARTESTA INC",
                period_yyyymm="202601",
                source_sender="andrea@artestastore.com",
                source_subject="Factura_2026-0009.pdf",
                attachment_names=["Factura_2026-0009.pdf"],
                review_path=r"ARTESTA - 6. Finances\Artesta Inc\2026\202601\validation\pending_supplier_review",
                source_file=r"ARTESTA - 6. Finances\Artesta Inc\2026\202601\validation\pending_supplier_review\Factura_2026-0009.pdf",
                suggested_provider="ARTESTA STORE, S.L.",
                suggested_supplier_code="SHAREDSERVICESSL",
                notes="Sample seeded review item for API integration.",
            ),
            StoredReviewItem(
                id=str(uuid.uuid4()),
                kind="unmatched_supplier",
                status="open",
                company="ARTESTA STORE, S.L.",
                period_yyyymm="202601",
                source_sender="adobe@notification.example",
                source_subject="IEE2026001813920.pdf",
                attachment_names=["IEE2026001813920.pdf"],
                review_path=r"ARTESTA - 6. Finances\Artesta Store, S.L\2026\202601\validation\pending_supplier_review",
                source_file=r"ARTESTA - 6. Finances\Artesta Store, S.L\2026\202601\validation\pending_supplier_review\IEE2026001813920.pdf",
                suggested_provider="ADOBE SYSTEMS SOFTWARE IRELAND LTD",
                suggested_supplier_code="ADOBE",
                notes="Sample seeded review item for API integration.",
            ),
        ]

    def _connect(self):
        return psycopg.connect(self.database_url)

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}")
            conn.execute(self._suppliers_table_sql())
            conn.execute(self._documents_table_sql())
            conn.execute(self._review_items_table_sql())
            conn.execute(self._shopify_payout_transactions_table_sql())
            conn.execute(self._paypal_transactions_raw_table_sql())
            conn.execute(self._payment_order_transactions_table_sql())
            conn.execute(self._payment_fee_monthly_summary_table_sql())
            conn.execute(self._payroll_documents_table_sql())
            conn.execute(self._otros_gastos_table_sql())
            conn.execute(self._otros_ingresos_table_sql())
            conn.execute(self._artist_royalties_documents_table_sql())
            conn.execute(self._artist_royalties_monthly_summary_table_sql())
            conn.execute(self._expected_invoices_table_sql())
            conn.execute(self._expected_invoice_runs_table_sql())
            conn.execute(self._mail_sync_state_table_sql())
            conn.execute(self._ingestion_queue_table_sql())
            self._ensure_suppliers_columns(conn)
            self._ensure_documents_columns(conn)
            conn.execute(
                f"""
                ALTER TABLE {SCHEMA_NAME}.paypal_transactions_raw
                ADD COLUMN IF NOT EXISTS shopify_order_name TEXT NOT NULL DEFAULT ''
                """
            )
            conn.execute(
                f"""
                ALTER TABLE {SCHEMA_NAME}.payroll_documents
                ADD COLUMN IF NOT EXISTS stored_filename TEXT NOT NULL DEFAULT ''
                """
            )
            conn.execute(
                f"""
                ALTER TABLE {SCHEMA_NAME}.payroll_documents
                ADD COLUMN IF NOT EXISTS email_message_id TEXT NOT NULL DEFAULT ''
                """
            )
            conn.execute("DROP INDEX IF EXISTS invoices_documents_unique_invoice_idx")
            conn.execute(self._documents_unique_index_sql())
            conn.execute(self._documents_exact_email_duplicate_index_sql())
            conn.execute(self._ingestion_queue_bucket_index_sql())
            conn.execute(self._ingestion_queue_source_index_sql())
            conn.execute(self._review_items_status_index_sql())
            conn.execute(self._documents_period_index_sql())
            conn.execute(self._shopify_payout_transactions_unique_index_sql())
            conn.execute(self._shopify_payout_transactions_period_index_sql())
            conn.execute(self._paypal_transactions_raw_unique_index_sql())
            conn.execute(self._paypal_transactions_raw_period_index_sql())
            conn.execute(self._payment_order_transactions_unique_index_sql())
            conn.execute(self._payment_order_transactions_period_index_sql())
            conn.execute(self._payment_fee_monthly_summary_unique_index_sql())
            conn.execute(self._payroll_documents_unique_index_sql())
            conn.execute(self._otros_gastos_unique_index_sql())
            conn.execute(self._otros_ingresos_unique_index_sql())
            conn.execute(self._artist_royalties_documents_unique_index_sql())
            conn.execute(self._artist_royalties_documents_period_index_sql())
            conn.execute(self._artist_royalties_summary_unique_index_sql())
            self._seed_suppliers(conn)
            if self._count_rows(conn, f"{SCHEMA_NAME}.review_items") == 0:
                if self._legacy_review_items_exists(conn):
                    self._migrate_legacy_review_items(conn)
            conn.commit()

    def _seed_suppliers(self, conn) -> None:
        for record in load_provider_catalog():
            company_code = self._company_code(record.company)
            supplier_id = self._resolve_seed_supplier_id(
                conn,
                company_code=company_code,
                company_name=record.company,
                supplier_code=record.supplier_code,
            )
            conn.execute(
                f"""
                INSERT INTO {SCHEMA_NAME}.suppliers (
                    id, company_code, current_folder, supplier_code, supplier_name,
                    billing_company_name, destination_path, is_active, notes, sender_emails
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, TRUE, %s, %s
                )
                ON CONFLICT (company_code, supplier_code) DO UPDATE SET
                    current_folder = EXCLUDED.current_folder,
                    supplier_name = EXCLUDED.supplier_name,
                    billing_company_name = EXCLUDED.billing_company_name,
                    destination_path = EXCLUDED.destination_path,
                    is_active = TRUE,
                    notes = EXCLUDED.notes,
                    sender_emails = EXCLUDED.sender_emails,
                    updated_at = NOW()
                """,
                (
                    supplier_id,
                    company_code,
                    record.current_folder,
                    record.supplier_code,
                    record.supplier_code,
                    record.provider_name,
                    record.destination_path,
                    record.notes,
                    json.dumps(list(record.sender_emails)),
                ),
            )

    def _resolve_seed_supplier_id(self, conn, *, company_code: str, company_name: str, supplier_code: str) -> str:
        existing = conn.execute(
            f"""
            SELECT id
            FROM {SCHEMA_NAME}.suppliers
            WHERE company_code = %s AND supplier_code = %s
            """,
            (company_code, supplier_code),
        ).fetchone()
        if existing:
            return str(existing[0])

        preferred_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"suppliers:{company_code}:{supplier_code}"))
        conflicting = conn.execute(
            f"""
            SELECT company_code, supplier_code
            FROM {SCHEMA_NAME}.suppliers
            WHERE id = %s
            """,
            (preferred_id,),
        ).fetchone()
        if not conflicting:
            return preferred_id

        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"suppliers:{company_name}:{company_code}:{supplier_code}"))

    def _ensure_suppliers_columns(self, conn) -> None:
        for column_name, column_definition in SUPPLIERS_COLUMN_DEFINITIONS:
            conn.execute(
                f"""
                ALTER TABLE {SCHEMA_NAME}.suppliers
                ADD COLUMN IF NOT EXISTS {column_name} {column_definition}
                """
            )

    def _seed_review_items(self, conn) -> None:
        for item in self._seed_items():
            self._insert_seeded_review_item(conn, item)

    def _ensure_documents_columns(self, conn) -> None:
        for column_name, column_definition in DOCUMENTS_COLUMN_DEFINITIONS:
            conn.execute(
                f"""
                ALTER TABLE {SCHEMA_NAME}.documents
                ADD COLUMN IF NOT EXISTS {column_name} {column_definition}
                """
            )

    def _insert_seeded_review_item(self, conn, item: StoredReviewItem) -> None:
        document_id = str(uuid.uuid4())
        company_code = self._company_code(item.company)
        supplier_id = self._find_supplier_id(conn, company_code, item.suggested_supplier_code)
        attachment_name = (item.attachment_names or [""])[0]
        conn.execute(
            f"""
            INSERT INTO {SCHEMA_NAME}.documents (
                id, invoice_number, invoice_date, issuer_company_name, billed_company_name,
                supplier_name, company_code, windows_path, drive_url, vat_percent, gross_amount,
                vat_amount, net_amount, supplier_id, supplier_code, currency_code, drive_file_id,
                storage_root, document_type, status, source_channel, email_message_id, email_thread_id,
                attachment_original_name, parser_name, parser_confidence, extracted_raw, review_notes,
                created_at, updated_at, source_sender, source_subject, period_yyyymm,
                received_at, sender_email, original_filename
            ) VALUES (
                %s, '', NULL, %s, %s,
                %s, %s, %s, '', NULL, NULL,
                NULL, NULL, %s, %s, '', '',
                'GOOGLE_DRIVE', 'invoice', 'pending_review', 'import', '', '',
                %s, '', NULL, %s::jsonb, %s,
                NOW(), NOW(), %s, %s, %s,
                NULL, %s, %s
            )
            """,
            (
                document_id,
                item.suggested_provider,
                item.company,
                item.suggested_provider,
                company_code,
                item.source_file,
                supplier_id,
                item.suggested_supplier_code,
                attachment_name,
                json.dumps({}),
                item.notes,
                item.source_sender,
                item.source_subject,
                item.period_yyyymm,
                item.source_sender,
                attachment_name,
            ),
        )
        conn.execute(
            f"""
            INSERT INTO {SCHEMA_NAME}.review_items (
                id, document_id, kind, status, suggested_supplier_code,
                suggested_supplier_name, notes
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                item.id,
                document_id,
                item.kind,
                item.status,
                item.suggested_supplier_code,
                item.suggested_provider,
                item.notes,
            ),
        )

    def _legacy_review_items_exists(self, conn) -> bool:
        row = conn.execute("SELECT to_regclass('public.review_items')").fetchone()
        return bool(row and row[0])

    def _migrate_legacy_review_items(self, conn) -> None:
        rows = conn.execute(
            """
            SELECT id, kind, status, company, period_yyyymm, source_sender, source_subject,
                   attachment_names, review_path, source_file, suggested_provider,
                   suggested_supplier_code, notes, destination_file, drive_file_id, drive_view_url
            FROM public.review_items
            ORDER BY period_yyyymm, source_subject
            """
        ).fetchall()
        for row in rows:
            item = StoredReviewItem(
                id=str(row[0]),
                kind=row[1],
                status=row[2],
                company=row[3],
                period_yyyymm=row[4],
                source_sender=row[5],
                source_subject=row[6],
                attachment_names=json.loads(row[7] or "[]"),
                review_path=row[8],
                source_file=row[9] or self._compose_legacy_source_file(row[8], row[6]),
                suggested_provider=row[10],
                suggested_supplier_code=row[11],
                notes=row[12],
                destination_file=row[13],
                drive_file_id=row[14],
                drive_view_url=row[15],
            )
            self._insert_seeded_review_item(conn, item)

    def _upsert_payment_order_transactions_db(self, transactions: list[PaymentOrderTransaction]) -> int:
        if not transactions:
            return 0
        chunk_size = 100
        for start in range(0, len(transactions), chunk_size):
            chunk = transactions[start:start + chunk_size]
            with self._connect() as conn:
                for transaction in chunk:
                    conn.execute(
                        f"""
                        INSERT INTO {SCHEMA_NAME}.payment_order_transactions (
                            id, platform, company_code, market_code, currency_code, order_id, order_name,
                            external_transaction_id, external_payout_id, transaction_date, payout_date,
                            transaction_type, status, gross_amount, fee_amount, net_amount,
                            chargeback_amount, chargeback_fee_amount, affects_balance, is_cancelled,
                            is_chargeback, payment_reference, customer_reference, raw_payload, period_yyyymm,
                            created_at, updated_at
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s, %s::jsonb, %s,
                            NOW(), NOW()
                        )
                        ON CONFLICT (platform, external_transaction_id) DO UPDATE SET
                            company_code = EXCLUDED.company_code,
                            market_code = EXCLUDED.market_code,
                            currency_code = EXCLUDED.currency_code,
                            order_id = EXCLUDED.order_id,
                            order_name = EXCLUDED.order_name,
                            external_payout_id = EXCLUDED.external_payout_id,
                            transaction_date = EXCLUDED.transaction_date,
                            payout_date = EXCLUDED.payout_date,
                            transaction_type = EXCLUDED.transaction_type,
                            status = EXCLUDED.status,
                            gross_amount = EXCLUDED.gross_amount,
                            fee_amount = EXCLUDED.fee_amount,
                            net_amount = EXCLUDED.net_amount,
                            chargeback_amount = EXCLUDED.chargeback_amount,
                            chargeback_fee_amount = EXCLUDED.chargeback_fee_amount,
                            affects_balance = EXCLUDED.affects_balance,
                            is_cancelled = EXCLUDED.is_cancelled,
                            is_chargeback = EXCLUDED.is_chargeback,
                            payment_reference = EXCLUDED.payment_reference,
                            customer_reference = EXCLUDED.customer_reference,
                            raw_payload = EXCLUDED.raw_payload,
                            period_yyyymm = EXCLUDED.period_yyyymm,
                            updated_at = NOW()
                        """,
                        (
                            transaction.id,
                            transaction.platform,
                            transaction.company_code,
                            transaction.market_code,
                            transaction.currency_code,
                            transaction.order_id,
                            transaction.order_name,
                            transaction.external_transaction_id,
                            transaction.external_payout_id,
                            transaction.transaction_date,
                            transaction.payout_date or None,
                            transaction.transaction_type,
                            transaction.status,
                            transaction.gross_amount,
                            transaction.fee_amount,
                            transaction.net_amount,
                            transaction.chargeback_amount,
                            transaction.chargeback_fee_amount,
                            transaction.affects_balance,
                            transaction.is_cancelled,
                            transaction.is_chargeback,
                            transaction.payment_reference,
                            transaction.customer_reference,
                            json.dumps(transaction.raw_payload or {}),
                            transaction.period_yyyymm,
                        ),
                    )
                conn.commit()
        return len(transactions)

    def _upsert_shopify_payout_transactions_db(self, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0
        with self._connect() as conn:
            for record in records:
                conn.execute(
                    f"""
                    INSERT INTO {SCHEMA_NAME}.shopify_payout_transactions (
                        id, source_record_id, transaction_date, type, order_id, order_name,
                        card_brand, card_source, payout_status, payout_date, payout_id,
                        available_on, amount, fee, net, checkout, payment_method_name,
                        presentment_amount, presentment_currency, currency, vat,
                        business_entity_name, business_entity_id, company_code, market_code,
                        raw_payload, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s::jsonb, NOW(), NOW()
                    )
                    ON CONFLICT (source_record_id) DO UPDATE SET
                        transaction_date = EXCLUDED.transaction_date,
                        type = EXCLUDED.type,
                        order_id = EXCLUDED.order_id,
                        order_name = EXCLUDED.order_name,
                        card_brand = EXCLUDED.card_brand,
                        card_source = EXCLUDED.card_source,
                        payout_status = EXCLUDED.payout_status,
                        payout_date = EXCLUDED.payout_date,
                        payout_id = EXCLUDED.payout_id,
                        available_on = EXCLUDED.available_on,
                        amount = EXCLUDED.amount,
                        fee = EXCLUDED.fee,
                        net = EXCLUDED.net,
                        checkout = EXCLUDED.checkout,
                        payment_method_name = EXCLUDED.payment_method_name,
                        presentment_amount = EXCLUDED.presentment_amount,
                        presentment_currency = EXCLUDED.presentment_currency,
                        currency = EXCLUDED.currency,
                        vat = EXCLUDED.vat,
                        business_entity_name = EXCLUDED.business_entity_name,
                        business_entity_id = EXCLUDED.business_entity_id,
                        company_code = EXCLUDED.company_code,
                        market_code = EXCLUDED.market_code,
                        raw_payload = EXCLUDED.raw_payload,
                        updated_at = NOW()
                    """,
                    (
                        str(uuid.uuid5(uuid.NAMESPACE_URL, f"shopify-payout-row:{record.get('source_record_id', '')}")),
                        record.get("source_record_id", ""),
                        record.get("transaction_date") or None,
                        record.get("type", ""),
                        record.get("order_id", ""),
                        record.get("order_name", ""),
                        record.get("card_brand", ""),
                        record.get("card_source", ""),
                        record.get("payout_status", ""),
                        record.get("payout_date") or None,
                        record.get("payout_id", ""),
                        record.get("available_on") or None,
                        record.get("amount", "0.00"),
                        record.get("fee", "0.00"),
                        record.get("net", "0.00"),
                        record.get("checkout", ""),
                        record.get("payment_method_name", ""),
                        record.get("presentment_amount") or None,
                        record.get("presentment_currency", ""),
                        record.get("currency", ""),
                        record.get("vat", "0.00"),
                        record.get("business_entity_name", ""),
                        record.get("business_entity_id", ""),
                        record.get("company_code", ""),
                        record.get("market_code", ""),
                        json.dumps(record.get("raw_payload", {}) or {}),
                    ),
                )
            conn.commit()
        return len(records)

    def _upsert_paypal_transactions_raw_db(self, records: list[dict[str, Any]]) -> int:
        if not records:
            return 0
        with self._connect() as conn:
            for record in records:
                conn.execute(
                    f"""
                    INSERT INTO {SCHEMA_NAME}.paypal_transactions_raw (
                        id, source_record_id, transaction_date, fecha, hora, zona_horaria,
                        nombre, tipo, estado, divisa, bruto, tarifa, neto,
                        sender_email, recipient_email, transaction_id, shipping_address,
                        address_status, item_name, item_id, shipping_amount, insurance_amount,
                        sales_tax_amount, option1_name, option1_value, option2_name, option2_value,
                        reference_transaction_id, invoice_number, custom_number, quantity,
                        receipt_id, balance_amount, address_line_1, address_line_2, city, region,
                        postal_code, country, contact_phone, subject, note, country_code,
                        balance_impact, order_number, shopify_order_name, company_code, market_code, raw_payload,
                        created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s::jsonb,
                        NOW(), NOW()
                    )
                    ON CONFLICT (source_record_id) DO UPDATE SET
                        transaction_date = EXCLUDED.transaction_date,
                        fecha = EXCLUDED.fecha,
                        hora = EXCLUDED.hora,
                        zona_horaria = EXCLUDED.zona_horaria,
                        nombre = EXCLUDED.nombre,
                        tipo = EXCLUDED.tipo,
                        estado = EXCLUDED.estado,
                        divisa = EXCLUDED.divisa,
                        bruto = EXCLUDED.bruto,
                        tarifa = EXCLUDED.tarifa,
                        neto = EXCLUDED.neto,
                        sender_email = EXCLUDED.sender_email,
                        recipient_email = EXCLUDED.recipient_email,
                        transaction_id = EXCLUDED.transaction_id,
                        shipping_address = EXCLUDED.shipping_address,
                        address_status = EXCLUDED.address_status,
                        item_name = EXCLUDED.item_name,
                        item_id = EXCLUDED.item_id,
                        shipping_amount = EXCLUDED.shipping_amount,
                        insurance_amount = EXCLUDED.insurance_amount,
                        sales_tax_amount = EXCLUDED.sales_tax_amount,
                        option1_name = EXCLUDED.option1_name,
                        option1_value = EXCLUDED.option1_value,
                        option2_name = EXCLUDED.option2_name,
                        option2_value = EXCLUDED.option2_value,
                        reference_transaction_id = EXCLUDED.reference_transaction_id,
                        invoice_number = EXCLUDED.invoice_number,
                        custom_number = EXCLUDED.custom_number,
                        quantity = EXCLUDED.quantity,
                        receipt_id = EXCLUDED.receipt_id,
                        balance_amount = EXCLUDED.balance_amount,
                        address_line_1 = EXCLUDED.address_line_1,
                        address_line_2 = EXCLUDED.address_line_2,
                        city = EXCLUDED.city,
                        region = EXCLUDED.region,
                        postal_code = EXCLUDED.postal_code,
                        country = EXCLUDED.country,
                        contact_phone = EXCLUDED.contact_phone,
                        subject = EXCLUDED.subject,
                        note = EXCLUDED.note,
                        country_code = EXCLUDED.country_code,
                        balance_impact = EXCLUDED.balance_impact,
                        order_number = EXCLUDED.order_number,
                        shopify_order_name = EXCLUDED.shopify_order_name,
                        company_code = EXCLUDED.company_code,
                        market_code = EXCLUDED.market_code,
                        raw_payload = EXCLUDED.raw_payload,
                        updated_at = NOW()
                    """,
                    (
                        str(uuid.uuid5(uuid.NAMESPACE_URL, f"paypal-raw-row:{record.get('source_record_id', '')}")),
                        record.get("source_record_id", ""),
                        record.get("transaction_date") or None,
                        record.get("fecha", ""),
                        record.get("hora", ""),
                        record.get("zona_horaria", ""),
                        record.get("nombre", ""),
                        record.get("tipo", ""),
                        record.get("estado", ""),
                        record.get("divisa", ""),
                        record.get("bruto", "0.00"),
                        record.get("tarifa", "0.00"),
                        record.get("neto", "0.00"),
                        record.get("sender_email", ""),
                        record.get("recipient_email", ""),
                        record.get("transaction_id", ""),
                        record.get("shipping_address", ""),
                        record.get("address_status", ""),
                        record.get("item_name", ""),
                        record.get("item_id", ""),
                        record.get("shipping_amount", "0.00"),
                        record.get("insurance_amount", "0.00"),
                        record.get("sales_tax_amount", "0.00"),
                        record.get("option1_name", ""),
                        record.get("option1_value", ""),
                        record.get("option2_name", ""),
                        record.get("option2_value", ""),
                        record.get("reference_transaction_id", ""),
                        record.get("invoice_number", ""),
                        record.get("custom_number", ""),
                        record.get("quantity", ""),
                        record.get("receipt_id", ""),
                        record.get("balance_amount") or None,
                        record.get("address_line_1", ""),
                        record.get("address_line_2", ""),
                        record.get("city", ""),
                        record.get("region", ""),
                        record.get("postal_code", ""),
                        record.get("country", ""),
                        record.get("contact_phone", ""),
                        record.get("subject", ""),
                        record.get("note", ""),
                        record.get("country_code", ""),
                        record.get("balance_impact", ""),
                        record.get("order_number", ""),
                        record.get("shopify_order_name", ""),
                        record.get("company_code", ""),
                        record.get("market_code", ""),
                        json.dumps(record.get("raw_payload", {}) or {}),
                    ),
                )
            conn.commit()
        return len(records)

    def _replace_payment_fee_monthly_summary_db(
        self,
        *,
        company_code: str,
        platform: str | None,
        summaries: list[PaymentFeeSummaryRow],
    ) -> None:
        with self._connect() as conn:
            if platform:
                conn.execute(
                    f"DELETE FROM {SCHEMA_NAME}.payment_fee_monthly_summary WHERE company_code = %s AND platform = %s",
                    (company_code, platform),
                )
            else:
                conn.execute(
                    f"DELETE FROM {SCHEMA_NAME}.payment_fee_monthly_summary WHERE company_code = %s",
                    (company_code,),
                )
            for summary in summaries:
                conn.execute(
                    f"""
                    INSERT INTO {SCHEMA_NAME}.payment_fee_monthly_summary (
                        id, company_code, period_yyyymm, platform, market_code, currency_code,
                        orders_count, transactions_count, gross_amount, fee_amount,
                        chargeback_amount, chargeback_fee_amount, total_cost_amount, net_amount,
                        payout_count, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, NOW(), NOW()
                    )
                    """,
                    (
                        str(uuid.uuid5(uuid.NAMESPACE_URL, f"payment-summary:{company_code}:{summary.period_yyyymm}:{summary.platform}:{summary.market_code}:{summary.currency_code}")),
                        company_code,
                        summary.period_yyyymm,
                        summary.platform,
                        summary.market_code,
                        summary.currency_code,
                        summary.orders_count,
                        summary.transactions_count,
                        summary.gross_amount,
                        summary.fee_amount,
                        summary.chargeback_amount,
                        summary.chargeback_fee_amount,
                        summary.total_cost_amount,
                        summary.net_amount,
                        summary.payout_count,
                    ),
                )
                conn.commit()

    def _delete_payment_order_transactions_range_db(
        self,
        *,
        platform: str,
        date_from: str,
        date_to: str,
    ) -> int:
        with self._connect() as conn:
            row = conn.execute(
                f"""
                DELETE FROM {SCHEMA_NAME}.payment_order_transactions
                WHERE platform = %s
                  AND transaction_date >= %s::timestamptz
                  AND transaction_date <= %s::timestamptz
                RETURNING 1
                """,
                (platform, f"{date_from}T00:00:00Z", f"{date_to}T23:59:59Z"),
            ).fetchall()
            conn.commit()
        return len(row)

    def _delete_shopify_payout_transactions_range_db(self, *, date_from: str, date_to: str) -> int:
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                DELETE FROM {SCHEMA_NAME}.shopify_payout_transactions
                WHERE transaction_date >= %s::timestamptz
                  AND transaction_date <= %s::timestamptz
                RETURNING 1
                """,
                (f"{date_from}T00:00:00Z", f"{date_to}T23:59:59Z"),
            ).fetchall()
            conn.commit()
        return len(rows)

    def _delete_paypal_transactions_range_db(self, *, date_from: str, date_to: str) -> int:
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                DELETE FROM {SCHEMA_NAME}.paypal_transactions_raw
                WHERE transaction_date >= %s::timestamptz
                  AND transaction_date <= %s::timestamptz
                RETURNING 1
                """,
                (f"{date_from}T00:00:00Z", f"{date_to}T23:59:59Z"),
            ).fetchall()
            conn.commit()
        return len(rows)

    def _list_payment_order_transactions_db(
        self,
        *,
        company_code: str | None,
        platform: str | None,
        period_yyyymm: str | None,
        date_from: str | None,
        date_to: str | None,
        market_code: str | None,
        is_chargeback: bool | None,
        payout_id: str | None,
        include_unpaid_shopify: bool = False,
    ) -> list[PaymentOrderTransaction]:
        query = f"""
            SELECT id, platform, company_code, market_code, currency_code, order_id, order_name,
                   external_transaction_id, external_payout_id, transaction_date, payout_date,
                   transaction_type, status, gross_amount, fee_amount, net_amount,
                   chargeback_amount, chargeback_fee_amount, affects_balance, is_cancelled,
                   is_chargeback, payment_reference, customer_reference, raw_payload
            FROM {SCHEMA_NAME}.payment_order_transactions
            WHERE 1=1
        """
        if not include_unpaid_shopify:
            query += " AND (platform <> 'shopify' OR payout_date IS NOT NULL)"
        params: list[object] = []
        if company_code:
            query += " AND company_code = %s"
            params.append(company_code)
        if platform:
            query += " AND platform = %s"
            params.append(platform)
        if period_yyyymm:
            query += " AND period_yyyymm = %s"
            params.append(period_yyyymm)
        if date_from:
            query += " AND transaction_date >= %s::timestamptz"
            params.append(f"{date_from}T00:00:00Z")
        if date_to:
            query += " AND transaction_date <= %s::timestamptz"
            params.append(f"{date_to}T23:59:59Z")
        if market_code:
            query += " AND market_code = %s"
            params.append(market_code)
        if is_chargeback is not None:
            query += " AND is_chargeback = %s"
            params.append(is_chargeback)
        if payout_id:
            query += " AND external_payout_id = %s"
            params.append(payout_id)
        query += " ORDER BY transaction_date, external_transaction_id"
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [
            PaymentOrderTransaction(
                id=str(row[0]),
                platform=row[1],
                company_code=row[2],
                market_code=row[3],
                currency_code=row[4],
                order_id=row[5] or "",
                order_name=row[6] or "",
                external_transaction_id=row[7],
                external_payout_id=row[8] or "",
                transaction_date=row[9].isoformat().replace("+00:00", "Z"),
                payout_date=row[10].isoformat().replace("+00:00", "Z") if row[10] else "",
                transaction_type=row[11] or "",
                status=row[12] or "",
                gross_amount=row[13],
                fee_amount=row[14],
                net_amount=row[15],
                chargeback_amount=row[16],
                chargeback_fee_amount=row[17],
                affects_balance=bool(row[18]),
                is_cancelled=bool(row[19]),
                is_chargeback=bool(row[20]),
                payment_reference=row[21] or "",
                customer_reference=row[22] or "",
                raw_payload=row[23] or {},
            )
            for row in rows
        ]

    def _list_shopify_payout_transactions_db(
        self,
        *,
        date_from: str | None,
        date_to: str | None,
    ) -> list[dict[str, Any]]:
        query = f"""
            SELECT source_record_id, transaction_date, type, order_id, order_name, card_brand, card_source,
                   payout_status, payout_date, payout_id, available_on, amount, fee, net, checkout,
                   payment_method_name, presentment_amount, presentment_currency, currency, vat,
                   business_entity_name, business_entity_id, company_code, market_code, raw_payload
            FROM {SCHEMA_NAME}.shopify_payout_transactions
            WHERE 1=1
        """
        params: list[object] = []
        if date_from:
            query += " AND transaction_date >= %s::timestamptz"
            params.append(f"{date_from}T00:00:00Z")
        if date_to:
            query += " AND transaction_date <= %s::timestamptz"
            params.append(f"{date_to}T23:59:59Z")
        query += " ORDER BY transaction_date, source_record_id"
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [
            {
                "source_record_id": row[0],
                "transaction_date": row[1].isoformat().replace("+00:00", "Z") if row[1] else "",
                "type": row[2] or "",
                "order_id": row[3] or "",
                "order_name": row[4] or "",
                "card_brand": row[5] or "",
                "card_source": row[6] or "",
                "payout_status": row[7] or "",
                "payout_date": row[8].isoformat().replace("+00:00", "Z") if row[8] else "",
                "payout_id": row[9] or "",
                "available_on": row[10].isoformat().replace("+00:00", "Z") if row[10] else "",
                "amount": str(row[11]),
                "fee": str(row[12]),
                "net": str(row[13]),
                "checkout": row[14] or "",
                "payment_method_name": row[15] or "",
                "presentment_amount": str(row[16]) if row[16] is not None else "",
                "presentment_currency": row[17] or "",
                "currency": row[18] or "",
                "vat": str(row[19]),
                "business_entity_name": row[20] or "",
                "business_entity_id": row[21] or "",
                "company_code": row[22] or "",
                "market_code": row[23] or "",
                "raw_payload": row[24] or {},
            }
            for row in rows
        ]

    def _list_paypal_transactions_raw_db(
        self,
        *,
        date_from: str | None,
        date_to: str | None,
    ) -> list[dict[str, Any]]:
        query = f"""
            SELECT source_record_id, transaction_date, fecha, hora, zona_horaria, nombre, tipo, estado, divisa,
                   bruto, tarifa, neto, sender_email, recipient_email, transaction_id, shipping_address,
                   address_status, item_name, item_id, shipping_amount, insurance_amount, sales_tax_amount,
                   option1_name, option1_value, option2_name, option2_value, reference_transaction_id,
                   invoice_number, custom_number, quantity, receipt_id, balance_amount, address_line_1,
                   address_line_2, city, region, postal_code, country, contact_phone, subject, note,
                   country_code, balance_impact, order_number, shopify_order_name, company_code, market_code, raw_payload
            FROM {SCHEMA_NAME}.paypal_transactions_raw
            WHERE 1=1
        """
        params: list[object] = []
        if date_from:
            query += " AND transaction_date >= %s::timestamptz"
            params.append(f"{date_from}T00:00:00Z")
        if date_to:
            query += " AND transaction_date <= %s::timestamptz"
            params.append(f"{date_to}T23:59:59Z")
        query += " ORDER BY transaction_date, source_record_id"
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [
            {
                "source_record_id": row[0],
                "transaction_date": row[1].isoformat().replace("+00:00", "Z") if row[1] else "",
                "fecha": row[2] or "",
                "hora": row[3] or "",
                "zona_horaria": row[4] or "",
                "nombre": row[5] or "",
                "tipo": row[6] or "",
                "estado": row[7] or "",
                "divisa": row[8] or "",
                "bruto": str(row[9]),
                "tarifa": str(row[10]),
                "neto": str(row[11]),
                "sender_email": row[12] or "",
                "recipient_email": row[13] or "",
                "transaction_id": row[14] or "",
                "shipping_address": row[15] or "",
                "address_status": row[16] or "",
                "item_name": row[17] or "",
                "item_id": row[18] or "",
                "shipping_amount": str(row[19]),
                "insurance_amount": str(row[20]),
                "sales_tax_amount": str(row[21]),
                "option1_name": row[22] or "",
                "option1_value": row[23] or "",
                "option2_name": row[24] or "",
                "option2_value": row[25] or "",
                "reference_transaction_id": row[26] or "",
                "invoice_number": row[27] or "",
                "custom_number": row[28] or "",
                "quantity": row[29] or "",
                "receipt_id": row[30] or "",
                "balance_amount": str(row[31]) if row[31] is not None else "",
                "address_line_1": row[32] or "",
                "address_line_2": row[33] or "",
                "city": row[34] or "",
                "region": row[35] or "",
                "postal_code": row[36] or "",
                "country": row[37] or "",
                "contact_phone": row[38] or "",
                "subject": row[39] or "",
                "note": row[40] or "",
                "country_code": row[41] or "",
                "balance_impact": row[42] or "",
                "order_number": row[43] or "",
                "shopify_order_name": row[44] or "",
                "company_code": row[45] or "",
                "market_code": row[46] or "",
                "raw_payload": row[47] or {},
            }
            for row in rows
        ]

    def _list_payment_fee_monthly_summary_db(
        self,
        *,
        company_code: str | None,
        platform: str | None,
        period_yyyymm: str | None,
        date_from: str | None,
        date_to: str | None,
    ) -> list[PaymentFeeSummaryRow]:
        query = f"""
            SELECT company_code, period_yyyymm, platform, market_code, currency_code, orders_count,
                   transactions_count, gross_amount, fee_amount, chargeback_amount,
                   chargeback_fee_amount, total_cost_amount, net_amount, payout_count
            FROM {SCHEMA_NAME}.payment_fee_monthly_summary
            WHERE 1=1
        """
        params: list[object] = []
        if company_code:
            query += " AND company_code = %s"
            params.append(company_code)
        if platform:
            query += " AND platform = %s"
            params.append(platform)
        if period_yyyymm:
            query += " AND period_yyyymm = %s"
            params.append(period_yyyymm)
        if date_from:
            query += " AND period_yyyymm >= %s"
            params.append(date_from[:7].replace("-", ""))
        if date_to:
            query += " AND period_yyyymm <= %s"
            params.append(date_to[:7].replace("-", ""))
        query += " ORDER BY period_yyyymm, platform, market_code"
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [
            PaymentFeeSummaryRow(
                company_code=row[0],
                period_yyyymm=row[1],
                platform=row[2],
                market_code=row[3],
                currency_code=row[4],
                orders_count=row[5],
                transactions_count=row[6],
                gross_amount=row[7],
                fee_amount=row[8],
                chargeback_amount=row[9],
                chargeback_fee_amount=row[10],
                total_cost_amount=row[11],
                net_amount=row[12],
                payout_count=row[13],
            )
            for row in rows
        ]

    def _list_suppliers_db(self, company: str | None = None) -> list[dict[str, str]]:
        query = f"""
            SELECT company_code, current_folder, supplier_name, supplier_code, destination_path, notes, sender_emails
            FROM {SCHEMA_NAME}.suppliers
            WHERE is_active = TRUE
        """
        params: list[str] = []
        if company:
            query += " AND company_code = %s"
            params.append(self._company_code(company))
        query += " ORDER BY company_code, supplier_name, supplier_code"
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [
            {
                "company": self._company_name(row[0]),
                "current_folder": row[1],
                "provider_name": row[2],
                "supplier_code": row[3],
                "destination_path": row[4],
                "notes": row[5],
                "sender_emails": json.loads(row[6] or "[]"),
            }
            for row in rows
        ]

    def _row_to_item(self, row) -> StoredReviewItem:
        return StoredReviewItem(
            id=str(row[0]),
            kind=row[1],
            status=row[2],
            company=self._company_name(row[3]),
            period_yyyymm=row[4],
            source_sender=row[5],
            source_subject=row[6],
            attachment_names=json.loads(row[7] or "[]"),
            review_path=self._parent_windows_path(row[8]),
            source_file=row[8] or "",
            suggested_provider=row[9],
            suggested_supplier_code=row[10],
            notes=row[11],
            destination_file=row[12] or row[8] or "",
            drive_file_id=row[13] or "",
            drive_view_url=row[14] or "",
        )

    def _list_review_items_db(self, status: str | None = None) -> list[StoredReviewItem]:
        query = f"""
            SELECT ri.id, ri.kind, ri.status, d.company_code, d.period_yyyymm, d.source_sender,
                   d.source_subject, jsonb_build_array(COALESCE(d.original_filename, d.attachment_original_name))::text,
                   d.windows_path, ri.suggested_supplier_name, ri.suggested_supplier_code,
                   ri.notes, d.windows_path, d.drive_file_id, d.drive_url
            FROM {SCHEMA_NAME}.review_items ri
            JOIN {SCHEMA_NAME}.documents d ON d.id = ri.document_id
        """
        params: list[str] = []
        if status:
            query += " WHERE ri.status = %s"
            params.append(status)
        query += " ORDER BY d.period_yyyymm, d.source_subject"
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row_to_item(row) for row in rows]

    def _get_review_item_db(self, review_item_id: str) -> StoredReviewItem:
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT ri.id, ri.kind, ri.status, d.company_code, d.period_yyyymm, d.source_sender,
                       d.source_subject, jsonb_build_array(COALESCE(d.original_filename, d.attachment_original_name))::text,
                       d.windows_path, ri.suggested_supplier_name, ri.suggested_supplier_code,
                       ri.notes, d.windows_path, d.drive_file_id, d.drive_url
                FROM {SCHEMA_NAME}.review_items ri
                JOIN {SCHEMA_NAME}.documents d ON d.id = ri.document_id
                WHERE ri.id = %s
                """,
                (review_item_id,),
            ).fetchone()
        if row is None:
            raise KeyError(review_item_id)
        return self._row_to_item(row)

    def _resolve_review_item_db(
        self,
        review_item_id: str,
        *,
        company: str,
        supplier_code: str,
        invoice_date: str,
        invoice_number: str,
    ) -> StoredReviewItem:
        company_code = self._company_code(company)
        provider = get_provider(self._company_name(company_code), supplier_code)
        item = self._get_review_item_db(review_item_id)
        logical_windows_path = self._build_windows_path(
            company_name=provider.company,
            supplier_code=supplier_code,
            invoice_date=invoice_date,
            invoice_number=invoice_number,
            destination_path=provider.destination_path,
            extension=Path(item.source_file).suffix.lower() or ".pdf",
        )
        destination_file = logical_windows_path
        if self.finance_root and item.source_file and Path(item.source_file).exists():
            result = apply_review_decision(
                ReviewDecision(
                    root=self.finance_root,
                    source_file=Path(item.source_file),
                    company=company,
                    supplier_code=supplier_code,
                    invoice_date=invoice_date,
                    invoice_number=invoice_number,
                )
            )
            destination_file = str(result.destination_file)
        with self._connect() as conn:
            document_row = conn.execute(
                f"SELECT document_id FROM {SCHEMA_NAME}.review_items WHERE id = %s",
                (review_item_id,),
            ).fetchone()
            if document_row is None:
                raise KeyError(review_item_id)
            supplier_id = self._find_supplier_id(conn, company_code, supplier_code)
            conn.execute(
                f"""
                UPDATE {SCHEMA_NAME}.documents
                SET invoice_number = %s,
                    invoice_date = %s,
                    issuer_company_name = %s,
                    billed_company_name = %s,
                    supplier_name = %s,
                    company_code = %s,
                    windows_path = %s,
                    supplier_id = %s,
                    supplier_code = %s,
                    status = 'resolved',
                    review_notes = COALESCE(review_notes, ''),
                    period_yyyymm = TO_CHAR(%s::date, 'YYYYMM'),
                    updated_at = NOW()
                WHERE id = %s
                """,
                (
                    invoice_number,
                    invoice_date,
                    provider.provider_name,
                    self._company_name(company_code),
                    provider.provider_name,
                    company_code,
                    destination_file,
                    supplier_id,
                    supplier_code,
                    invoice_date,
                    document_row[0],
                ),
            )
            conn.execute(
                f"""
                UPDATE {SCHEMA_NAME}.review_items
                SET status = 'resolved',
                    suggested_supplier_code = %s,
                    suggested_supplier_name = %s,
                    resolved_at = NOW(),
                    resolved_by = 'manual',
                    updated_at = NOW()
                WHERE id = %s
                """,
                (supplier_code, provider.provider_name, review_item_id),
            )
            conn.commit()
        return self._get_review_item_db(review_item_id)

    def _find_supplier_id(self, conn, company_code: str, supplier_code: str) -> str | None:
        row = conn.execute(
            f"SELECT id FROM {SCHEMA_NAME}.suppliers WHERE company_code = %s AND supplier_code = %s",
            (company_code, supplier_code),
        ).fetchone()
        return str(row[0]) if row else None

    def _count_rows(self, conn, relation: str) -> int:
        return int(conn.execute(f"SELECT COUNT(*) FROM {relation}").fetchone()[0])

    def _provider_record_to_dict(self, record: ProviderRecord) -> dict[str, str]:
        return {
            "company": record.company,
            "current_folder": record.current_folder,
            "provider_name": record.provider_name,
            "supplier_code": record.supplier_code,
            "destination_path": record.destination_path,
            "notes": record.notes,
            "sender_emails": list(record.sender_emails),
        }

    def _row_to_ingestion_queue_item(self, row) -> IngestionQueueItem:
        return IngestionQueueItem(
            id=str(row[0]),
            source=str(row[1]),
            gmail_message_id=str(row[2] or ""),
            gmail_attachment_id=str(row[3] or ""),
            original_filename=str(row[4] or ""),
            stored_filename=str(row[5] or ""),
            sender_email=str(row[6] or ""),
            subject=str(row[7] or ""),
            received_at=row[8],
            drive_file_id=str(row[9] or ""),
            drive_url=str(row[10] or ""),
            validation_bucket=str(row[11] or ""),
            detected_supplier_code=str(row[12] or ""),
            detected_company_code=str(row[13] or ""),
            parser_name=str(row[14] or ""),
            parse_status=str(row[15] or ""),
            parse_error=str(row[16] or ""),
            document_id=str(row[17] or ""),
            mime_type=str(row[18] or ""),
            heuristic_reason=str(row[19] or ""),
            created_at=row[20],
            updated_at=row[21],
        )

    def _company_code(self, company: str) -> str:
        normalized = company.strip()
        if normalized.upper() in COMPANY_NAMES:
            return normalized.upper()
        if normalized in ("Ltd", "Inc", "SL"):
            return normalized.upper()
        return COMPANY_CODES.get(normalized, COMPANY_CODES.get(normalized.upper(), normalized.upper()))

    def _company_name(self, company_code: str) -> str:
        normalized_code = company_code.strip().upper()
        return COMPANY_NAMES.get(normalized_code, company_code)

    def _compose_legacy_source_file(self, review_path: str, source_subject: str) -> str:
        if not review_path:
            return source_subject or ""
        if not source_subject:
            return review_path
        return f"{review_path}\\{source_subject}"

    def _parent_windows_path(self, windows_path: str) -> str:
        return ntpath.dirname(windows_path) if windows_path else ""

    def _build_windows_path(
        self,
        *,
        company_name: str,
        supplier_code: str,
        invoice_date: str,
        invoice_number: str,
        destination_path: str,
        extension: str,
    ) -> str:
        invoice_dt = datetime.strptime(invoice_date, "%Y-%m-%d")
        filename = f"{supplier_code}_{invoice_dt.strftime('%Y%m%d')}_{invoice_number}{extension}"
        parts = [
            "ARTESTA - 6. Finances",
            company_folder_name(company_name),
            invoice_dt.strftime("%Y"),
            invoice_dt.strftime("%Y%m"),
            *Path(destination_path).parts,
            filename,
        ]
        return "\\".join(parts)

    def _suppliers_table_sql(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.suppliers (
                id UUID PRIMARY KEY,
                company_code TEXT NOT NULL,
                current_folder TEXT NOT NULL DEFAULT '',
                supplier_code TEXT NOT NULL,
                supplier_name TEXT NOT NULL,
                billing_company_name TEXT NOT NULL DEFAULT '',
                destination_path TEXT NOT NULL DEFAULT '',
                sender_emails TEXT NOT NULL DEFAULT '[]',
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                notes TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (company_code, supplier_code)
            )
        """

    def _documents_table_sql(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.documents (
                id UUID PRIMARY KEY,
                invoice_number TEXT NOT NULL DEFAULT '',
                invoice_date DATE NULL,
                issuer_company_name TEXT NOT NULL DEFAULT '',
                billed_company_name TEXT NOT NULL DEFAULT '',
                supplier_name TEXT NOT NULL DEFAULT '',
                company_code TEXT NOT NULL,
                windows_path TEXT NOT NULL DEFAULT '',
                drive_url TEXT NOT NULL DEFAULT '',
                received_at TIMESTAMPTZ NULL,
                sender_email TEXT NOT NULL DEFAULT '',
                original_filename TEXT NOT NULL DEFAULT '',
                division_invoice TEXT NOT NULL DEFAULT '',
                billing_period_start DATE NULL,
                billing_period_end DATE NULL,
                vat_percent NUMERIC(8,4) NULL,
                gross_amount NUMERIC(14,2) NULL,
                vat_amount NUMERIC(14,2) NULL,
                net_amount NUMERIC(14,2) NULL,
                supplier_id UUID NULL REFERENCES {SCHEMA_NAME}.suppliers(id),
                supplier_code TEXT NOT NULL DEFAULT '',
                currency_code TEXT NOT NULL DEFAULT '',
                drive_file_id TEXT NOT NULL DEFAULT '',
                storage_root TEXT NOT NULL DEFAULT 'GOOGLE_DRIVE',
                document_type TEXT NOT NULL DEFAULT 'invoice',
                status TEXT NOT NULL DEFAULT 'pending_review',
                source_channel TEXT NOT NULL DEFAULT 'manual',
                email_message_id TEXT NOT NULL DEFAULT '',
                email_thread_id TEXT NOT NULL DEFAULT '',
                attachment_original_name TEXT NOT NULL DEFAULT '',
                parser_name TEXT NOT NULL DEFAULT '',
                parser_confidence NUMERIC(5,4) NULL,
                extracted_raw JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                review_notes TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                source_sender TEXT NOT NULL DEFAULT '',
                source_subject TEXT NOT NULL DEFAULT '',
                period_yyyymm TEXT NOT NULL DEFAULT ''
            )
        """

    def _review_items_table_sql(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.review_items (
                id UUID PRIMARY KEY,
                document_id UUID NOT NULL REFERENCES {SCHEMA_NAME}.documents(id) ON DELETE CASCADE,
                kind TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                suggested_supplier_code TEXT NOT NULL DEFAULT '',
                suggested_supplier_name TEXT NOT NULL DEFAULT '',
                notes TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                resolved_at TIMESTAMPTZ NULL,
                resolved_by TEXT NOT NULL DEFAULT ''
            )
        """

    def _payment_order_transactions_table_sql(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.payment_order_transactions (
                id UUID PRIMARY KEY,
                platform TEXT NOT NULL,
                company_code TEXT NOT NULL,
                market_code TEXT NOT NULL DEFAULT '',
                currency_code TEXT NOT NULL DEFAULT '',
                order_id TEXT NOT NULL DEFAULT '',
                order_name TEXT NOT NULL DEFAULT '',
                external_transaction_id TEXT NOT NULL,
                external_payout_id TEXT NOT NULL DEFAULT '',
                transaction_date TIMESTAMPTZ NOT NULL,
                payout_date TIMESTAMPTZ NULL,
                transaction_type TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL DEFAULT '',
                gross_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
                fee_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
                net_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
                chargeback_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
                chargeback_fee_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
                affects_balance BOOLEAN NOT NULL DEFAULT TRUE,
                is_cancelled BOOLEAN NOT NULL DEFAULT FALSE,
                is_chargeback BOOLEAN NOT NULL DEFAULT FALSE,
                payment_reference TEXT NOT NULL DEFAULT '',
                customer_reference TEXT NOT NULL DEFAULT '',
                raw_payload JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                period_yyyymm TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """

    def _shopify_payout_transactions_table_sql(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.shopify_payout_transactions (
                id UUID PRIMARY KEY,
                source_record_id TEXT NOT NULL,
                transaction_date TIMESTAMPTZ NOT NULL,
                type TEXT NOT NULL DEFAULT '',
                order_id TEXT NOT NULL DEFAULT '',
                order_name TEXT NOT NULL DEFAULT '',
                card_brand TEXT NOT NULL DEFAULT '',
                card_source TEXT NOT NULL DEFAULT '',
                payout_status TEXT NOT NULL DEFAULT '',
                payout_date TIMESTAMPTZ NULL,
                payout_id TEXT NOT NULL DEFAULT '',
                available_on TIMESTAMPTZ NULL,
                amount NUMERIC(14,2) NOT NULL DEFAULT 0,
                fee NUMERIC(14,2) NOT NULL DEFAULT 0,
                net NUMERIC(14,2) NOT NULL DEFAULT 0,
                checkout TEXT NOT NULL DEFAULT '',
                payment_method_name TEXT NOT NULL DEFAULT '',
                presentment_amount NUMERIC(14,2) NULL,
                presentment_currency TEXT NOT NULL DEFAULT '',
                currency TEXT NOT NULL DEFAULT '',
                vat NUMERIC(14,2) NOT NULL DEFAULT 0,
                business_entity_name TEXT NOT NULL DEFAULT '',
                business_entity_id TEXT NOT NULL DEFAULT '',
                company_code TEXT NOT NULL DEFAULT '',
                market_code TEXT NOT NULL DEFAULT '',
                raw_payload JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """

    def _paypal_transactions_raw_table_sql(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.paypal_transactions_raw (
                id UUID PRIMARY KEY,
                source_record_id TEXT NOT NULL,
                transaction_date TIMESTAMPTZ NOT NULL,
                fecha TEXT NOT NULL DEFAULT '',
                hora TEXT NOT NULL DEFAULT '',
                zona_horaria TEXT NOT NULL DEFAULT '',
                nombre TEXT NOT NULL DEFAULT '',
                tipo TEXT NOT NULL DEFAULT '',
                estado TEXT NOT NULL DEFAULT '',
                divisa TEXT NOT NULL DEFAULT '',
                bruto NUMERIC(14,2) NOT NULL DEFAULT 0,
                tarifa NUMERIC(14,2) NOT NULL DEFAULT 0,
                neto NUMERIC(14,2) NOT NULL DEFAULT 0,
                sender_email TEXT NOT NULL DEFAULT '',
                recipient_email TEXT NOT NULL DEFAULT '',
                transaction_id TEXT NOT NULL DEFAULT '',
                shipping_address TEXT NOT NULL DEFAULT '',
                address_status TEXT NOT NULL DEFAULT '',
                item_name TEXT NOT NULL DEFAULT '',
                item_id TEXT NOT NULL DEFAULT '',
                shipping_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
                insurance_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
                sales_tax_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
                option1_name TEXT NOT NULL DEFAULT '',
                option1_value TEXT NOT NULL DEFAULT '',
                option2_name TEXT NOT NULL DEFAULT '',
                option2_value TEXT NOT NULL DEFAULT '',
                reference_transaction_id TEXT NOT NULL DEFAULT '',
                invoice_number TEXT NOT NULL DEFAULT '',
                custom_number TEXT NOT NULL DEFAULT '',
                quantity TEXT NOT NULL DEFAULT '',
                receipt_id TEXT NOT NULL DEFAULT '',
                balance_amount NUMERIC(14,2) NULL,
                address_line_1 TEXT NOT NULL DEFAULT '',
                address_line_2 TEXT NOT NULL DEFAULT '',
                city TEXT NOT NULL DEFAULT '',
                region TEXT NOT NULL DEFAULT '',
                postal_code TEXT NOT NULL DEFAULT '',
                country TEXT NOT NULL DEFAULT '',
                contact_phone TEXT NOT NULL DEFAULT '',
                subject TEXT NOT NULL DEFAULT '',
                note TEXT NOT NULL DEFAULT '',
                country_code TEXT NOT NULL DEFAULT '',
                balance_impact TEXT NOT NULL DEFAULT '',
                order_number TEXT NOT NULL DEFAULT '',
                shopify_order_name TEXT NOT NULL DEFAULT '',
                company_code TEXT NOT NULL DEFAULT '',
                market_code TEXT NOT NULL DEFAULT '',
                raw_payload JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """

    def _payment_fee_monthly_summary_table_sql(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.payment_fee_monthly_summary (
                id UUID PRIMARY KEY,
                company_code TEXT NOT NULL,
                period_yyyymm TEXT NOT NULL,
                platform TEXT NOT NULL,
                market_code TEXT NOT NULL DEFAULT '',
                currency_code TEXT NOT NULL DEFAULT '',
                orders_count INTEGER NOT NULL DEFAULT 0,
                transactions_count INTEGER NOT NULL DEFAULT 0,
                gross_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
                fee_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
                chargeback_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
                chargeback_fee_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
                total_cost_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
                net_amount NUMERIC(14,2) NOT NULL DEFAULT 0,
                payout_count INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """

    def _payroll_documents_table_sql(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.payroll_documents (
                id UUID PRIMARY KEY,
                company_code TEXT NOT NULL,
                provider_code TEXT NOT NULL DEFAULT '',
                provider_name TEXT NOT NULL DEFAULT '',
                source_channel TEXT NOT NULL DEFAULT 'manual',
                source_sender TEXT NOT NULL DEFAULT '',
                source_subject TEXT NOT NULL DEFAULT '',
                original_filename TEXT NOT NULL DEFAULT '',
                document_type TEXT NOT NULL DEFAULT 'payroll',
                payroll_period_start DATE NULL,
                payroll_period_end DATE NULL,
                period_yyyymm TEXT NOT NULL DEFAULT '',
                employee_count INTEGER NULL,
                gross_pay_amount NUMERIC(14,2) NULL,
                employee_deductions_amount NUMERIC(14,2) NULL,
                net_pay_amount NUMERIC(14,2) NULL,
                employer_social_security_amount NUMERIC(14,2) NULL,
                total_company_cost_amount NUMERIC(14,2) NULL,
                social_security_liquidation_amount NUMERIC(14,2) NULL,
                tax_withholdings_amount NUMERIC(14,2) NULL,
                currency_code TEXT NOT NULL DEFAULT 'EUR',
                windows_path TEXT NOT NULL DEFAULT '',
                drive_file_id TEXT NOT NULL DEFAULT '',
                drive_url TEXT NOT NULL DEFAULT '',
                stored_filename TEXT NOT NULL DEFAULT '',
                email_message_id TEXT NOT NULL DEFAULT '',
                extracted_raw JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                review_notes TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """

    def _expected_invoices_table_sql(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.expected_invoices (
                id UUID PRIMARY KEY,
                company_code TEXT NOT NULL,
                supplier_id UUID NULL REFERENCES {SCHEMA_NAME}.suppliers(id),
                supplier_code TEXT NOT NULL DEFAULT '',
                expected_name TEXT NOT NULL DEFAULT '',
                recurrence TEXT NOT NULL DEFAULT '',
                day_of_month SMALLINT NULL,
                day_tolerance SMALLINT NOT NULL DEFAULT 0,
                active_from DATE NOT NULL DEFAULT CURRENT_DATE,
                active_to DATE NULL,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                notes TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """

    def _expected_invoice_runs_table_sql(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.expected_invoice_runs (
                id UUID PRIMARY KEY,
                expected_invoice_id UUID NOT NULL REFERENCES {SCHEMA_NAME}.expected_invoices(id) ON DELETE CASCADE,
                period_yyyymm TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                matched_document_id UUID NULL REFERENCES {SCHEMA_NAME}.documents(id),
                checked_at TIMESTAMPTZ NULL,
                notes TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """

    def _mail_sync_state_table_sql(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.mail_sync_state (
                id UUID PRIMARY KEY,
                mailbox TEXT NOT NULL,
                sync_name TEXT NOT NULL,
                last_processed_at TIMESTAMPTZ NULL,
                last_processed_message_id TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (mailbox, sync_name)
            )
        """

    def _ingestion_queue_table_sql(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.ingestion_queue (
                id UUID PRIMARY KEY,
                source TEXT NOT NULL DEFAULT '',
                gmail_message_id TEXT NOT NULL DEFAULT '',
                gmail_attachment_id TEXT NOT NULL DEFAULT '',
                original_filename TEXT NOT NULL DEFAULT '',
                stored_filename TEXT NOT NULL DEFAULT '',
                sender_email TEXT NOT NULL DEFAULT '',
                subject TEXT NOT NULL DEFAULT '',
                received_at TIMESTAMPTZ NULL,
                drive_file_id TEXT NOT NULL DEFAULT '',
                drive_url TEXT NOT NULL DEFAULT '',
                validation_bucket TEXT NOT NULL DEFAULT '',
                detected_supplier_code TEXT NOT NULL DEFAULT '',
                detected_company_code TEXT NOT NULL DEFAULT '',
                parser_name TEXT NOT NULL DEFAULT '',
                parse_status TEXT NOT NULL DEFAULT '',
                parse_error TEXT NOT NULL DEFAULT '',
                document_id UUID NULL REFERENCES {SCHEMA_NAME}.documents(id) ON DELETE SET NULL,
                mime_type TEXT NOT NULL DEFAULT '',
                heuristic_reason TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """

    def _artist_royalties_documents_table_sql(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.artist_royalties_documents (
                id UUID PRIMARY KEY,
                company_code TEXT NOT NULL,
                supplier_code TEXT NOT NULL DEFAULT 'ROYALTIES',
                supplier_name TEXT NOT NULL DEFAULT 'ARTIST ROYALTIES',
                invoice_number TEXT NOT NULL DEFAULT '',
                credit_note_number TEXT NOT NULL DEFAULT '',
                invoice_date DATE NULL,
                billing_period_start DATE NULL,
                billing_period_end DATE NULL,
                period_yyyymm TEXT NOT NULL DEFAULT '',
                artist_name TEXT NOT NULL DEFAULT '',
                artist_tax_id TEXT NOT NULL DEFAULT '',
                artist_email TEXT NOT NULL DEFAULT '',
                artist_country TEXT NOT NULL DEFAULT '',
                artist_region_code TEXT NOT NULL DEFAULT '',
                payment_method TEXT NOT NULL DEFAULT '',
                gross_amount NUMERIC(14,2) NULL,
                withholding_percent NUMERIC(8,4) NULL,
                withholding_amount NUMERIC(14,2) NULL,
                net_amount NUMERIC(14,2) NULL,
                currency_code TEXT NOT NULL DEFAULT 'EUR',
                windows_path TEXT NOT NULL DEFAULT '',
                drive_file_id TEXT NOT NULL DEFAULT '',
                drive_url TEXT NOT NULL DEFAULT '',
                original_filename TEXT NOT NULL DEFAULT '',
                source_channel TEXT NOT NULL DEFAULT 'manual',
                parser_name TEXT NOT NULL DEFAULT '',
                parser_confidence NUMERIC(5,4) NULL,
                extracted_raw JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                review_notes TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """

    def _artist_royalties_monthly_summary_table_sql(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.artist_royalties_monthly_summary (
                id UUID PRIMARY KEY,
                company_code TEXT NOT NULL,
                supplier_code TEXT NOT NULL DEFAULT 'ROYALTIES',
                summary_scope TEXT NOT NULL,
                period_yyyymm TEXT NOT NULL,
                posters_amount NUMERIC(14,2) NULL,
                stationery_amount NUMERIC(14,2) NULL,
                gross_amount NUMERIC(14,2) NULL,
                withholding_amount NUMERIC(14,2) NULL,
                withholding_percent NUMERIC(8,4) NULL,
                net_amount NUMERIC(14,2) NULL,
                paypal_amount NUMERIC(14,2) NULL,
                bank_transfer_amount NUMERIC(14,2) NULL,
                one_x_amount NUMERIC(14,2) NULL,
                source_filename TEXT NOT NULL DEFAULT '',
                windows_path TEXT NOT NULL DEFAULT '',
                drive_file_id TEXT NOT NULL DEFAULT '',
                drive_url TEXT NOT NULL DEFAULT '',
                extracted_raw JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """

    def _documents_unique_index_sql(self) -> str:
        return f"""
            CREATE UNIQUE INDEX IF NOT EXISTS invoices_documents_unique_invoice_idx
            ON {SCHEMA_NAME}.documents (company_code, supplier_code, invoice_number, division_invoice)
            WHERE invoice_number <> ''
        """

    def _review_items_status_index_sql(self) -> str:
        return f"""
            CREATE INDEX IF NOT EXISTS invoices_review_items_status_idx
            ON {SCHEMA_NAME}.review_items (status, kind)
        """

    def _documents_period_index_sql(self) -> str:
        return f"""
            CREATE INDEX IF NOT EXISTS invoices_documents_company_period_idx
            ON {SCHEMA_NAME}.documents (company_code, period_yyyymm)
        """

    def _shopify_payout_transactions_unique_index_sql(self) -> str:
        return f"""
            CREATE UNIQUE INDEX IF NOT EXISTS invoices_shopify_payout_transactions_unique_idx
            ON {SCHEMA_NAME}.shopify_payout_transactions (source_record_id)
        """

    def _shopify_payout_transactions_period_index_sql(self) -> str:
        return f"""
            CREATE INDEX IF NOT EXISTS invoices_shopify_payout_transactions_period_idx
            ON {SCHEMA_NAME}.shopify_payout_transactions (company_code, transaction_date)
        """

    def _paypal_transactions_raw_unique_index_sql(self) -> str:
        return f"""
            CREATE UNIQUE INDEX IF NOT EXISTS invoices_paypal_transactions_raw_unique_idx
            ON {SCHEMA_NAME}.paypal_transactions_raw (source_record_id)
        """

    def _paypal_transactions_raw_period_index_sql(self) -> str:
        return f"""
            CREATE INDEX IF NOT EXISTS invoices_paypal_transactions_raw_period_idx
            ON {SCHEMA_NAME}.paypal_transactions_raw (company_code, transaction_date)
        """

    def _payment_order_transactions_unique_index_sql(self) -> str:
        return f"""
            CREATE UNIQUE INDEX IF NOT EXISTS invoices_payment_order_transactions_unique_idx
            ON {SCHEMA_NAME}.payment_order_transactions (platform, external_transaction_id)
        """

    def _payment_order_transactions_period_index_sql(self) -> str:
        return f"""
            CREATE INDEX IF NOT EXISTS invoices_payment_order_transactions_period_idx
            ON {SCHEMA_NAME}.payment_order_transactions (company_code, platform, period_yyyymm)
        """

    def _payment_fee_monthly_summary_unique_index_sql(self) -> str:
        return f"""
            CREATE UNIQUE INDEX IF NOT EXISTS invoices_payment_fee_monthly_summary_unique_idx
            ON {SCHEMA_NAME}.payment_fee_monthly_summary (company_code, period_yyyymm, platform, market_code, currency_code)
        """

    def _otros_gastos_table_sql(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.otros_gastos (
                id UUID PRIMARY KEY,
                company_code TEXT NOT NULL,
                period_yyyymm TEXT NOT NULL,
                amount_eur NUMERIC(14,2) NOT NULL DEFAULT 0,
                notes TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """

    def upsert_otros_gastos(self, *, company_code: str, period_yyyymm: str, amount_eur: float, notes: str = "") -> dict[str, Any]:
        if not self.database_url:
            raise RuntimeError("otros_gastos requires DATABASE_URL")
        import uuid as _uuid
        with self._connect() as conn:
            row = conn.execute(
                f"""
                INSERT INTO {SCHEMA_NAME}.otros_gastos (id, company_code, period_yyyymm, amount_eur, notes, updated_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (company_code, period_yyyymm)
                DO UPDATE SET amount_eur = EXCLUDED.amount_eur, notes = EXCLUDED.notes, updated_at = NOW()
                RETURNING id, company_code, period_yyyymm, amount_eur::float, notes
                """,
                (str(_uuid.uuid4()), company_code, period_yyyymm, amount_eur, notes),
            ).fetchone()
            return {
                "id": str(row[0]),
                "company_code": str(row[1]),
                "period_yyyymm": str(row[2]),
                "amount_eur": float(row[3]),
                "notes": str(row[4] or ""),
            }

    # ------------------------------------------------------------------
    # Frame purchases (supply.frame_purchases / supply.frame_purchase_lines)
    # ------------------------------------------------------------------

    def insert_frame_purchase(
        self,
        *,
        fabricante: str,
        purchase_date: str,  # ISO date string "YYYY-MM-DD"
        currency: str,
        notes: str = "",
        lines: list[dict],  # [{frame_color, frame_size, quantity, unit_price}]
    ) -> dict[str, Any]:
        """Insert a frame purchase order with its line items. Returns the created purchase."""
        if not self.database_url:
            raise RuntimeError("frame_purchases requires DATABASE_URL")
        with self._connect() as conn:
            row = conn.execute(
                """
                INSERT INTO supply.frame_purchases (fabricante, purchase_date, currency, notes)
                VALUES (%s, %s::date, %s, %s)
                RETURNING id, fabricante, purchase_date::text, currency, notes, created_at::text
                """,
                (fabricante, purchase_date, currency, notes),
            ).fetchone()
            purchase_id = row[0]
            for line in lines:
                conn.execute(
                    """
                    INSERT INTO supply.frame_purchase_lines
                        (purchase_id, frame_color, frame_size, quantity, unit_price)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (purchase_id, line["frame_color"], line["frame_size"],
                     int(line["quantity"]), str(line["unit_price"])),
                )
            return {
                "id": row[0],
                "fabricante": row[1],
                "purchase_date": row[2],
                "currency": row[3],
                "notes": row[4] or "",
                "created_at": row[5],
                "lines": lines,
            }

    def get_frame_purchases(self, fabricante: str | None = None) -> list[dict[str, Any]]:
        """Return all frame purchases with their lines, optionally filtered by fabricante."""
        if not self.database_url:
            return []
        with self._connect() as conn:
            if fabricante:
                rows = conn.execute(
                    """
                    SELECT fp.id, fp.fabricante, fp.purchase_date::text, fp.currency,
                           fp.notes, fp.created_at::text,
                           fpl.frame_color, fpl.frame_size, fpl.quantity, fpl.unit_price::text
                    FROM supply.frame_purchases fp
                    JOIN supply.frame_purchase_lines fpl ON fpl.purchase_id = fp.id
                    WHERE fp.fabricante = %s
                    ORDER BY fp.purchase_date DESC, fp.id, fpl.frame_color, fpl.frame_size
                    """,
                    (fabricante,),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT fp.id, fp.fabricante, fp.purchase_date::text, fp.currency,
                           fp.notes, fp.created_at::text,
                           fpl.frame_color, fpl.frame_size, fpl.quantity, fpl.unit_price::text
                    FROM supply.frame_purchases fp
                    JOIN supply.frame_purchase_lines fpl ON fpl.purchase_id = fp.id
                    ORDER BY fp.purchase_date DESC, fp.id, fpl.frame_color, fpl.frame_size
                    """,
                ).fetchall()
        # Group lines by purchase
        purchases: dict[int, dict] = {}
        for row in rows:
            pid = row[0]
            if pid not in purchases:
                purchases[pid] = {
                    "id": pid,
                    "fabricante": row[1],
                    "purchase_date": row[2],
                    "currency": row[3],
                    "notes": row[4] or "",
                    "created_at": row[5],
                    "lines": [],
                }
            purchases[pid]["lines"].append({
                "frame_color": row[6],
                "frame_size": row[7],
                "quantity": row[8],
                "unit_price": row[9],
            })
        return list(purchases.values())

    def get_frame_consumption_daily(
        self,
        *,
        fabricante: str,
        date_from: str,  # ISO date "YYYY-MM-DD"
        date_to: str,    # ISO date "YYYY-MM-DD"
    ) -> list[dict[str, Any]]:
        """Return daily frame consumption for a fabricante in a date range."""
        if not self.database_url:
            return []
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT fecha_ddmmaaaa::text, frame_color, frame_size,
                       COALESCE(SUM(quantity), 0) AS quantity
                FROM supply.consumo_marcos_diario
                WHERE fabricante = %s
                  AND fecha_ddmmaaaa BETWEEN %s::date AND %s::date
                GROUP BY fecha_ddmmaaaa, frame_color, frame_size
                ORDER BY fecha_ddmmaaaa, frame_color, frame_size
                """,
                (fabricante, date_from, date_to),
            ).fetchall()
        return [
            {"fecha_ddmmaaaa": r[0], "frame_color": r[1], "frame_size": r[2], "quantity": r[3]}
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Frame stock – materialized tables (v2)
    # ------------------------------------------------------------------

    def refresh_frame_consumption(
        self,
        *,
        fabricante: str,
        months: list[str],  # list of yyyymm strings
    ) -> None:
        """Refresh frame_consumption_valued + frame_stock_monthly for given months."""
        if not self.database_url or not months:
            return
        from lector_facturas.supply_stock import refresh_frame_consumption_month
        from psycopg.rows import dict_row
        with self._connect() as conn:
            for mes_yyyymm in sorted(months):
                refresh_frame_consumption_month(fabricante, mes_yyyymm, conn)
            conn.commit()

    def set_frame_consumption_override(
        self,
        *,
        fabricante: str,
        mes_yyyymm: str,
        frame_color: str,
        frame_size: str,
        quantity_override: int,
        notes: str = "",
    ) -> None:
        """Set a manual quantity override for a SKU+month, then refresh that month."""
        if not self.database_url:
            return
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO supply.frame_consumption_valued
                    (fabricante, mes_yyyymm, frame_color, frame_size,
                     quantity_system, quantity_effective, unit_wac_opening,
                     wac_calculated_at, amount_system, amount_effective,
                     quantity_override, override_notes, override_set_at, updated_at)
                VALUES (%s, %s, %s, %s, 0, %s, 0, NOW(), 0, 0, %s, %s, NOW(), NOW())
                ON CONFLICT (fabricante, mes_yyyymm, frame_color, frame_size) DO UPDATE SET
                    quantity_override = EXCLUDED.quantity_override,
                    override_notes    = EXCLUDED.override_notes,
                    override_set_at   = NOW(),
                    quantity_effective = EXCLUDED.quantity_override,
                    updated_at        = NOW()
                """,
                (fabricante, mes_yyyymm, frame_color, frame_size,
                 quantity_override, quantity_override, notes),
            )
            conn.commit()
        # Re-run refresh to recompute amounts with the new override
        self.refresh_frame_consumption(fabricante=fabricante, months=[mes_yyyymm])

    def get_frame_sku_wac(
        self,
        *,
        fabricante: str,
        frame_color: str,
        frame_size: str,
    ) -> list[dict[str, Any]]:
        """Return WAC history for a single SKU, newest first."""
        if not self.database_url:
            return []
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT effective_from::text, wac, units_on_hand, purchase_id,
                       created_at::text
                FROM supply.frame_sku_wac
                WHERE fabricante = %s AND frame_color = %s AND frame_size = %s
                ORDER BY effective_from DESC, id DESC
                """,
                (fabricante, frame_color, frame_size),
            ).fetchall()
        return [
            {
                "effective_from": r[0],
                "wac": r[1],
                "units_on_hand": r[2],
                "purchase_id": r[3],
                "created_at": r[4],
            }
            for r in rows
        ]

    def get_frame_stock_monthly(
        self,
        *,
        fabricante: str,
        mes_yyyymm: str,
    ) -> dict[str, Any] | None:
        """Return a single row from frame_stock_monthly, or None if not found."""
        if not self.database_url:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT fabricante, mes_yyyymm, currency,
                       opening_units, opening_value,
                       purchased_units, purchased_value,
                       consumed_units, consumed_value,
                       closing_units, closing_value,
                       calculated_at::text
                FROM supply.frame_stock_monthly
                WHERE fabricante = %s AND mes_yyyymm = %s
                """,
                (fabricante, mes_yyyymm),
            ).fetchone()
        if row is None:
            return None
        return {
            "fabricante": row[0], "mes_yyyymm": row[1], "currency": row[2],
            "opening_units": row[3], "opening_value": row[4],
            "purchased_units": row[5], "purchased_value": row[6],
            "consumed_units": row[7], "consumed_value": row[8],
            "closing_units": row[9], "closing_value": row[10],
            "calculated_at": row[11],
        }

    def _payroll_documents_unique_index_sql(self) -> str:
        return f"""
            CREATE UNIQUE INDEX IF NOT EXISTS invoices_payroll_documents_unique_idx
            ON {SCHEMA_NAME}.payroll_documents (company_code, period_yyyymm, original_filename)
        """

    def _otros_gastos_unique_index_sql(self) -> str:
        return f"""
            CREATE UNIQUE INDEX IF NOT EXISTS invoices_otros_gastos_unique_idx
            ON {SCHEMA_NAME}.otros_gastos (company_code, period_yyyymm)
        """

    def _otros_ingresos_table_sql(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.otros_ingresos (
                id UUID PRIMARY KEY,
                company_code TEXT NOT NULL,
                period_yyyymm TEXT NOT NULL,
                amount_eur NUMERIC(14,2) NOT NULL DEFAULT 0,
                notes TEXT NOT NULL DEFAULT '',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """

    def _otros_ingresos_unique_index_sql(self) -> str:
        return f"""
            CREATE UNIQUE INDEX IF NOT EXISTS invoices_otros_ingresos_unique_idx
            ON {SCHEMA_NAME}.otros_ingresos (company_code, period_yyyymm)
        """

    def upsert_otros_ingresos(self, *, company_code: str, period_yyyymm: str, amount_eur: float, notes: str = "") -> dict[str, Any]:
        if not self.database_url:
            raise RuntimeError("otros_ingresos requires DATABASE_URL")
        import uuid as _uuid
        with self._connect() as conn:
            row = conn.execute(
                f"""
                INSERT INTO {SCHEMA_NAME}.otros_ingresos (id, company_code, period_yyyymm, amount_eur, notes, updated_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (company_code, period_yyyymm)
                DO UPDATE SET amount_eur = EXCLUDED.amount_eur, notes = EXCLUDED.notes, updated_at = NOW()
                RETURNING id, company_code, period_yyyymm, amount_eur::float, notes
                """,
                (str(_uuid.uuid4()), company_code, period_yyyymm, amount_eur, notes),
            ).fetchone()
            return {
                "id": str(row[0]),
                "company_code": str(row[1]),
                "period_yyyymm": str(row[2]),
                "amount_eur": float(row[3]),
                "notes": str(row[4] or ""),
            }

    def _artist_royalties_documents_unique_index_sql(self) -> str:
        return f"""
            CREATE UNIQUE INDEX IF NOT EXISTS invoices_artist_royalties_documents_unique_idx
            ON {SCHEMA_NAME}.artist_royalties_documents (company_code, period_yyyymm, invoice_number)
            WHERE invoice_number <> ''
        """

    def _artist_royalties_documents_period_index_sql(self) -> str:
        return f"""
            CREATE INDEX IF NOT EXISTS invoices_artist_royalties_documents_period_idx
            ON {SCHEMA_NAME}.artist_royalties_documents (company_code, period_yyyymm)
        """

    def _artist_royalties_summary_unique_index_sql(self) -> str:
        return f"""
            CREATE UNIQUE INDEX IF NOT EXISTS invoices_artist_royalties_monthly_summary_unique_idx
            ON {SCHEMA_NAME}.artist_royalties_monthly_summary (company_code, period_yyyymm, summary_scope)
        """

    def _documents_exact_email_duplicate_index_sql(self) -> str:
        return f"""
            CREATE UNIQUE INDEX IF NOT EXISTS invoices_documents_exact_email_duplicate_idx
            ON {SCHEMA_NAME}.documents (email_message_id, original_filename)
            WHERE email_message_id <> '' AND original_filename <> ''
        """

    def _ingestion_queue_bucket_index_sql(self) -> str:
        return f"""
            CREATE INDEX IF NOT EXISTS invoices_ingestion_queue_bucket_idx
            ON {SCHEMA_NAME}.ingestion_queue (validation_bucket, received_at)
        """

    def _ingestion_queue_source_index_sql(self) -> str:
        return f"""
            CREATE INDEX IF NOT EXISTS invoices_ingestion_queue_source_idx
            ON {SCHEMA_NAME}.ingestion_queue (gmail_message_id, gmail_attachment_id, original_filename)
        """
