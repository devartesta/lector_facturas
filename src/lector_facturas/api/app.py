from __future__ import annotations

import os
import uuid
from functools import lru_cache
from datetime import UTC, datetime
from zoneinfo import ZoneInfo
from pathlib import Path

from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, Request, Response, UploadFile

from lector_facturas.api.schemas import (
    CompanyOut,
    DailyRunOut,
    DriveBootstrapIn,
    DriveBootstrapOut,
    FramePurchaseIn,
    FramePurchaseOut,
    FrameStockSummaryOut,
    GoogleDriveStatusOut,
    IngestionQueueItemOut,
    MailSyncRunIn,
    MailSyncRunOut,
    MailSyncStateOut,
    PaymentFeeSummaryOut,
    PaymentFeeSyncIn,
    PaymentFeeSyncOut,
    PaymentOrderTransactionOut,
    PygConsolidatedSyncIn,
    PygConsolidatedSyncOut,
    PygIncSyncIn,
    PygIncSyncOut,
    PygLtdSyncIn,
    PygLtdSyncOut,
    PygSlSyncIn,
    PygSlSyncOut,
    ResolveReviewItemIn,
    ResolveReviewItemOut,
    ReviewItemOut,
    ReviewDigestRunIn,
    ReviewDigestRunOut,
    SupplierOut,
    ValidationProcessRunOut,
)
from lector_facturas.gmail_sync import (
    INVOICE_FILE_EXTENSIONS,
    classify_invoice_attachment,
    download_attachment_bytes,
    list_messages_in_window,
    looks_like_invoice_attachment,
)
from lector_facturas.api.store import ReviewStore
from lector_facturas.drive_bootstrap import bootstrap_drive_structure
from lector_facturas.google_drive import GoogleDriveClient
from lector_facturas.invoice_ingestion import (
    VALIDATION_ROOT_PARTS,
    ensure_drive_path,
    ensure_validation_folders,
    process_email_attachment,
    stage_email_attachment,
    stage_payroll_attachment,
    process_validation_drive_file,
)
from lector_facturas.payment_fees import PayPalClient, PaymentFeeService, ShopifyPaymentsClient
from lector_facturas.pyg_sync import sync_pyg_consolidated_to_drive, sync_pyg_inc_to_drive, sync_pyg_ltd_to_drive, sync_pyg_sl_to_drive
from lector_facturas.review_notifications import (
    ProcessedInvoiceItem,
    build_nightly_review_digest_email,
    send_message_via_gmail,
)
from lector_facturas.review_notifications import NightlyReviewDigest
from lector_facturas.settings import AppSettings, load_settings


def create_app() -> FastAPI:
    app = FastAPI(title="lector_facturas API", version="0.1.0")

    @app.middleware("http")
    async def api_key_middleware(request: Request, call_next: object) -> Response:
        if request.url.path == "/health":
            return await call_next(request)
        secret = os.environ.get("API_SECRET_KEY", "").strip()
        if secret:
            auth = request.headers.get("Authorization", "")
            if auth != f"Bearer {secret}":
                return Response(
                    content='{"detail":"Unauthorized"}',
                    status_code=401,
                    media_type="application/json",
                )
        return await call_next(request)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/companies", response_model=list[CompanyOut])
    def list_companies(store: ReviewStore = Depends(get_store)) -> list[CompanyOut]:
        return [CompanyOut(**item) for item in store.list_companies()]

    @app.get("/suppliers", response_model=list[SupplierOut])
    def list_suppliers(
        company: str | None = Query(default=None),
        store: ReviewStore = Depends(get_store),
    ) -> list[SupplierOut]:
        return [SupplierOut(**item) for item in store.list_suppliers(company=company)]

    @app.get("/review-items", response_model=list[ReviewItemOut])
    def list_review_items(
        status: str | None = Query(default=None),
        store: ReviewStore = Depends(get_store),
    ) -> list[ReviewItemOut]:
        return [ReviewItemOut(**item.__dict__) for item in store.list_review_items(status=status)]

    @app.get("/review-items/{review_item_id}", response_model=ReviewItemOut)
    def get_review_item(review_item_id: str, store: ReviewStore = Depends(get_store)) -> ReviewItemOut:
        try:
            item = store.get_review_item(review_item_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Review item not found") from exc
        return ReviewItemOut(**item.__dict__)

    def _get_mail_sync_state(
        mailbox: str | None = Query(default=None),
        sync_name: str = Query(default="revision-correo-principal"),
        store: ReviewStore = Depends(get_store),
        settings: AppSettings = Depends(get_settings),
    ) -> MailSyncStateOut:
        resolved_mailbox = mailbox or settings.gmail_sender
        state = store.get_mail_sync_state(mailbox=resolved_mailbox, sync_name=sync_name)
        return MailSyncStateOut(**state)

    @app.get("/jobs/mail-sync/state", response_model=MailSyncStateOut)
    def get_mail_sync_state(
        mailbox: str | None = Query(default=None),
        sync_name: str = Query(default="revision-correo-principal"),
        store: ReviewStore = Depends(get_store),
        settings: AppSettings = Depends(get_settings),
    ) -> MailSyncStateOut:
        return _get_mail_sync_state(
            mailbox=mailbox,
            sync_name=sync_name,
            store=store,
            settings=settings,
        )

    @app.get("/jobs/email-review/state", response_model=MailSyncStateOut)
    def get_email_review_state(
        mailbox: str | None = Query(default=None),
        sync_name: str = Query(default="revision-correo-principal"),
        store: ReviewStore = Depends(get_store),
        settings: AppSettings = Depends(get_settings),
    ) -> MailSyncStateOut:
        return _get_mail_sync_state(
            mailbox=mailbox,
            sync_name=sync_name,
            store=store,
            settings=settings,
        )

    def _run_mail_sync(
        payload: MailSyncRunIn,
        store: ReviewStore = Depends(get_store),
        settings: AppSettings = Depends(get_settings),
    ) -> MailSyncRunOut:
        if not settings.gmail_ready:
            raise HTTPException(status_code=400, detail="Gmail settings are incomplete.")
        resolved_mailbox = payload.mailbox or settings.gmail_sender
        current_state = store.get_mail_sync_state(mailbox=resolved_mailbox, sync_name=payload.sync_name)
        effective_from_at = payload.from_at or current_state["last_processed_at"]
        if effective_from_at is None:
            raise HTTPException(
                status_code=400,
                detail="from_at is required on the first sync run.",
            )
        effective_to_at = payload.to_at or datetime.now(tz=UTC)
        if effective_to_at.tzinfo is None or effective_from_at.tzinfo is None:
            raise HTTPException(status_code=400, detail="from_at and to_at must include timezone information.")
        if effective_to_at < effective_from_at:
            raise HTTPException(status_code=400, detail="to_at must be greater than or equal to from_at.")

        messages = list_messages_in_window(
            settings.to_gmail_config(),
            from_at=effective_from_at,
            to_at=effective_to_at,
            max_messages=payload.max_messages,
        )
        attachments_found = 0
        duplicate_attachments = 0
        new_attachments = 0
        skipped_non_invoice_attachments = 0
        sample_new_attachments: list[str] = []
        sample_duplicate_attachments: list[str] = []
        candidate_attachments: list[tuple[object, object]] = []
        export_attachments: list[tuple[object, object]] = []
        last_processed_at = current_state["last_processed_at"]
        last_processed_message_id = current_state["last_processed_message_id"]

        for message in messages:
            if last_processed_at is None or message.received_at >= last_processed_at:
                last_processed_at = message.received_at
                last_processed_message_id = message.message_id
            for attachment in message.attachments:
                attachments_found += 1
                suffix = Path(attachment.filename).suffix.lower()
                if suffix not in INVOICE_FILE_EXTENSIONS:
                    skipped_non_invoice_attachments += 1
                    continue
                if not looks_like_invoice_attachment(message, attachment):
                    skipped_non_invoice_attachments += 1
                    continue
                export_attachments.append((message, attachment))
                if store.document_exists_exact(
                    email_message_id=message.message_id,
                    original_filename=attachment.filename,
                ):
                    duplicate_attachments += 1
                    if len(sample_duplicate_attachments) < 25:
                        sample_duplicate_attachments.append(
                            f"{message.received_at.isoformat()} | {attachment.filename} | {message.sender_email or '-'} | {message.subject or '-'}"
                        )
                    continue
                if store.document_exists_by_original_filename(
                    original_filename=attachment.filename,
                ) or store.document_exists_by_normalized_filename(
                    original_filename=attachment.filename,
                ):
                    duplicate_attachments += 1
                    if len(sample_duplicate_attachments) < 25:
                        sample_duplicate_attachments.append(
                            f"{message.received_at.isoformat()} | {attachment.filename} | {message.sender_email or '-'} | {message.subject or '-'}"
                        )
                    continue
                new_attachments += 1
                candidate_attachments.append((message, attachment))
                if len(sample_new_attachments) < 25:
                    sample_new_attachments.append(
                        f"{message.received_at.isoformat()} | {attachment.filename} | {message.sender_email or '-'} | {message.subject or '-'}"
                    )

        if payload.update_checkpoint and last_processed_at is not None:
            state = store.upsert_mail_sync_state(
                mailbox=resolved_mailbox,
                sync_name=payload.sync_name,
                last_processed_at=last_processed_at,
                last_processed_message_id=last_processed_message_id,
            )
        else:
            state = {
                "mailbox": resolved_mailbox,
                "sync_name": payload.sync_name,
                "last_processed_at": last_processed_at,
                "last_processed_message_id": last_processed_message_id,
            }

        candidates_folder_path = ""
        candidates_folder_url = ""
        if payload.export_candidates_to_drive and candidate_attachments:
            if not settings.google_oauth_ready or not settings.drive_root_folder_id:
                raise HTTPException(status_code=400, detail="Google Drive is not configured for candidate export.")
            drive_client = GoogleDriveClient(settings.to_drive_config())
            folder_name = (
                f"{effective_from_at.astimezone(UTC).strftime('%Y%m%d%H%M')}_"
                f"{effective_to_at.astimezone(UTC).strftime('%Y%m%d%H%M')}"
            )
            parent_id = settings.drive_root_folder_id
            path_parts = ["validation", "email-review-candidates", folder_name]
            for part in path_parts:
                folder = drive_client.ensure_folder(name=part, parent_id=parent_id)
                parent_id = str(folder["id"])
            drive_client.ensure_folder(name="invoices", parent_id=parent_id)
            drive_client.ensure_folder(name="no-invoices", parent_id=parent_id)
            candidates_folder = drive_client.get_file(parent_id)
            candidates_folder_path = (
                "ARTESTA - 6. Finances\\validation\\email-review-candidates\\"
                f"{folder_name}"
            )
            candidates_folder_url = str(candidates_folder.get("webViewLink", ""))
            gmail_config = settings.to_gmail_config()
            for message, attachment in candidate_attachments:
                content = download_attachment_bytes(
                    gmail_config,
                    message_id=message.message_id,
                    attachment_id=attachment.attachment_id,
                )
                stored_name = (
                    f"{message.received_at.astimezone(UTC).strftime('%Y%m%dT%H%M%SZ')}_"
                    f"{_safe_sender_fragment(message.sender_email)}_"
                    f"{attachment.filename}"
                )
                mime_type = attachment.mime_type or "application/pdf"
                drive_client.ensure_file(
                    name=stored_name,
                    parent_id=parent_id,
                    content=content,
                    mime_type=mime_type,
                )

        email_sent = False
        email_message_id = ""
        if payload.send_email and settings.gmail_ready:
            period_label = f"{effective_from_at.astimezone(UTC).strftime('%Y%m%d%H%M')}-{effective_to_at.astimezone(UTC).strftime('%Y%m%d%H%M')}"
            digest = NightlyReviewDigest(
                company=resolved_mailbox,
                period_yyyymm=period_label,
                loaded_invoice_items=(),
                pending_load_items=tuple(sample_new_attachments),
                duplicate_items=tuple(sample_duplicate_attachments),
                notes=(
                    "Resumen de la revisión de correo. "
                    "Las facturas pendientes son adjuntos detectados que aun no se han cargado automaticamente."
                ),
            )
            response = send_message_via_gmail(
                build_nightly_review_digest_email(digest, settings.to_gmail_config()),
                settings.to_gmail_config(),
            )
            email_sent = True
            email_message_id = str(response.get("id", ""))

        return MailSyncRunOut(
            mailbox=resolved_mailbox,
            sync_name=payload.sync_name,
            effective_from_at=effective_from_at,
            effective_to_at=effective_to_at,
            messages_scanned=len(messages),
            attachments_found=attachments_found,
            duplicate_attachments=duplicate_attachments,
            new_attachments=new_attachments,
            skipped_non_invoice_attachments=skipped_non_invoice_attachments,
            last_processed_at=state["last_processed_at"],
            last_processed_message_id=state["last_processed_message_id"],
            email_sent=email_sent,
            email_message_id=email_message_id,
            candidates_folder_path=candidates_folder_path,
            candidates_folder_url=candidates_folder_url,
            sample_new_attachments=sample_new_attachments,
            sample_duplicate_attachments=sample_duplicate_attachments,
        )

    def _run_validation_to_process_v2(
        *,
        store: ReviewStore,
        settings: AppSettings,
    ) -> ValidationProcessRunOut:
        if not settings.google_oauth_ready or not settings.drive_root_folder_id:
            raise HTTPException(status_code=400, detail="Google Drive is not configured.")
        drive_client = GoogleDriveClient(settings.to_drive_config())
        validation_folders = ensure_validation_folders(drive_client, root_folder_id=settings.drive_root_folder_id)
        to_process_files = [
            item
            for item in drive_client.list_files(parent_id=str(validation_folders["to_process"]["id"]))
            if item.get("mimeType") != "application/vnd.google-apps.folder"
        ]
        processed_items: list[str] = []
        returned_items: list[str] = []
        ignored_duplicates = 0
        processed_count = 0
        returned_count = 0
        for file_item in to_process_files:
            result = process_validation_drive_file(
                store=store,
                drive_client=drive_client,
                root_folder_id=settings.drive_root_folder_id,
                validation_folders=validation_folders,
                file_item=file_item,
            )
            if result.action == "processed_from_to_process":
                processed_count += 1
                if len(processed_items) < 25:
                    processed_items.append(result.summary_line)
            elif result.action == "returned_to_to_check":
                returned_count += 1
                if len(returned_items) < 25:
                    returned_items.append(result.summary_line)
            elif result.action == "ignored_duplicate":
                ignored_duplicates += 1
        return ValidationProcessRunOut(
            processed_from_to_process=processed_count,
            returned_to_to_check=returned_count,
            ignored_duplicates=ignored_duplicates,
            sample_processed_from_to_process=processed_items,
            sample_returned_to_to_check=returned_items,
        )

    def _run_mail_sync_v2(
        payload: MailSyncRunIn,
        *,
        store: ReviewStore,
        settings: AppSettings,
    ) -> MailSyncRunOut:
        if not settings.gmail_ready:
            raise HTTPException(status_code=400, detail="Gmail settings are incomplete.")
        if not settings.google_oauth_ready or not settings.drive_root_folder_id:
            raise HTTPException(status_code=400, detail="Google Drive is not configured.")
        resolved_mailbox = payload.mailbox or settings.gmail_sender
        current_state = store.get_mail_sync_state(mailbox=resolved_mailbox, sync_name=payload.sync_name)
        effective_from_at = payload.from_at or current_state["last_processed_at"]
        if effective_from_at is None:
            raise HTTPException(status_code=400, detail="from_at is required on the first sync run.")
        effective_to_at = payload.to_at or datetime.now(tz=UTC)
        if effective_to_at.tzinfo is None or effective_from_at.tzinfo is None:
            raise HTTPException(status_code=400, detail="from_at and to_at must include timezone information.")
        if effective_to_at < effective_from_at:
            raise HTTPException(status_code=400, detail="to_at must be greater than or equal to from_at.")

        drive_client = GoogleDriveClient(settings.to_drive_config())
        validation_folders = ensure_validation_folders(drive_client, root_folder_id=settings.drive_root_folder_id)
        validation_root = drive_client.ensure_folder(name=VALIDATION_ROOT_PARTS[0], parent_id=settings.drive_root_folder_id)
        gmail_config = settings.to_gmail_config()
        messages = list_messages_in_window(
            gmail_config,
            from_at=effective_from_at,
            to_at=effective_to_at,
            max_messages=payload.max_messages,
        )
        attachments_found = 0
        duplicate_attachments = 0
        new_attachments = 0
        skipped_non_invoice_attachments = 0
        auto_processed_attachments = 0
        sent_to_to_check = 0
        sent_to_no_invoice = 0
        sample_new_attachments: list[str] = []
        sample_duplicate_attachments: list[str] = []
        sample_auto_processed: list[str] = []
        sample_to_check: list[str] = []
        sample_no_invoice: list[str] = []
        last_processed_at = current_state["last_processed_at"]
        last_processed_message_id = current_state["last_processed_message_id"]

        for message in messages:
            if last_processed_at is None or message.received_at >= last_processed_at:
                last_processed_at = message.received_at
                last_processed_message_id = message.message_id
            for attachment in message.attachments:
                attachments_found += 1
                suffix = Path(attachment.filename).suffix.lower()
                if suffix not in INVOICE_FILE_EXTENSIONS:
                    skipped_non_invoice_attachments += 1
                    continue
                content = download_attachment_bytes(
                    gmail_config,
                    message_id=message.message_id,
                    attachment_id=attachment.attachment_id,
                )
                result = process_email_attachment(
                    store=store,
                    drive_client=drive_client,
                    root_folder_id=settings.drive_root_folder_id,
                    validation_folders=validation_folders,
                    message=message,
                    attachment=attachment,
                    content=content,
                )
                if result.action == "ignored_duplicate":
                    duplicate_attachments += 1
                    if len(sample_duplicate_attachments) < 25:
                        sample_duplicate_attachments.append(result.summary_line)
                    continue
                new_attachments += 1
                if len(sample_new_attachments) < 25:
                    sample_new_attachments.append(result.summary_line)
                if result.action == "auto_processed":
                    auto_processed_attachments += 1
                    if len(sample_auto_processed) < 25:
                        sample_auto_processed.append(result.summary_line)
                elif result.action == "to_check":
                    sent_to_to_check += 1
                    if len(sample_to_check) < 25:
                        sample_to_check.append(result.summary_line)
                elif result.action == "no_invoice":
                    sent_to_no_invoice += 1
                    if len(sample_no_invoice) < 25:
                        sample_no_invoice.append(result.summary_line)

        if payload.update_checkpoint and last_processed_at is not None:
            state = store.upsert_mail_sync_state(
                mailbox=resolved_mailbox,
                sync_name=payload.sync_name,
                last_processed_at=last_processed_at,
                last_processed_message_id=last_processed_message_id,
            )
        else:
            state = {
                "mailbox": resolved_mailbox,
                "sync_name": payload.sync_name,
                "last_processed_at": last_processed_at,
                "last_processed_message_id": last_processed_message_id,
            }

        validation_run = ValidationProcessRunOut(
            processed_from_to_process=0,
            returned_to_to_check=0,
            ignored_duplicates=0,
            sample_processed_from_to_process=[],
            sample_returned_to_to_check=[],
        )
        if payload.process_validation_to_process:
            validation_run = _run_validation_to_process_v2(store=store, settings=settings)

        email_sent = False
        email_message_id = ""
        if payload.send_email and settings.gmail_ready:
            period_label = f"{effective_from_at.astimezone(UTC).strftime('%Y%m%d%H%M')}-{effective_to_at.astimezone(UTC).strftime('%Y%m%d%H%M')}"
            digest = NightlyReviewDigest(
                company=resolved_mailbox,
                period_yyyymm=period_label,
                loaded_invoice_items=tuple(sample_auto_processed),
                pending_load_items=tuple(sample_new_attachments),
                duplicate_items=tuple(sample_duplicate_attachments),
                to_check_items=tuple(sample_to_check),
                no_invoice_items=tuple(sample_no_invoice),
                processed_from_to_process_items=tuple(validation_run.sample_processed_from_to_process),
                returned_to_to_check_items=tuple(validation_run.sample_returned_to_to_check),
                notes="Resumen de la revision automatica de correo y de la cola validation/to-process.",
            )
            response = send_message_via_gmail(
                build_nightly_review_digest_email(digest, settings.to_gmail_config()),
                settings.to_gmail_config(),
            )
            email_sent = True
            email_message_id = str(response.get("id", ""))

        return MailSyncRunOut(
            mailbox=resolved_mailbox,
            sync_name=payload.sync_name,
            effective_from_at=effective_from_at,
            effective_to_at=effective_to_at,
            messages_scanned=len(messages),
            attachments_found=attachments_found,
            duplicate_attachments=duplicate_attachments,
            new_attachments=new_attachments,
            skipped_non_invoice_attachments=skipped_non_invoice_attachments,
            auto_processed_attachments=auto_processed_attachments,
            sent_to_to_check=sent_to_to_check,
            sent_to_no_invoice=sent_to_no_invoice,
            processed_from_to_process=validation_run.processed_from_to_process,
            returned_to_to_check=validation_run.returned_to_to_check,
            last_processed_at=state["last_processed_at"],
            last_processed_message_id=state["last_processed_message_id"],
            email_sent=email_sent,
            email_message_id=email_message_id,
            validation_root_path="ARTESTA - 6. Finances\\validation",
            validation_root_url=str(validation_root.get("webViewLink", "")),
            sample_new_attachments=sample_new_attachments,
            sample_duplicate_attachments=sample_duplicate_attachments,
            sample_auto_processed=sample_auto_processed,
            sample_to_check=sample_to_check,
            sample_no_invoice=sample_no_invoice,
            sample_processed_from_to_process=validation_run.sample_processed_from_to_process,
            sample_returned_to_to_check=validation_run.sample_returned_to_to_check,
        )

    def _run_email_download_v2(
        payload: MailSyncRunIn,
        *,
        store: ReviewStore,
        settings: AppSettings,
    ) -> MailSyncRunOut:
        if not settings.gmail_ready:
            raise HTTPException(status_code=400, detail="Gmail settings are incomplete.")
        if not settings.google_oauth_ready or not settings.drive_root_folder_id:
            raise HTTPException(status_code=400, detail="Google Drive is not configured.")
        resolved_mailbox = payload.mailbox or settings.gmail_sender
        current_state = store.get_mail_sync_state(mailbox=resolved_mailbox, sync_name=payload.sync_name)
        effective_from_at = payload.from_at or current_state["last_processed_at"]
        if effective_from_at is None:
            raise HTTPException(status_code=400, detail="from_at is required on the first sync run.")
        effective_to_at = payload.to_at or datetime.now(tz=UTC)
        if effective_to_at.tzinfo is None or effective_from_at.tzinfo is None:
            raise HTTPException(status_code=400, detail="from_at and to_at must include timezone information.")
        if effective_to_at < effective_from_at:
            raise HTTPException(status_code=400, detail="to_at must be greater than or equal to from_at.")

        drive_client = GoogleDriveClient(settings.to_drive_config())
        validation_folders = ensure_validation_folders(drive_client, root_folder_id=settings.drive_root_folder_id)
        gmail_config = settings.to_gmail_config()
        messages = list_messages_in_window(
            gmail_config,
            from_at=effective_from_at,
            to_at=effective_to_at,
            max_messages=payload.max_messages,
        )

        attachments_found = 0
        duplicate_attachments = 0
        new_attachments = 0
        skipped_non_invoice_attachments = 0
        sent_to_to_process = 0
        sent_to_no_invoice = 0
        sample_new_attachments: list[str] = []
        sample_duplicate_attachments: list[str] = []
        sample_to_process: list[str] = []
        sample_no_invoice: list[str] = []
        last_processed_at = current_state["last_processed_at"]
        last_processed_message_id = current_state["last_processed_message_id"]

        for message in messages:
            if last_processed_at is None or message.received_at >= last_processed_at:
                last_processed_at = message.received_at
                last_processed_message_id = message.message_id
            for attachment in message.attachments:
                attachments_found += 1
                suffix = Path(attachment.filename).suffix.lower()
                if suffix in {".xlsx", ".xls"} and _is_proco_sender(message.sender_email):
                    proco_detail_folder_id = str(validation_folders.get("proco_detail", {}).get("id", ""))
                    if proco_detail_folder_id:
                        xlsx_content = download_attachment_bytes(
                            gmail_config,
                            message_id=message.message_id,
                            attachment_id=attachment.attachment_id,
                        )
                        drive_client.upload_file(
                            name=attachment.filename,
                            parent_id=proco_detail_folder_id,
                            content=xlsx_content,
                            mime_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        )
                    skipped_non_invoice_attachments += 1
                    continue
                if suffix == ".pdf" and _is_payroll_sender(message.sender_email):
                    period_yyyymm = message.received_at.strftime("%Y%m") if message.received_at else datetime.now(tz=UTC).strftime("%Y%m")
                    company_code = "SL"
                    payroll_folder_path = f"ARTESTA - 6. Finances\\Artesta Store, S.L\\{period_yyyymm[:4]}\\{period_yyyymm}\\expenses\\opex\\staff"
                    payroll_folder_id = ensure_drive_path(drive_client, root_folder_id=settings.drive_root_folder_id, windows_path=payroll_folder_path)
                    pdf_content = download_attachment_bytes(
                        gmail_config,
                        message_id=message.message_id,
                        attachment_id=attachment.attachment_id,
                    )
                    result = stage_payroll_attachment(
                        store=store,
                        drive_client=drive_client,
                        payroll_folder_id=payroll_folder_id,
                        message=message,
                        attachment=attachment,
                        content=pdf_content,
                        period_yyyymm=period_yyyymm,
                        company_code=company_code,
                    )
                    if result.action == "ignored_duplicate":
                        duplicate_attachments += 1
                        if len(sample_duplicate_attachments) < 25:
                            sample_duplicate_attachments.append(result.summary_line)
                    else:
                        new_attachments += 1
                        if len(sample_new_attachments) < 25:
                            sample_new_attachments.append(result.summary_line)
                    continue
                if suffix not in INVOICE_FILE_EXTENSIONS:
                    skipped_non_invoice_attachments += 1
                    continue
                content = download_attachment_bytes(
                    gmail_config,
                    message_id=message.message_id,
                    attachment_id=attachment.attachment_id,
                )
                result = stage_email_attachment(
                    store=store,
                    drive_client=drive_client,
                    validation_folders=validation_folders,
                    message=message,
                    attachment=attachment,
                    content=content,
                )
                if result.action == "ignored_duplicate":
                    duplicate_attachments += 1
                    if len(sample_duplicate_attachments) < 25:
                        sample_duplicate_attachments.append(result.summary_line)
                    continue
                new_attachments += 1
                if len(sample_new_attachments) < 25:
                    sample_new_attachments.append(result.summary_line)
                if result.action == "to_process":
                    sent_to_to_process += 1
                    if len(sample_to_process) < 25:
                        sample_to_process.append(result.summary_line)
                elif result.action == "no_invoice":
                    sent_to_no_invoice += 1
                    if len(sample_no_invoice) < 25:
                        sample_no_invoice.append(result.summary_line)

        if payload.update_checkpoint and last_processed_at is not None:
            state = store.upsert_mail_sync_state(
                mailbox=resolved_mailbox,
                sync_name=payload.sync_name,
                last_processed_at=last_processed_at,
                last_processed_message_id=last_processed_message_id,
            )
        else:
            state = {
                "mailbox": resolved_mailbox,
                "sync_name": payload.sync_name,
                "last_processed_at": last_processed_at,
                "last_processed_message_id": last_processed_message_id,
            }

        return MailSyncRunOut(
            mailbox=resolved_mailbox,
            sync_name=payload.sync_name,
            effective_from_at=effective_from_at,
            effective_to_at=effective_to_at,
            messages_scanned=len(messages),
            attachments_found=attachments_found,
            duplicate_attachments=duplicate_attachments,
            new_attachments=new_attachments,
            skipped_non_invoice_attachments=skipped_non_invoice_attachments,
            sent_to_no_invoice=sent_to_no_invoice,
            last_processed_at=state["last_processed_at"],
            last_processed_message_id=state["last_processed_message_id"],
            sample_new_attachments=sample_new_attachments,
            sample_duplicate_attachments=sample_duplicate_attachments,
            sample_to_check=sample_to_process,
            sample_no_invoice=sample_no_invoice,
            validation_root_path="ARTESTA - 6. Finances\\validation",
            validation_root_url="",
        )

    def _run_review_digest_v2(
        payload: ReviewDigestRunIn,
        *,
        store: ReviewStore,
        settings: AppSettings,
    ) -> ReviewDigestRunOut:
        if not settings.gmail_ready:
            raise HTTPException(status_code=400, detail="Gmail settings are incomplete.")
        resolved_mailbox = payload.mailbox or settings.gmail_sender
        company = settings.company_name or resolved_mailbox
        now_utc = datetime.now(tz=UTC)
        effective_to_at = payload.to_at or now_utc
        effective_from_at = payload.from_at or effective_to_at.replace(hour=0, minute=0, second=0, microsecond=0)

        madrid = ZoneInfo("Europe/Madrid")
        period_str = (
            f"{effective_from_at.astimezone(madrid).strftime('%d/%m/%Y %H:%M')}"
            f" – {effective_to_at.astimezone(madrid).strftime('%H:%M')}"
        )

        items = store.list_ingestion_queue()
        window_items = [
            item for item in items
            if item.updated_at is not None and effective_from_at <= item.updated_at <= effective_to_at
        ]

        duplicate_items = tuple(
            f"{item.updated_at.astimezone(madrid).strftime('%H:%M')} | {item.original_filename} | {item.heuristic_reason or 'duplicada'}"
            for item in window_items
            if item.validation_bucket == "ignored_duplicate"
        )
        to_check_items = tuple(
            f"{item.updated_at.astimezone(madrid).strftime('%H:%M')} | {item.original_filename} | {item.parse_error or item.heuristic_reason or '-'}"
            for item in window_items
            if item.validation_bucket == "to_check" and item.source != "manual_to_process"
        )

        # Deduplicate no_invoice by filename — keep latest updated_at per filename
        _no_invoice_seen: dict[str, object] = {}
        for item in window_items:
            if item.validation_bucket == "no_invoice":
                existing = _no_invoice_seen.get(item.original_filename)
                if existing is None or (item.updated_at and item.updated_at > existing.updated_at):  # type: ignore[union-attr]
                    _no_invoice_seen[item.original_filename] = item
        no_invoice_items = tuple(
            f"{item.updated_at.astimezone(madrid).strftime('%H:%M')} | {item.original_filename} | {item.heuristic_reason or '-'}"  # type: ignore[union-attr]
            for item in _no_invoice_seen.values()
        )

        returned_to_to_check_items = tuple(
            f"{item.updated_at.astimezone(madrid).strftime('%H:%M')} | {item.original_filename} | {item.parse_error or item.heuristic_reason or '-'}"
            for item in window_items
            if item.validation_bucket == "to_check" and item.source == "manual_to_process"
        )

        # Unified processed items (auto_processed from any source)
        raw_processed = [item for item in window_items if item.validation_bucket == "auto_processed"]
        doc_ids = [item.document_id for item in raw_processed if item.document_id]
        doc_data = store.get_documents_by_ids(doc_ids)

        processed_items = tuple(
            ProcessedInvoiceItem(
                filename=item.stored_filename or item.original_filename,
                supplier_code=item.detected_supplier_code or "-",
                drive_url=item.drive_url or doc_data.get(item.document_id, {}).get("drive_url", ""),
                windows_path=doc_data.get(item.document_id, {}).get("windows_path", ""),
                gross_amount=doc_data.get(item.document_id, {}).get("gross_amount"),
                currency_code=doc_data.get(item.document_id, {}).get("currency_code", ""),
                invoice_number=doc_data.get(item.document_id, {}).get("invoice_number", ""),
                updated_at=item.updated_at,
            )
            for item in raw_processed
        )

        email_sent = False
        email_message_id = ""
        if payload.send_email:
            digest = NightlyReviewDigest(
                company=company,
                period_yyyymm=period_str,
                duplicate_items=duplicate_items,
                to_check_items=to_check_items,
                no_invoice_items=no_invoice_items,
                returned_to_to_check_items=returned_to_to_check_items,
                processed_items=processed_items,
            )
            response = send_message_via_gmail(
                build_nightly_review_digest_email(digest, settings.to_gmail_config()),
                settings.to_gmail_config(),
            )
            email_sent = True
            email_message_id = str(response.get("id", ""))

        return ReviewDigestRunOut(
            mailbox=resolved_mailbox,
            sync_name=payload.sync_name,
            effective_from_at=effective_from_at,
            effective_to_at=effective_to_at,
            auto_processed=len(processed_items),
            to_check=len(to_check_items),
            no_invoice=len(no_invoice_items),
            duplicates=len(duplicate_items),
            processed_from_to_process=len([i for i in raw_processed if i.source == "manual_to_process"]),
            returned_to_to_check=len(returned_to_to_check_items),
            email_sent=email_sent,
            email_message_id=email_message_id,
        )

    @app.post("/jobs/mail-sync/run", response_model=MailSyncRunOut)
    def run_mail_sync(
        payload: MailSyncRunIn,
        store: ReviewStore = Depends(get_store),
        settings: AppSettings = Depends(get_settings),
    ) -> MailSyncRunOut:
        return _run_mail_sync_v2(payload=payload, store=store, settings=settings)

    @app.post("/jobs/email-review/run", response_model=MailSyncRunOut)
    def run_email_review(
        payload: MailSyncRunIn,
        store: ReviewStore = Depends(get_store),
        settings: AppSettings = Depends(get_settings),
    ) -> MailSyncRunOut:
        return _run_mail_sync_v2(payload=payload, store=store, settings=settings)

    @app.post("/jobs/email-download/run", response_model=MailSyncRunOut)
    def run_email_download(
        payload: MailSyncRunIn,
        store: ReviewStore = Depends(get_store),
        settings: AppSettings = Depends(get_settings),
    ) -> MailSyncRunOut:
        return _run_email_download_v2(payload=payload, store=store, settings=settings)

    @app.post("/jobs/invoice-processing/run", response_model=ValidationProcessRunOut)
    def run_invoice_processing(
        store: ReviewStore = Depends(get_store),
        settings: AppSettings = Depends(get_settings),
    ) -> ValidationProcessRunOut:
        return _run_validation_to_process_v2(store=store, settings=settings)

    @app.post("/jobs/validation-to-process/run", response_model=ValidationProcessRunOut)
    def run_validation_to_process(
        store: ReviewStore = Depends(get_store),
        settings: AppSettings = Depends(get_settings),
    ) -> ValidationProcessRunOut:
        return _run_validation_to_process_v2(store=store, settings=settings)

    @app.post("/jobs/daily-review-email/run", response_model=ReviewDigestRunOut)
    def run_daily_review_email(
        payload: ReviewDigestRunIn,
        store: ReviewStore = Depends(get_store),
        settings: AppSettings = Depends(get_settings),
    ) -> ReviewDigestRunOut:
        return _run_review_digest_v2(payload=payload, store=store, settings=settings)

    @app.get("/validation/queue", response_model=list[IngestionQueueItemOut])
    def list_validation_queue(
        bucket: str | None = Query(default=None),
        store: ReviewStore = Depends(get_store),
    ) -> list[IngestionQueueItemOut]:
        return [IngestionQueueItemOut(**item.__dict__) for item in store.list_ingestion_queue(bucket=bucket)]

    @app.get("/validation/queue/{queue_item_id}", response_model=IngestionQueueItemOut)
    def get_validation_queue_item(
        queue_item_id: str,
        store: ReviewStore = Depends(get_store),
    ) -> IngestionQueueItemOut:
        try:
            item = store.get_ingestion_queue_item(queue_item_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Validation queue item not found") from exc
        return IngestionQueueItemOut(**item.__dict__)

    @app.delete("/documents/{document_id}")
    def delete_document(
        document_id: str,
        trash_drive_file: bool = Query(default=False),
        store: ReviewStore = Depends(get_store),
        settings: AppSettings = Depends(get_settings),
    ) -> dict:
        deleted = store.delete_document(document_id=document_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Document not found")
        if trash_drive_file and deleted.get("drive_file_id") and settings.google_oauth_ready:
            drive_client = GoogleDriveClient(settings.to_drive_config())
            drive_client.trash_file(file_id=deleted["drive_file_id"])
        return {"deleted": True, "document_id": document_id, "drive_file_id": deleted.get("drive_file_id", "")}

    @app.put("/otros-gastos/{company_code}/{period_yyyymm}")
    def upsert_otros_gastos(
        company_code: str,
        period_yyyymm: str,
        body: dict,
        store: ReviewStore = Depends(get_store),
    ) -> dict:
        """Insert or update an 'Otros gastos' entry for a company/period.
        Body: {"amount_eur": 123.45, "notes": "optional note"}
        company_code: SL | LTD | INC
        period_yyyymm: e.g. 202601
        """
        amount_eur = float(body.get("amount_eur", 0))
        notes = str(body.get("notes", ""))
        return store.upsert_otros_gastos(
            company_code=company_code.upper(),
            period_yyyymm=period_yyyymm,
            amount_eur=amount_eur,
            notes=notes,
        )

    @app.put("/otros-ingresos/{company_code}/{period_yyyymm}")
    def upsert_otros_ingresos(
        company_code: str,
        period_yyyymm: str,
        body: dict,
        store: ReviewStore = Depends(get_store),
    ) -> dict:
        """Insert or update an 'Otros ingresos' entry for a company/period.
        Body: {"amount_eur": 123.45, "notes": "optional note"}
        company_code: SL | LTD | INC
        period_yyyymm: e.g. 202601
        """
        amount_eur = float(body.get("amount_eur", 0))
        notes = str(body.get("notes", ""))
        return store.upsert_otros_ingresos(
            company_code=company_code.upper(),
            period_yyyymm=period_yyyymm,
            amount_eur=amount_eur,
            notes=notes,
        )

    # ------------------------------------------------------------------
    # Frame purchases / stock
    # ------------------------------------------------------------------

    @app.post("/supply/frame-purchases", response_model=FramePurchaseOut, status_code=201)
    def create_frame_purchase(
        body: FramePurchaseIn,
        store: ReviewStore = Depends(get_store),
        settings: AppSettings = Depends(get_settings),
    ) -> FramePurchaseOut:
        """Register a frame purchase order with unit prices per SKU (color × size).
        fabricante: 'Proco' (LTD/GBP) or 'TGI' (INC/USD).
        """
        lines = [
            {
                "frame_color": line.frame_color,
                "frame_size": line.frame_size,
                "quantity": line.quantity,
                "unit_price": str(line.unit_price),
            }
            for line in body.lines
        ]
        result = store.insert_frame_purchase(
            fabricante=body.fabricante,
            purchase_date=body.purchase_date.isoformat(),
            currency=body.currency,
            notes=body.notes,
            lines=lines,
        )
        return FramePurchaseOut(**result)

    @app.get("/supply/frame-purchases", response_model=list[FramePurchaseOut])
    def list_frame_purchases(
        fabricante: str | None = Query(default=None),
        store: ReviewStore = Depends(get_store),
    ) -> list[FramePurchaseOut]:
        """List all registered frame purchases, optionally filtered by fabricante."""
        rows = store.get_frame_purchases(fabricante=fabricante)
        return [FramePurchaseOut(**r) for r in rows]

    @app.get("/supply/frame-stock/{fabricante}/{yyyymm}", response_model=FrameStockSummaryOut)
    def get_frame_stock(
        fabricante: str,
        yyyymm: str,
        settings: AppSettings = Depends(get_settings),
    ) -> FrameStockSummaryOut:
        """Compute WAC-based stock summary for a fabricante and month (yyyymm).
        Returns opening/closing stock values and consumed value.
        fabricante: 'Proco' | 'TGI'
        """
        from lector_facturas.supply_stock import compute_frame_stock_summary
        if not settings.database_url:
            raise HTTPException(status_code=503, detail="DATABASE_URL not configured")
        summary = compute_frame_stock_summary(
            fabricante=fabricante,
            yyyymm=yyyymm,
            database_url=settings.database_url,
        )
        return FrameStockSummaryOut(
            fabricante=summary.fabricante,
            yyyymm=summary.yyyymm,
            currency=summary.currency,
            opening_units=summary.opening_units,
            opening_value=summary.opening_value,
            consumed_units=summary.consumed_units,
            consumed_value=summary.consumed_value,
            purchased_units=summary.purchased_units,
            closing_units=summary.closing_units,
            closing_value=summary.closing_value,
        )

    @app.get("/integrations/google-drive/status", response_model=GoogleDriveStatusOut)
    def google_drive_status(settings: AppSettings = Depends(get_settings)) -> GoogleDriveStatusOut:
        if not settings.google_oauth_ready:
            return GoogleDriveStatusOut(oauth_ready=False, drive_ready=False, error="Google OAuth is not configured.")
        try:
            client = GoogleDriveClient(settings.to_drive_config())
            payload = client.about()
            user = payload.get("user", {}) if isinstance(payload, dict) else {}
            return GoogleDriveStatusOut(
                oauth_ready=True,
                drive_ready=True,
                shared_drive_id=settings.drive_shared_drive_id,
                root_folder_id=settings.drive_root_folder_id,
                user_email=str(user.get("emailAddress", "")),
                user_display_name=str(user.get("displayName", "")),
            )
        except Exception as exc:  # noqa: BLE001
            return GoogleDriveStatusOut(
                oauth_ready=True,
                drive_ready=False,
                shared_drive_id=settings.drive_shared_drive_id,
                root_folder_id=settings.drive_root_folder_id,
                error=str(exc),
            )

    @app.post("/integrations/google-drive/bootstrap", response_model=DriveBootstrapOut)
    def google_drive_bootstrap(
        payload: DriveBootstrapIn,
        settings: AppSettings = Depends(get_settings),
    ) -> DriveBootstrapOut:
        if not settings.google_oauth_ready:
            raise HTTPException(status_code=400, detail="Google OAuth is not configured.")
        try:
            client = GoogleDriveClient(settings.to_drive_config())
            result = bootstrap_drive_structure(
                client,
                root_name=payload.root_name,
                year=payload.year,
                start_month=payload.start_month,
                end_month=payload.end_month,
                entities=tuple(payload.entities),
                parent_id=payload.parent_id,
            )
            root_folder = client.get_file(result.root_folder_id)
            return DriveBootstrapOut(
                root_folder_id=result.root_folder_id,
                root_folder_name=result.root_folder_name,
                root_folder_url=str(root_folder.get("webViewLink", "")),
                created_paths_count=len(result.created_paths),
                sample_paths=list(result.created_paths[:20]),
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    @app.post("/integrations/payment-fees/sync", response_model=list[PaymentFeeSyncOut])
    def sync_payment_fees(
        payload: PaymentFeeSyncIn,
        service: PaymentFeeService = Depends(get_payment_fee_service),
    ) -> list[PaymentFeeSyncOut]:
        try:
            results = service.sync(
                date_from=payload.date_from,
                date_to=payload.date_to,
                platform=payload.platform,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return [PaymentFeeSyncOut(**result.__dict__) for result in results]

    @app.post("/integrations/pyg/sl/sync", response_model=PygSlSyncOut)
    def sync_pyg_sl(
        payload: PygSlSyncIn,
        settings: AppSettings = Depends(get_settings),
    ) -> PygSlSyncOut:
        try:
            result = sync_pyg_sl_to_drive(
                settings=settings,
                year=payload.year,
                drive_folder_id=payload.drive_folder_id or None,
                file_name=payload.file_name or None,
            )
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return PygSlSyncOut(
            year=result.year,
            drive_folder_id=result.drive_folder_id,
            drive_file_id=result.drive_file_id,
            drive_file_name=result.drive_file_name,
            drive_file_url=result.drive_file_url,
            local_output_path=result.local_output_path,
            replaced_file_ids=list(result.replaced_file_ids),
        )

    @app.post("/integrations/pyg/ltd/sync", response_model=PygLtdSyncOut)
    def sync_pyg_ltd(
        payload: PygLtdSyncIn,
        settings: AppSettings = Depends(get_settings),
    ) -> PygLtdSyncOut:
        try:
            result = sync_pyg_ltd_to_drive(
                settings=settings,
                year=payload.year,
                drive_folder_id=payload.drive_folder_id or None,
                file_name=payload.file_name or None,
            )
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return PygLtdSyncOut(
            year=result.year,
            drive_folder_id=result.drive_folder_id,
            drive_file_id=result.drive_file_id,
            drive_file_name=result.drive_file_name,
            drive_file_url=result.drive_file_url,
            local_output_path=result.local_output_path,
            replaced_file_ids=list(result.replaced_file_ids),
        )

    @app.post("/integrations/pyg/inc/sync", response_model=PygIncSyncOut)
    def sync_pyg_inc(
        payload: PygIncSyncIn,
        settings: AppSettings = Depends(get_settings),
    ) -> PygIncSyncOut:
        try:
            result = sync_pyg_inc_to_drive(
                settings=settings,
                year=payload.year,
                drive_folder_id=payload.drive_folder_id or None,
                file_name=payload.file_name or None,
            )
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return PygIncSyncOut(
            year=result.year,
            drive_folder_id=result.drive_folder_id,
            drive_file_id=result.drive_file_id,
            drive_file_name=result.drive_file_name,
            drive_file_url=result.drive_file_url,
            local_output_path=result.local_output_path,
            replaced_file_ids=list(result.replaced_file_ids),
        )

    @app.post("/integrations/pyg/consolidated/sync", response_model=PygConsolidatedSyncOut)
    def sync_pyg_consolidated(
        payload: PygConsolidatedSyncIn,
        settings: AppSettings = Depends(get_settings),
    ) -> PygConsolidatedSyncOut:
        try:
            result = sync_pyg_consolidated_to_drive(
                settings=settings,
                year=payload.year,
                drive_folder_id=payload.drive_folder_id or None,
                file_name=payload.file_name or None,
            )
        except RuntimeError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        return PygConsolidatedSyncOut(
            year=result.year,
            drive_folder_id=result.drive_folder_id,
            drive_file_id=result.drive_file_id,
            drive_file_name=result.drive_file_name,
            drive_file_url=result.drive_file_url,
            local_output_path=result.local_output_path,
            replaced_file_ids=list(result.replaced_file_ids),
        )

    @app.get("/payment-fees/transactions", response_model=list[PaymentOrderTransactionOut])
    def list_payment_fee_transactions(
        company_code: str | None = Query(default=None),
        platform: str | None = Query(default=None),
        period_yyyymm: str | None = Query(default=None),
        date_from: str | None = Query(default=None),
        date_to: str | None = Query(default=None),
        market_code: str | None = Query(default=None),
        is_chargeback: bool | None = Query(default=None),
        payout_id: str | None = Query(default=None),
        store: ReviewStore = Depends(get_store),
    ) -> list[PaymentOrderTransactionOut]:
        items = store.list_payment_order_transactions(
            company_code=company_code,
            platform=platform,
            period_yyyymm=period_yyyymm,
            date_from=date_from,
            date_to=date_to,
            market_code=market_code,
            is_chargeback=is_chargeback,
            payout_id=payout_id,
        )
        return [PaymentOrderTransactionOut(**item.to_json_dict()) for item in items]

    @app.get("/payment-fees/summary", response_model=list[PaymentFeeSummaryOut])
    def list_payment_fee_summary(
        company_code: str | None = Query(default=None),
        platform: str | None = Query(default=None),
        period_yyyymm: str | None = Query(default=None),
        date_from: str | None = Query(default=None),
        date_to: str | None = Query(default=None),
        store: ReviewStore = Depends(get_store),
    ) -> list[PaymentFeeSummaryOut]:
        items = store.list_payment_fee_monthly_summary(
            company_code=company_code,
            platform=platform,
            period_yyyymm=period_yyyymm,
            date_from=date_from,
            date_to=date_to,
        )
        return [PaymentFeeSummaryOut(**item.to_json_dict()) for item in items]

    @app.post("/review-items/{review_item_id}/resolve", response_model=ResolveReviewItemOut)
    def resolve_review_item(
        review_item_id: str,
        payload: ResolveReviewItemIn,
        store: ReviewStore = Depends(get_store),
    ) -> ResolveReviewItemOut:
        try:
            item = store.resolve_review_item(
                review_item_id,
                company=payload.company,
                supplier_code=payload.supplier_code,
                invoice_date=payload.invoice_date,
                invoice_number=payload.invoice_number,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Review item not found") from exc
        return ResolveReviewItemOut(
            id=item.id,
            status=item.status,
            destination_file=item.destination_file,
            supplier_code=item.suggested_supplier_code,
        )

    @app.post("/jobs/daily-run", response_model=DailyRunOut)
    def daily_run(
        send_email: bool = Query(default=True),
        store: ReviewStore = Depends(get_store),
        settings: AppSettings = Depends(get_settings),
    ) -> DailyRunOut:
        digest = store.build_nightly_digest()
        email_sent = False
        email_message_id = ""
        if send_email and settings.gmail_ready:
            gmail_config = settings.to_gmail_config()
            response = send_message_via_gmail(
                build_nightly_review_digest_email(digest, gmail_config),
                gmail_config,
            )
            email_sent = True
            email_message_id = str(response.get("id", ""))
        return DailyRunOut(
            review_items_open=len(store.list_review_items(status="open")),
            email_sent=email_sent,
            email_message_id=email_message_id,
        )

    @app.post("/validation/to-process/upload")
    async def upload_to_process(
        file: UploadFile = File(...),
        sender_email: str = Form(default=""),
        store: ReviewStore = Depends(get_store),
        settings: AppSettings = Depends(get_settings),
    ) -> dict:
        if not settings.google_oauth_ready or not settings.drive_root_folder_id:
            raise HTTPException(status_code=400, detail="Google Drive is not configured.")
        content = await file.read()
        filename = file.filename or "upload.pdf"
        drive_client = GoogleDriveClient(settings.to_drive_config())
        validation_folders = ensure_validation_folders(drive_client, root_folder_id=settings.drive_root_folder_id)
        to_process_folder_id = str(validation_folders["to_process"]["id"])
        drive_file = drive_client.upload_file(
            name=filename,
            parent_id=to_process_folder_id,
            content=content,
            mime_type="application/pdf",
        )
        drive_file_id = str(drive_file.get("id", ""))
        drive_url = str(drive_file.get("webViewLink", ""))
        queue_item_id = str(uuid.uuid4())
        store.upsert_ingestion_queue_item(
            queue_item_id=queue_item_id,
            source="manual_upload",
            original_filename=filename,
            stored_filename=filename,
            sender_email=sender_email,
            drive_file_id=drive_file_id,
            drive_url=drive_url,
            validation_bucket="to_process",
        )
        return {"filename": filename, "drive_file_id": drive_file_id, "drive_url": drive_url}

    return app


@lru_cache(maxsize=1)
def get_store() -> ReviewStore:
    storage_path_env = os.environ.get("LECTOR_FACTURAS_REVIEW_STORE")
    finance_root_env = os.environ.get("FINANCE_ROOT")
    database_url_env = os.environ.get("DATABASE_URL")
    return ReviewStore(
        storage_path=Path(storage_path_env) if storage_path_env else None,
        finance_root=Path(finance_root_env) if finance_root_env else None,
        database_url=database_url_env,
    )


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    return load_settings()


def get_payment_fee_service(
    store: ReviewStore = Depends(get_store),
    settings: AppSettings = Depends(get_settings),
) -> PaymentFeeService:
    shopify_client = ShopifyPaymentsClient(settings.to_shopify_config()) if settings.shopify_ready else None
    paypal_client = PayPalClient(settings.to_paypal_config()) if settings.paypal_ready else None
    return PaymentFeeService(
        store,
        shopify_client=shopify_client,
        paypal_client=paypal_client,
    )


def _is_proco_sender(sender_email: str) -> bool:
    s = sender_email.lower()
    return "precisionproco" in s or "precision printing" in s


def _is_payroll_sender(sender_email: str) -> bool:
    return "dosconsulting" in sender_email.lower()


def _safe_sender_fragment(sender_email: str) -> str:
    cleaned = "".join(char if char.isalnum() else "-" for char in sender_email.lower())
    cleaned = cleaned.strip("-")
    return cleaned or "unknown"


app = create_app()
