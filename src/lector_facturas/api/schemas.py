from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from decimal import Decimal

from pydantic import BaseModel, Field


ReviewItemKind = Literal["unmatched_supplier", "historical_invoice", "missing_expected"]
ReviewItemStatus = Literal["open", "resolved"]


class CompanyOut(BaseModel):
    code: str
    name: str


class SupplierOut(BaseModel):
    company: str
    current_folder: str
    provider_name: str
    supplier_code: str
    destination_path: str
    notes: str = ""
    sender_emails: list[str] = Field(default_factory=list)
    payment_terms_days: int = 30
    is_direct_debit: bool = False
    preferred_payment_method: str = ""


class ReviewItemOut(BaseModel):
    id: str
    kind: ReviewItemKind
    status: ReviewItemStatus
    company: str
    period_yyyymm: str
    source_sender: str = ""
    source_subject: str = ""
    attachment_names: list[str] = Field(default_factory=list)
    review_path: str = ""
    source_file: str = ""
    suggested_provider: str = ""
    suggested_supplier_code: str = ""
    notes: str = ""
    destination_file: str = ""
    drive_file_id: str = ""
    drive_view_url: str = ""


class ResolveReviewItemIn(BaseModel):
    company: str
    supplier_code: str
    invoice_date: str
    invoice_number: str


class ResolveReviewItemOut(BaseModel):
    id: str
    status: ReviewItemStatus
    destination_file: str
    supplier_code: str


class DailyRunOut(BaseModel):
    review_items_open: int
    email_sent: bool
    email_message_id: str = ""


class MailSyncStateOut(BaseModel):
    mailbox: str
    sync_name: str
    last_processed_at: datetime | None = None
    last_processed_message_id: str = ""


class MailSyncRunIn(BaseModel):
    mailbox: str = ""
    sync_name: str = "revision-correo-principal"
    from_at: datetime | None = None
    to_at: datetime | None = None
    max_messages: int = 1000
    update_checkpoint: bool = True
    send_email: bool = False
    export_candidates_to_drive: bool = False
    process_validation_to_process: bool = True


class MailSyncRunOut(BaseModel):
    mailbox: str
    sync_name: str
    effective_from_at: datetime
    effective_to_at: datetime
    messages_scanned: int
    attachments_found: int
    duplicate_attachments: int
    new_attachments: int
    skipped_non_invoice_attachments: int
    auto_processed_attachments: int = 0
    sent_to_to_check: int = 0
    sent_to_no_invoice: int = 0
    processed_from_to_process: int = 0
    returned_to_to_check: int = 0
    last_processed_at: datetime | None = None
    last_processed_message_id: str = ""
    email_sent: bool = False
    email_message_id: str = ""
    validation_root_path: str = ""
    validation_root_url: str = ""
    sample_new_attachments: list[str] = Field(default_factory=list)
    sample_duplicate_attachments: list[str] = Field(default_factory=list)
    sample_auto_processed: list[str] = Field(default_factory=list)
    sample_to_check: list[str] = Field(default_factory=list)
    sample_no_invoice: list[str] = Field(default_factory=list)
    sample_processed_from_to_process: list[str] = Field(default_factory=list)
    sample_returned_to_to_check: list[str] = Field(default_factory=list)


class ValidationProcessRunOut(BaseModel):
    processed_from_to_process: int
    returned_to_to_check: int
    ignored_duplicates: int = 0
    sample_processed_from_to_process: list[str] = Field(default_factory=list)
    sample_returned_to_to_check: list[str] = Field(default_factory=list)


class ReviewDigestRunIn(BaseModel):
    mailbox: str = ""
    sync_name: str = "revision-correo-principal"
    from_at: datetime | None = None
    to_at: datetime | None = None
    send_email: bool = True


class ReviewDigestRunOut(BaseModel):
    mailbox: str
    sync_name: str
    effective_from_at: datetime
    effective_to_at: datetime
    auto_processed: int = 0
    to_check: int = 0
    no_invoice: int = 0
    duplicates: int = 0
    processed_from_to_process: int = 0
    returned_to_to_check: int = 0
    email_sent: bool = False
    email_message_id: str = ""


class IngestionQueueItemOut(BaseModel):
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


class GoogleDriveStatusOut(BaseModel):
    oauth_ready: bool
    drive_ready: bool
    shared_drive_id: str = ""
    root_folder_id: str = ""
    user_email: str = ""
    user_display_name: str = ""
    error: str = ""


class DriveBootstrapIn(BaseModel):
    root_name: str = "ARTESTA - 6. Finances"
    year: int = 2026
    start_month: int = 1
    end_month: int = 12
    parent_id: str = "root"
    entities: list[str] = Field(default_factory=lambda: ["SL", "Ltd", "Inc"])


class DriveBootstrapOut(BaseModel):
    root_folder_id: str
    root_folder_name: str
    root_folder_url: str = ""
    created_paths_count: int
    sample_paths: list[str] = Field(default_factory=list)


PaymentPlatform = Literal["shopify", "paypal"]


class PaymentFeeSyncIn(BaseModel):
    date_from: str
    date_to: str
    platform: PaymentPlatform | None = None


class PaymentFeeSyncOut(BaseModel):
    platform: PaymentPlatform
    transactions_upserted: int
    summaries_rebuilt: int
    date_from: str
    date_to: str


class PaymentFeeDetailSyncIn(BaseModel):
    company_code: str
    period_yyyymm: str
    drive_folder_id: str = ""
    file_name: str = ""


class PaymentFeeDetailSyncOut(BaseModel):
    company_code: str
    period_yyyymm: str
    drive_folder_id: str
    drive_file_id: str
    drive_file_name: str
    drive_file_url: str = ""
    local_output_path: str
    replaced_file_ids: list[str] = Field(default_factory=list)


class PaymentOrderTransactionOut(BaseModel):
    id: str
    platform: PaymentPlatform
    company_code: str
    market_code: str
    currency_code: str
    order_id: str = ""
    order_name: str = ""
    external_transaction_id: str
    external_payout_id: str = ""
    transaction_date: str
    payout_date: str = ""
    transaction_type: str = ""
    status: str = ""
    gross_amount: Decimal
    fee_amount: Decimal
    net_amount: Decimal
    chargeback_amount: Decimal
    chargeback_fee_amount: Decimal
    affects_balance: bool
    is_cancelled: bool
    is_chargeback: bool
    payment_reference: str = ""
    customer_reference: str = ""


class PaymentFeeSummaryOut(BaseModel):
    company_code: str
    period_yyyymm: str
    platform: PaymentPlatform
    market_code: str
    currency_code: str
    orders_count: int
    transactions_count: int
    gross_amount: Decimal
    fee_amount: Decimal
    chargeback_amount: Decimal
    chargeback_fee_amount: Decimal
    total_cost_amount: Decimal
    net_amount: Decimal
    payout_count: int


class PygSlSyncIn(BaseModel):
    year: int = 2026
    drive_folder_id: str = ""
    file_name: str = "pyg_sl_2026.xlsx"


class PygSlSyncOut(BaseModel):
    year: int
    drive_folder_id: str
    drive_file_id: str
    drive_file_name: str
    drive_file_url: str = ""
    local_output_path: str
    replaced_file_ids: list[str] = Field(default_factory=list)


class PygLtdSyncIn(BaseModel):
    year: int = 2026
    drive_folder_id: str = ""
    file_name: str = "pyg_ltd_2026.xlsx"


class PygLtdSyncOut(BaseModel):
    year: int
    drive_folder_id: str
    drive_file_id: str
    drive_file_name: str
    drive_file_url: str = ""
    local_output_path: str
    replaced_file_ids: list[str] = Field(default_factory=list)


class PygIncSyncIn(BaseModel):
    year: int = 2026
    drive_folder_id: str = ""
    file_name: str = "pyg_inc_2026.xlsx"


class PygIncSyncOut(BaseModel):
    year: int
    drive_folder_id: str
    drive_file_id: str
    drive_file_name: str
    drive_file_url: str = ""
    local_output_path: str
    replaced_file_ids: list[str] = Field(default_factory=list)


class PygConsolidatedSyncIn(BaseModel):
    year: int = 2026
    drive_folder_id: str = ""
    file_name: str = "pyg_consolidado_2026.xlsx"


class PygConsolidatedSyncOut(BaseModel):
    year: int
    drive_folder_id: str
    drive_file_id: str
    drive_file_name: str
    drive_file_url: str = ""
    local_output_path: str
    replaced_file_ids: list[str] = Field(default_factory=list)


PygCompany = Literal["consolidado", "sl", "ltd", "inc"]


class PygSnapshotRowOut(BaseModel):
    code: str
    label: str
    level: int
    kind: str
    parent_code: str | None = None
    style_key: str
    default_expanded: bool = False
    values_base: list[float] = Field(default_factory=list)
    values_eur: list[float] = Field(default_factory=list)
    total_base: float
    total_eur: float


class PygSnapshotOut(BaseModel):
    company: PygCompany
    base_currency: str
    months: list[str] = Field(default_factory=list)
    generated_at: datetime
    drive_file_name: str = ""
    drive_file_url: str = ""
    fx_mode: str = ""
    rows: list[PygSnapshotRowOut] = Field(default_factory=list)


class PygCellDetailItemOut(BaseModel):
    company: str
    period_yyyymm: str
    label: str
    invoice_number: str = ""
    amount_local: float
    amount_base: float
    amount_eur: float
    currency: str
    source: str = ""
    drive_url: str = ""


class PygCellDetailOut(BaseModel):
    company: PygCompany
    row_code: str
    row_label: str
    period_kind: str
    period_key: str
    currency: str
    cell_amount: float
    supported: bool = False
    is_reconciled: bool = False
    message: str = ""
    items: list[PygCellDetailItemOut] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Frame purchases / stock
# ---------------------------------------------------------------------------

class FramePurchaseLineIn(BaseModel):
    frame_color: str
    frame_size: str
    quantity: int = Field(..., gt=0)
    unit_price: Decimal = Field(..., ge=0)


class FramePurchaseIn(BaseModel):
    fabricante: str          # 'Proco' | 'TGI'
    purchase_date: date
    currency: str
    notes: str = ""
    lines: list[FramePurchaseLineIn] = Field(..., min_length=1)


class FramePurchaseLineOut(FramePurchaseLineIn):
    pass


class FramePurchaseOut(BaseModel):
    id: int
    fabricante: str
    purchase_date: str       # ISO date string
    currency: str
    notes: str
    created_at: str
    lines: list[FramePurchaseLineOut]


class FrameStockSummaryOut(BaseModel):
    fabricante: str
    yyyymm: str
    currency: str
    opening_units: int
    opening_value: Decimal
    consumed_units: int
    consumed_value: Decimal
    purchased_units: int
    closing_units: int
    closing_value: Decimal


class FrameConsumptionOverrideIn(BaseModel):
    quantity_override: int  # can be negative (stock return / correction)
    notes: str = ""


class FrameSkuWacEntryOut(BaseModel):
    effective_from: str   # ISO date
    wac: Decimal
    units_on_hand: int
    purchase_id: int
    created_at: str


class StockDetailSyncOut(BaseModel):
    fabricante: str
    mes_yyyymm: str
    drive_folder_id: str
    drive_file_id: str
    drive_file_name: str
    drive_file_url: str


class PaymentReconciliationSyncIn(BaseModel):
    company_code: str       # "SL", "LTD" or "INC"
    period_yyyymm: str      # e.g. "202602"


class PaymentReconciliationSyncOut(BaseModel):
    company_code: str
    period_yyyymm: str
    shopify_only_accounting: int
    shopify_only_payment: int
    shopify_amount_diff: int
    paypal_only_accounting: int
    paypal_only_payment: int
    paypal_amount_diff: int
    drive_file_name: str
    drive_file_url: str


class GestoriaSyncIn(BaseModel):
    company_code: str       # "SL", "LTD" or "INC"
    period_yyyymm: str      # e.g. "202602"


class GestoriaSyncOut(BaseModel):
    company_code: str
    period_yyyymm: str
    n_resumen_rows: int
    n_detalle_rows: int
    drive_file_name: str
    drive_file_url: str


class HourlyStepResult(BaseModel):
    step: str
    status: str          # "ok" | "error"
    detail: str = ""     # URL or error message


class SalesReportsRunOut(BaseModel):
    period_yyyymm: str
    results: list[HourlyStepResult]


class PygRunOut(BaseModel):
    year: int
    results: list[HourlyStepResult]


class PaymentFeeDetailRunOut(BaseModel):
    period_yyyymm: str
    results: list[HourlyStepResult]


# ---------------------------------------------------------------------------
# Payment tracking
# ---------------------------------------------------------------------------

class MarkDocumentPaymentIn(BaseModel):
    payment_status: str  # 'paid' | 'partial' | 'pending' | 'direct_debit'
    payment_date: date | None = None
    payment_method: str = ""  # 'bank_transfer' | 'card' | 'direct_debit' | 'paypal'
    payment_amount: Decimal | None = None
    payment_due_date: date | None = None  # if None, existing due_date is kept


class DocumentPaymentOut(BaseModel):
    id: str
    company_code: str
    supplier_code: str
    invoice_number: str
    invoice_date: date | None
    period_yyyymm: str
    gross_amount: Decimal | None
    net_amount: Decimal | None
    currency_code: str
    drive_url: str
    payment_status: str
    payment_date: date | None
    payment_method: str
    payment_amount: Decimal | None
    payment_due_date: date | None
    is_overdue: bool   # due_date < today AND status != 'paid'
    is_settled: bool   # direct_debit AND due_date <= today
    days_overdue: int | None  # positive = days past due, negative = days remaining
    is_direct_debit: bool = False
    document_type: str = "invoice"


class BulkPaymentIn(BaseModel):
    document_ids: list[str]
    payment_method: str = ""


class SupplierPaymentSettingsIn(BaseModel):
    payment_terms_days: int = 30
    is_direct_debit: bool = False
    preferred_payment_method: str = ""


class PaymentSettlementRunOut(BaseModel):
    settled_count: int
