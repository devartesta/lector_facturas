from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import psycopg

from lector_facturas.google_drive import GoogleDriveClient
from lector_facturas.settings import load_settings


SCHEMA_NAME = "invoices"
COMPANY_CODE = "SL"
COMPANY_FOLDER = "Artesta Store, S.L"
SUPPLIER_CODE = "BBVACNC"
SUPPLIER_NAME = "BANCO BILBAO VIZCAYA ARGENTARIA, S.A."
DESTINATION_PARTS = ("expenses", "opex", "administration")


@dataclass(frozen=True)
class CncInvoice:
    invoice_number: str
    invoice_date: date
    period_yyyymm: str
    original_filename: str
    source_path: Path | None
    drive_file_id: str
    email_message_id: str
    email_thread_id: str
    source_sender: str
    source_subject: str

    @property
    def source_channel(self) -> str:
        return "gmail" if self.email_message_id or self.email_thread_id else "manual"

    @property
    def canonical_filename(self) -> str:
        return f"{SUPPLIER_CODE}_{self.invoice_date:%Y%m%d}_{self.invoice_number}.pdf"

    @property
    def windows_path(self) -> str:
        return "\\".join(
            [
                "ARTESTA - 6. Finances",
                COMPANY_FOLDER,
                self.period_yyyymm[:4],
                self.period_yyyymm,
                *DESTINATION_PARTS,
                self.canonical_filename,
            ]
        )

    @property
    def billing_period_start(self) -> date:
        return date(int(self.period_yyyymm[:4]), int(self.period_yyyymm[4:]), 1)

    @property
    def billing_period_end(self) -> date:
        month = int(self.period_yyyymm[4:])
        year = int(self.period_yyyymm[:4])
        if month == 12:
            return date(year, 12, 31)
        return date(year, month + 1, 1).replace(day=1) - _one_day()


def _one_day():
    from datetime import timedelta

    return timedelta(days=1)


def main() -> int:
    settings = load_settings()
    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not configured.")
    if not settings.google_oauth_ready or not settings.drive_root_folder_id:
        raise RuntimeError("Google Drive settings are incomplete.")

    client = GoogleDriveClient(settings.to_drive_config())
    supplier_id = None
    processed: list[tuple[str, str, str, str]] = []

    invoices = [
        CncInvoice(
            invoice_number="250992A00599666",
            invoice_date=date(2025, 12, 5),
            period_yyyymm="202512",
            original_filename="1769421561992.pdf",
            source_path=_first_existing(
                [
                    Path.home() / "Downloads" / "1769421561992.pdf",
                    Path.home()
                    / "OneDrive - Artesta"
                    / "ARTESTA - 6. Finances"
                    / COMPANY_FOLDER
                    / "2025"
                    / "4Q"
                    / "202512"
                    / "Gastos"
                    / "Proveedores"
                    / "cnc"
                    / "1769421561992 (1).pdf",
                ]
            ),
            drive_file_id="",
            email_message_id="19bf9c0b0bd9fbab",
            email_thread_id="19bf9c0b0bd9fbab",
            source_sender="victor.morales.esteban@bbva.com",
            source_subject="Re: [External] Re: Entrega de maquina",
        ),
        CncInvoice(
            invoice_number="260992A00006870",
            invoice_date=date(2026, 1, 5),
            period_yyyymm="202601",
            original_filename="1769421583520.pdf",
            source_path=_first_existing(
                [
                    Path.home() / "Downloads" / "1769421583520.pdf",
                    Path.home()
                    / "OneDrive - Artesta"
                    / "ARTESTA - 6. Finances"
                    / COMPANY_FOLDER
                    / "2026"
                    / "1Q"
                    / "202601"
                    / "Gastos"
                    / "Proveedores"
                    / "CNC"
                    / "1769421583520 (1).pdf",
                ]
            ),
            drive_file_id="",
            email_message_id="19bf9c0b0bd9fbab",
            email_thread_id="19bf9c0b0bd9fbab",
            source_sender="victor.morales.esteban@bbva.com",
            source_subject="Re: [External] Re: Entrega de maquina",
        ),
        CncInvoice(
            invoice_number="260992A00059603",
            invoice_date=date(2026, 2, 3),
            period_yyyymm="202602",
            original_filename="descargaFactura (2).pdf",
            source_path=None,
            drive_file_id="1dHZmyEU31XVQo0HzAXqqQdVHVk8JtKKt",
            email_message_id="19daf38579cb052a",
            email_thread_id="19daf38579cb052a",
            source_sender="administracion@hannun.com",
            source_subject="Re: Facturacion Marzo 2026",
        ),
        CncInvoice(
            invoice_number="260992A00214940",
            invoice_date=date(2026, 5, 4),
            period_yyyymm="202605",
            original_filename="Renting_04.05.2026.pdf",
            source_path=_first_existing(
                [
                    Path.home() / "Downloads" / "Renting_04.05.2026.pdf",
                ]
            ),
            drive_file_id="",
            email_message_id="",
            email_thread_id="",
            source_sender="manual",
            source_subject="Manual import from local PDF",
        ),
        CncInvoice(
            invoice_number="260992A00266172",
            invoice_date=date(2026, 6, 3),
            period_yyyymm="202606",
            original_filename="Renting_03.06.2026.pdf",
            source_path=_first_existing(
                [
                    Path.home() / "Downloads" / "Renting_03.06.2026.pdf",
                ]
            ),
            drive_file_id="",
            email_message_id="",
            email_thread_id="",
            source_sender="manual",
            source_subject="Manual import from local PDF",
        ),
    ]

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            supplier_id = lookup_supplier_id(cur)
            for invoice in invoices:
                parent_id = ensure_drive_path(client, settings.drive_root_folder_id, invoice.windows_path)
                drive_file = ensure_or_move_drive_file(client, invoice=invoice, parent_id=parent_id)
                upsert_document(cur, invoice=invoice, supplier_id=supplier_id, drive_file=drive_file)
                processed.append(
                    (
                        invoice.invoice_number,
                        invoice.period_yyyymm,
                        str(drive_file.get("id", "")),
                        str(drive_file.get("webViewLink", "")),
                    )
                )
        conn.commit()

    for invoice_number, period, drive_id, drive_url in processed:
        print(f"{invoice_number} -> {period} | {drive_id} | {drive_url}")
    return 0


def _first_existing(paths: list[Path]) -> Path:
    for path in paths:
        if path.exists():
            return path
    raise FileNotFoundError("No local source file found: " + ", ".join(str(path) for path in paths))


def lookup_supplier_id(cursor) -> str:
    row = cursor.execute(
        f"SELECT id FROM {SCHEMA_NAME}.suppliers WHERE company_code = %s AND supplier_code = %s",
        (COMPANY_CODE, SUPPLIER_CODE),
    ).fetchone()
    if not row:
        raise RuntimeError(f"Supplier {SUPPLIER_CODE} not found for {COMPANY_CODE}.")
    return str(row[0])


def ensure_drive_path(client: GoogleDriveClient, root_folder_id: str, windows_path: str) -> str:
    parts = windows_path.split("\\")
    parent_id = root_folder_id
    for folder_name in parts[1:-1]:
        folder = client.ensure_folder(name=folder_name, parent_id=parent_id)
        parent_id = str(folder["id"])
    return parent_id


def ensure_or_move_drive_file(client: GoogleDriveClient, *, invoice: CncInvoice, parent_id: str) -> dict[str, object]:
    existing = client.list_files(parent_id=parent_id, name=invoice.canonical_filename)
    if existing:
        return existing[0]
    if invoice.drive_file_id:
        moved = client.move_file(file_id=invoice.drive_file_id, new_parent_id=parent_id)
        if str(moved.get("name", "")) != invoice.canonical_filename:
            moved = client.update_file_name(file_id=invoice.drive_file_id, name=invoice.canonical_filename)
        return moved
    if invoice.source_path is None:
        raise FileNotFoundError(f"No source path or existing Drive id for {invoice.invoice_number}.")
    return client.ensure_file(
        name=invoice.canonical_filename,
        parent_id=parent_id,
        content=invoice.source_path.read_bytes(),
        mime_type="application/pdf",
    )


def upsert_document(cursor, *, invoice: CncInvoice, supplier_id: str, drive_file: dict[str, object]) -> None:
    gross = Decimal("1989.99")
    vat = Decimal("345.37")
    net = Decimal("1644.62")
    raw = {
        "operation_number": f"0182F{invoice.invoice_number}",
        "invoice_number": invoice.invoice_number,
        "contract_number": "0182 1049 0505 00000000215620",
        "charged_on": invoice.billing_period_start.isoformat(),
        "gross_amount": str(gross),
        "vat_amount": str(vat),
        "net_amount": str(net),
        "vat_percent": "21.00",
        "currency_code": "EUR",
        "period_yyyymm": invoice.period_yyyymm,
        "billing_period_start": invoice.billing_period_start.isoformat(),
        "billing_period_end": invoice.billing_period_end.isoformat(),
        "issuer_company_name": SUPPLIER_NAME,
        "billed_company_name": "ARTESTA STORE, S.L.",
        "local_source_file": str(invoice.source_path) if invoice.source_path else "",
        "drive_file_id": str(drive_file.get("id", "")),
        "source_email_message_id": invoice.email_message_id,
        "source_email_thread_id": invoice.email_thread_id,
        "source_email_subject": invoice.source_subject,
        "source_email_sender": invoice.source_sender,
    }
    payload = (
        invoice.invoice_number,
        invoice.invoice_date,
        SUPPLIER_NAME,
        "ARTESTA STORE, S.L.",
        SUPPLIER_NAME,
        COMPANY_CODE,
        invoice.windows_path,
        str(drive_file.get("webViewLink", "")),
        None,
        invoice.source_sender,
        invoice.original_filename,
        "",
        invoice.billing_period_start,
        invoice.billing_period_end,
        Decimal("21.00"),
        gross,
        vat,
        net,
        supplier_id,
        SUPPLIER_CODE,
        "EUR",
        str(drive_file.get("id", "")),
        "GOOGLE_DRIVE",
        "invoice",
        "classified",
        invoice.source_channel,
        invoice.email_message_id,
        invoice.email_thread_id,
        invoice.original_filename,
        "manual_bbvcnc",
        Decimal("0.9900"),
        json.dumps(raw, ensure_ascii=True),
        "Imported/repaired BBVA CNC renting invoice.",
        invoice.source_sender,
        invoice.source_subject,
        invoice.period_yyyymm,
    )

    existing = cursor.execute(
        f"""
        SELECT id FROM {SCHEMA_NAME}.documents
        WHERE company_code = %s AND supplier_code = %s AND invoice_number = %s AND division_invoice = ''
        """,
        (COMPANY_CODE, SUPPLIER_CODE, invoice.invoice_number),
    ).fetchone()
    if existing:
        cursor.execute(
            f"""UPDATE {SCHEMA_NAME}.documents SET
                invoice_date=%s, issuer_company_name=%s, billed_company_name=%s, supplier_name=%s, company_code=%s,
                windows_path=%s, drive_url=%s, received_at=%s, sender_email=%s, original_filename=%s, division_invoice=%s,
                billing_period_start=%s, billing_period_end=%s, vat_percent=%s, gross_amount=%s, vat_amount=%s, net_amount=%s,
                supplier_id=%s, supplier_code=%s, currency_code=%s, drive_file_id=%s, storage_root=%s, document_type=%s,
                status=%s, source_channel=%s, email_message_id=%s, email_thread_id=%s, attachment_original_name=%s, parser_name=%s,
                parser_confidence=%s, extracted_raw=%s::jsonb, review_notes=%s, source_sender=%s, source_subject=%s, period_yyyymm=%s,
                updated_at=NOW()
               WHERE id=%s""",
            payload[1:] + (str(existing[0]),),
        )
        return

    cursor.execute(
        f"""INSERT INTO {SCHEMA_NAME}.documents (
            id, invoice_number, invoice_date, issuer_company_name, billed_company_name, supplier_name, company_code, windows_path, drive_url,
            received_at, sender_email, original_filename, division_invoice, billing_period_start, billing_period_end, vat_percent, gross_amount,
            vat_amount, net_amount, supplier_id, supplier_code, currency_code, drive_file_id, storage_root, document_type, status, source_channel,
            email_message_id, email_thread_id, attachment_original_name, parser_name, parser_confidence, extracted_raw, review_notes,
            created_at, updated_at, source_sender, source_subject, period_yyyymm
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s,
            NOW(), NOW(), %s, %s, %s
        )""",
        (str(uuid.uuid4()),) + payload,
    )


if __name__ == "__main__":
    raise SystemExit(main())
