from __future__ import annotations

import argparse
from pathlib import Path
import json
import sys
import uuid

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import psycopg

from lector_facturas.google_drive import DriveConfig, GoogleDriveClient, GoogleOAuthConfig
from lector_facturas.parsers.canva import parse_canva_pdf
from lector_facturas.parsers.godaddy import parse_godaddy_pdf
from lector_facturas.parsers.gorgias import parse_gorgias_pdf
from lector_facturas.parsers.googleworkspace import parse_googleworkspace_pdf
from lector_facturas.parsers.hetzner import parse_hetzner_pdf
from lector_facturas.parsers.konvoai import parse_konvoai_pdf
from lector_facturas.parsers.masmovil import parse_masmovil_pdf
from lector_facturas.parsers.microsoft import parse_microsoft_pdf
from lector_facturas.parsers.openai import parse_openai_pdf
from lector_facturas.parsers.producthero import parse_producthero_pdf
from lector_facturas.parsers.shopify import parse_shopify_pdf
from lector_facturas.parsers.yumaai import parse_yumaai_pdf
from lector_facturas.review_workflow import company_folder_name, get_provider


SCHEMA_NAME = "invoices"
FINANCE_ROOT = Path(r"C:\Users\ADRISE~1\OneDrive - Artesta\ARTESTA - 6. Finances")
COMPANY = "ARTESTA STORE, S.L."
COMPANY_CODE = "SL"


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload a batch of technology invoices to Google Drive and register them in Postgres.")
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--root-folder-id", required=True)
    parser.add_argument("--client-id", required=True)
    parser.add_argument("--client-secret", required=True)
    parser.add_argument("--refresh-token", required=True)
    args = parser.parse_args()

    client = GoogleDriveClient(
        DriveConfig(
            oauth=GoogleOAuthConfig(
                client_id=args.client_id,
                client_secret=args.client_secret,
                refresh_token=args.refresh_token,
            ),
            root_folder_id=args.root_folder_id,
        )
    )

    specs = build_specs()
    uploaded: list[tuple[str, str, str]] = []
    with psycopg.connect(args.database_url) as conn:
        with conn.cursor() as cur:
            for spec in specs:
                for invoice_pdf in spec["files"]:
                    parsed = spec["parser"](invoice_pdf)
                    provider = get_provider(COMPANY, parsed.supplier_code)
                    filename = f"{parsed.supplier_code}_{parsed.invoice_date.strftime('%Y%m%d')}_{parsed.invoice_number.replace('/', '-')}.pdf"
                    windows_path = build_windows_path(provider.company, parsed.period_yyyymm, provider.destination_path, filename)
                    parent_id = ensure_drive_path(client, args.root_folder_id, windows_path)
                    pdf_drive = client.ensure_file(
                        name=filename,
                        parent_id=parent_id,
                        content=invoice_pdf.read_bytes(),
                        mime_type="application/pdf",
                    )
                    supplier_id = lookup_supplier_id(cur, COMPANY_CODE, parsed.supplier_code)
                    upsert_document_row(
                        cur,
                        supplier_id=supplier_id,
                        parsed=parsed,
                        windows_path=windows_path,
                        drive_url=str(pdf_drive.get("webViewLink", "")),
                        drive_file_id=str(pdf_drive.get("id", "")),
                        local_source_file=str(invoice_pdf),
                    )
                    uploaded.append((parsed.supplier_code, parsed.invoice_number, str(pdf_drive.get("webViewLink", ""))))
        conn.commit()

    print(f"Uploaded {len(uploaded)} technology invoices.")
    for item in uploaded:
        print(" | ".join(item))
    return 0


def build_specs() -> list[dict[str, object]]:
    jan = FINANCE_ROOT / "Artesta Store, S.L" / "2026" / "1Q" / "202601" / "Gastos" / "Proveedores"
    feb = FINANCE_ROOT / "Artesta Store, S.L" / "2026" / "1Q" / "202602" / "Gastos" / "Proveedores"
    return [
        {"parser": parse_canva_pdf, "files": [jan / "canva" / "invoice-04765-19148572.pdf", feb / "canva" / "invoice-04796-33900634 (1).pdf"]},
        {"parser": parse_googleworkspace_pdf, "files": [jan / "Google" / "5472439457.pdf", feb / "Google" / "5505030303.pdf"]},
        {"parser": parse_microsoft_pdf, "files": [jan / "microsoft" / "G135222851_9fd45cdbe51248e4a50e7b1380e7f460.pdf", feb / "microsoft" / "G140910268_d9ebaff6cb4e4401b7c0af1fec815995.pdf"]},
        {"parser": parse_openai_pdf, "files": [jan / "chatgpt" / "Invoice-BZHJNTUB-0001.pdf", feb / "chatgpt" / "Invoice-BZHJNTUB-0005.pdf"]},
        {"parser": parse_shopify_pdf, "files": [jan / "Shopify" / "Artesta_479105361.pdf", feb / "Shopify" / "Artesta_493806587.pdf"]},
        {"parser": parse_hetzner_pdf, "files": [jan / "HETZNER" / "Hetzner_2026-01-16_084000638408 (2).pdf", feb / "HETZNER" / "Hetzner_2026-02-16_084000695806 (1).pdf"]},
        {"parser": parse_gorgias_pdf, "files": [jan / "Gorgias" / "invoice_INC-01-2026-35934.pdf", feb / "Gorgias" / "gorgias-inc_invoice_16BVsEVE14zZv3qW4 (2).pdf"]},
        {"parser": parse_konvoai_pdf, "files": [jan / "konvo" / "Invoice-B5F7DF3C-6721.pdf", jan / "konvo" / "Invoice-B5F7DF3C-6940.pdf", feb / "konvo" / "Invoice-B5F7DF3C-7132 (1).pdf", feb / "konvo" / "Invoice-B5F7DF3C-7296 (1).pdf"]},
        {"parser": parse_masmovil_pdf, "files": [jan / "mas movil" / "68d220f4-fba3-4efe-ba70-d5f1d5c59746 (1).pdf"]},
        {"parser": parse_yumaai_pdf, "files": [jan / "YUMA" / "Invoice-OQXBYXMP-0004.pdf"]},
        {"parser": parse_godaddy_pdf, "files": [jan / "godaddy" / "MICUEN~1.PDF", jan / "godaddy" / "MICUEN~2.PDF"]},
        {"parser": parse_producthero_pdf, "files": [jan / "ProductHERO" / "invoice_205588.pdf"]},
    ]


def lookup_supplier_id(cursor, company_code: str, supplier_code: str) -> str | None:
    row = cursor.execute(
        f"SELECT id FROM {SCHEMA_NAME}.suppliers WHERE company_code = %s AND supplier_code = %s",
        (company_code, supplier_code),
    ).fetchone()
    return str(row[0]) if row else None


def upsert_document_row(cursor, *, supplier_id: str | None, parsed, windows_path: str, drive_url: str, drive_file_id: str, local_source_file: str) -> None:
    cursor.execute(
        f"SELECT id FROM {SCHEMA_NAME}.documents WHERE company_code = %s AND supplier_code = %s AND invoice_number = %s AND division_invoice = %s",
        (COMPANY_CODE, parsed.supplier_code, parsed.invoice_number, ""),
    )
    existing = cursor.fetchone()
    extracted_raw = json.dumps({**parsed.extracted_raw, "local_source_file": local_source_file})
    payload = (
        parsed.invoice_number, parsed.invoice_date, parsed.issuer_company_name, parsed.billed_company_name, parsed.supplier_name,
        COMPANY_CODE, windows_path, drive_url, None, parsed.sender_email, parsed.original_filename, "",
        parsed.billing_period_start, parsed.billing_period_end, parsed.vat_percent, parsed.gross_amount, parsed.vat_amount, parsed.net_amount,
        supplier_id, parsed.supplier_code, parsed.currency_code, drive_file_id, "GOOGLE_DRIVE", "invoice", "classified", "import", "", "",
        parsed.original_filename, parsed.parser_name, parsed.parser_confidence, extracted_raw, f"Imported by technology batch for {parsed.supplier_code}.",
        parsed.sender_email, parsed.original_filename, parsed.period_yyyymm,
    )
    update_payload = payload[1:]
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
            update_payload + (str(existing[0]),),
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


def build_windows_path(company: str, period_yyyymm: str, destination_path: str, filename: str) -> str:
    return "\\".join(["ARTESTA - 6. Finances", company_folder_name(company), period_yyyymm[:4], period_yyyymm, *Path(destination_path).parts, filename])


def ensure_drive_path(client: GoogleDriveClient, root_folder_id: str, windows_path: str) -> str:
    parts = windows_path.split("\\")
    parent_id = root_folder_id
    for folder_name in parts[1:-1]:
        folder = client.ensure_folder(name=folder_name, parent_id=parent_id)
        parent_id = str(folder["id"])
    return parent_id


if __name__ == "__main__":
    raise SystemExit(main())
