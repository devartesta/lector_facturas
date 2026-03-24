from __future__ import annotations

import argparse
from dataclasses import dataclass
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
from lector_facturas.parsers.payroll import parse_payroll_summary_pdf
from lector_facturas.review_workflow import company_folder_name


SCHEMA_NAME = "invoices"
FINANCE_ROOT = Path(r"C:\Users\ADRISE~1\OneDrive - Artesta\ARTESTA - 6. Finances")


@dataclass(frozen=True)
class PayrollBundle:
    period_yyyymm: str
    summary_pdf: Path
    payroll_pdf: Path
    source_sender: str
    source_subject: str


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload SL payroll summaries to Google Drive and register them in Postgres.")
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

    bundles = discover_bundles()
    uploaded: list[tuple[str, str]] = []
    with psycopg.connect(args.database_url) as conn:
        with conn.cursor() as cur:
            for bundle in bundles:
                parsed = parse_payroll_summary_pdf(bundle.summary_pdf)
                base_filename = f"DOSCONSULTING_{parsed.payroll_period_end.strftime('%Y%m%d')}_{parsed.period_yyyymm}"
                company_path = "\\".join(
                    [
                        "ARTESTA - 6. Finances",
                        company_folder_name("ARTESTA STORE, S.L."),
                        parsed.period_yyyymm[:4],
                        parsed.period_yyyymm,
                        "expenses",
                        "opex",
                        "staff",
                    ]
                )
                parent_id = ensure_drive_path(client, args.root_folder_id, company_path + "\\" + f"{base_filename}.pdf")
                summary_name = f"{base_filename}_SUMMARY.pdf"
                payroll_name = f"{base_filename}_DETAIL.pdf"
                summary_drive = client.ensure_file(
                    name=summary_name,
                    parent_id=parent_id,
                    content=bundle.summary_pdf.read_bytes(),
                    mime_type="application/pdf",
                )
                payroll_drive = client.ensure_file(
                    name=payroll_name,
                    parent_id=parent_id,
                    content=bundle.payroll_pdf.read_bytes(),
                    mime_type="application/pdf",
                )
                upsert_payroll_row(
                    cur,
                    parsed=parsed,
                    source_sender=bundle.source_sender,
                    source_subject=bundle.source_subject,
                    windows_path=company_path + "\\" + summary_name,
                    drive_file_id=str(summary_drive.get("id", "")),
                    drive_url=str(summary_drive.get("webViewLink", "")),
                    detail_entry={
                        "name": payroll_name,
                        "drive_file_id": str(payroll_drive.get("id", "")),
                        "drive_url": str(payroll_drive.get("webViewLink", "")),
                        "local_source_file": str(bundle.payroll_pdf),
                    },
                    local_source_file=str(bundle.summary_pdf),
                )
                uploaded.append((parsed.period_yyyymm, str(summary_drive.get("webViewLink", ""))))
        conn.commit()

    print(f"Uploaded {len(uploaded)} payroll rows.")
    for period, url in uploaded:
        print(f"- {period} | {url}")
    return 0


def discover_bundles() -> list[PayrollBundle]:
    jan_dir = FINANCE_ROOT / "Artesta Store, S.L" / "2026" / "1Q" / "202601" / "Gastos" / "Nóminas"
    feb_dir = FINANCE_ROOT / "Artesta Store, S.L" / "2026" / "1Q" / "202602" / "Gastos" / "Nóminas"
    return [
        PayrollBundle(
            period_yyyymm="202601",
            summary_pdf=next(jan_dir.glob("ARTESTA STORE RESUMEN NOMINA*.pdf")),
            payroll_pdf=next(jan_dir.glob("ARTESTA STORE NOMINAS*.pdf")),
            source_sender="amaya@dosconsulting.net",
            source_subject="ARTESTA STORE NOMINAS ENERO",
        ),
        PayrollBundle(
            period_yyyymm="202602",
            summary_pdf=next(feb_dir.glob("ARTESTA STORE RESUMEN NOMINA*.pdf")),
            payroll_pdf=next(feb_dir.glob("ARTESTA STORE NOMINAS*.pdf")),
            source_sender="amaya@dosconsulting.net",
            source_subject="ARTESTA STORE NOMINAS FEBRERO",
        ),
    ]


def upsert_payroll_row(
    cursor,
    *,
    parsed,
    source_sender: str,
    source_subject: str,
    windows_path: str,
    drive_file_id: str,
    drive_url: str,
    detail_entry: dict[str, str],
    local_source_file: str,
) -> None:
    cursor.execute(
        f"""
        SELECT id FROM {SCHEMA_NAME}.payroll_documents
        WHERE company_code = %s AND provider_code = %s AND period_yyyymm = %s
        """,
        (parsed.company_code, parsed.provider_code, parsed.period_yyyymm),
    )
    existing = cursor.fetchone()
    extracted_raw = json.dumps(
        {
            **parsed.extracted_raw,
            "source_summary_file": local_source_file,
            "detail_files": [detail_entry],
        }
    )
    payload = (
        parsed.company_code,
        parsed.provider_code,
        parsed.provider_name,
        "gmail",
        source_sender,
        source_subject,
        parsed.original_filename,
        "payroll_summary",
        parsed.payroll_period_start,
        parsed.payroll_period_end,
        parsed.period_yyyymm,
        parsed.employee_count,
        parsed.gross_pay_amount,
        parsed.employee_deductions_amount,
        parsed.net_pay_amount,
        parsed.employer_social_security_amount,
        parsed.total_company_cost_amount,
        parsed.social_security_liquidation_amount,
        parsed.tax_withholdings_amount,
        parsed.currency_code,
        windows_path,
        drive_file_id,
        drive_url,
        extracted_raw,
        "Imported from DOSCONSULTING payroll summary and payroll detail PDFs.",
    )
    if existing:
        cursor.execute(
            f"""
            UPDATE {SCHEMA_NAME}.payroll_documents
            SET company_code = %s,
                provider_code = %s,
                provider_name = %s,
                source_channel = %s,
                source_sender = %s,
                source_subject = %s,
                original_filename = %s,
                document_type = %s,
                payroll_period_start = %s,
                payroll_period_end = %s,
                period_yyyymm = %s,
                employee_count = %s,
                gross_pay_amount = %s,
                employee_deductions_amount = %s,
                net_pay_amount = %s,
                employer_social_security_amount = %s,
                total_company_cost_amount = %s,
                social_security_liquidation_amount = %s,
                tax_withholdings_amount = %s,
                currency_code = %s,
                windows_path = %s,
                drive_file_id = %s,
                drive_url = %s,
                extracted_raw = %s::jsonb,
                review_notes = %s,
                updated_at = NOW()
            WHERE id = %s
            """,
            payload + (str(existing[0]),),
        )
        return
    cursor.execute(
        f"""
        INSERT INTO {SCHEMA_NAME}.payroll_documents (
            id, company_code, provider_code, provider_name, source_channel, source_sender, source_subject,
            original_filename, document_type, payroll_period_start, payroll_period_end, period_yyyymm,
            employee_count, gross_pay_amount, employee_deductions_amount, net_pay_amount,
            employer_social_security_amount, total_company_cost_amount, social_security_liquidation_amount,
            tax_withholdings_amount, currency_code, windows_path, drive_file_id, drive_url,
            extracted_raw, review_notes, created_at, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s::jsonb, %s, NOW(), NOW()
        )
        """,
        (str(uuid.uuid4()),) + payload,
    )


def ensure_drive_path(client: GoogleDriveClient, root_folder_id: str, windows_path: str) -> str:
    parts = windows_path.split("\\")
    folder_parts = parts[1:-1]
    parent_id = root_folder_id
    for folder_name in folder_parts:
        folder = client.ensure_folder(name=folder_name, parent_id=parent_id)
        parent_id = str(folder["id"])
    return parent_id


if __name__ == "__main__":
    raise SystemExit(main())
