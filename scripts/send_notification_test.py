from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from lector_facturas.review_notifications import (
    GmailConfig,
    HistoricalInvoiceNotice,
    MissingExpectedInvoicesNotice,
    NightlyReviewDigest,
    UnmatchedInvoiceNotice,
    build_historical_invoice_email,
    build_missing_expected_invoices_email,
    build_nightly_review_digest_email,
    build_unmatched_supplier_email,
    send_message_via_gmail,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Send test notification emails for lector_facturas.")
    parser.add_argument(
        "--kind",
        choices=("unmatched_supplier", "historical_invoice", "missing_expected", "nightly_digest", "all"),
        default="nightly_digest",
    )
    parser.add_argument("--company", default="ARTESTA STORE, S.L.")
    parser.add_argument("--period", default="202603")
    return parser.parse_args()


def load_gmail_config() -> GmailConfig:
    recipients_env = os.environ.get("GMAIL_RECIPIENTS") or os.environ.get("GMAIL_RECIPIENT", "")
    recipients = tuple(recipient.strip() for recipient in recipients_env.split(",") if recipient.strip())
    if not recipients:
        raise RuntimeError("GMAIL_RECIPIENTS or GMAIL_RECIPIENT must be configured.")
    return GmailConfig(
        client_id=os.environ["GOOGLE_CLIENT_ID"],
        client_secret=os.environ["GOOGLE_CLIENT_SECRET"],
        refresh_token=os.environ["GOOGLE_REFRESH_TOKEN"],
        sender=os.environ["GMAIL_SENDER"],
        recipients=recipients,
        user_id=os.environ.get("GMAIL_USER_ID", "me"),
    )


def main() -> int:
    args = parse_args()
    config = load_gmail_config()
    review_path = Path(
        r"C:\Users\AdriàSebastià\OneDrive - Artesta\ARTESTA - 6. Finances"
    ) / "Artesta Store, S.L" / "2026" / args.period / "validation" / "pending_supplier_review"

    messages = []
    if args.kind in {"unmatched_supplier", "all"}:
        messages.append(
            build_unmatched_supplier_email(
                UnmatchedInvoiceNotice(
                    company=args.company,
                    period_yyyymm=args.period,
                    source_sender="billing@example.com",
                    source_subject="Factura para revisar",
                    attachment_names=("FACTURA_PRUEBA.pdf",),
                    review_path=review_path,
                    extracted_text="Texto de prueba para proveedor no reconocido.",
                    suggested_provider="META PLATFORMS IRELAND LIMITED",
                ),
                config,
            )
        )
    if args.kind in {"historical_invoice", "all"}:
        messages.append(
            build_historical_invoice_email(
                HistoricalInvoiceNotice(
                    company=args.company,
                    invoice_year=2025,
                    expected_year_from=2026,
                    source_sender="supplier@example.com",
                    source_subject="Factura diciembre 2025",
                    attachment_names=("FACTURA_2025.pdf",),
                    review_path=review_path,
                    invoice_number="2025-00421",
                    invoice_date="2025-12-30",
                    extracted_text="Factura emitida en 2025 detectada durante la carga automatica.",
                ),
                config,
            )
        )
    if args.kind in {"missing_expected", "all"}:
        messages.append(
            build_missing_expected_invoices_email(
                MissingExpectedInvoicesNotice(
                    company=args.company,
                    period_yyyymm=args.period,
                    missing_items=(
                        "META ADS - mensual - esperada el dia 3",
                        "SHOPIFY - mensual - esperada el dia 5",
                    ),
                    notes="Prueba de resumen diario. El catalogo de recurrencias se definira mas adelante.",
                ),
                config,
            )
        )
    if args.kind in {"nightly_digest", "all"}:
        messages.append(
            build_nightly_review_digest_email(
                NightlyReviewDigest(
                    company=args.company,
                    period_yyyymm=args.period,
                    unmatched_supplier_items=(
                        UnmatchedInvoiceNotice(
                            company=args.company,
                            period_yyyymm=args.period,
                            source_sender="billing@unknownvendor.com",
                            source_subject="Invoice March services",
                            attachment_names=("INV-3901.pdf",),
                            review_path=review_path,
                            suggested_provider="PRODUCTHERO",
                        ),
                        UnmatchedInvoiceNotice(
                            company=args.company,
                            period_yyyymm=args.period,
                            source_sender="ops@partner-mail.com",
                            source_subject="Factura marzo 2026",
                            attachment_names=("F26-188.pdf", "detalle.xlsx"),
                            review_path=review_path,
                        ),
                    ),
                    historical_invoice_items=(
                        HistoricalInvoiceNotice(
                            company=args.company,
                            invoice_year=2025,
                            expected_year_from=2026,
                            source_sender="supplier@example.com",
                            source_subject="Factura diciembre 2025",
                            attachment_names=("FACTURA_2025.pdf",),
                            review_path=review_path,
                            invoice_number="2025-00421",
                            invoice_date="2025-12-30",
                        ),
                    ),
                    missing_expected_items=(
                        "META ADS - mensual - esperada el dia 3",
                        "SHOPIFY - mensual - esperada el dia 5",
                    ),
                    notes="Prueba de resumen nocturno consolidado.",
                ),
                config,
            )
        )

    for message in messages:
        response = send_message_via_gmail(message, config)
        print(response.get("id", "sent"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
