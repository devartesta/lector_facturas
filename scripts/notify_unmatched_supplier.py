from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from lector_facturas.provider_catalog import ensure_pending_supplier_review
from lector_facturas.review_notifications import (
    GmailConfig,
    UnmatchedInvoiceNotice,
    send_unmatched_supplier_email,
)


ENTITY_ALIASES = {
    "SL": "Artesta Store, S.L",
    "Ltd": "Artesta Stores (UK) Ltd",
    "Inc": "Artesta Inc",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Store and notify unmatched supplier invoices.")
    parser.add_argument("--root", required=True)
    parser.add_argument("--company", required=True, help="SL, Ltd, Inc or folder name")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--period", required=True, help="YYYYMM")
    parser.add_argument("--sender", default="")
    parser.add_argument("--subject", default="")
    parser.add_argument("--file", action="append", default=[])
    parser.add_argument("--text", default="")
    parser.add_argument("--suggested-provider", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    company_folder = ENTITY_ALIASES.get(args.company, args.company)
    review = ensure_pending_supplier_review(Path(args.root), company_folder, args.year, args.period)

    copied_names: list[str] = []
    for file_arg in args.file:
        source = Path(file_arg)
        destination = review.path / source.name
        if source.resolve() != destination.resolve():
            shutil.copy2(source, destination)
        copied_names.append(destination.name)

    recipients_env = os.environ.get("GMAIL_RECIPIENTS") or os.environ.get("GMAIL_RECIPIENT", "")
    recipients = tuple(recipient.strip() for recipient in recipients_env.split(",") if recipient.strip())
    if not recipients:
        raise RuntimeError("GMAIL_RECIPIENTS or GMAIL_RECIPIENT must be configured.")

    gmail_config = GmailConfig(
        client_id=os.environ["GOOGLE_CLIENT_ID"],
        client_secret=os.environ["GOOGLE_CLIENT_SECRET"],
        refresh_token=os.environ["GOOGLE_REFRESH_TOKEN"],
        sender=os.environ["GMAIL_SENDER"],
        recipients=recipients,
        user_id=os.environ.get("GMAIL_USER_ID", "me"),
    )
    notice = UnmatchedInvoiceNotice(
        company=company_folder,
        period_yyyymm=args.period,
        source_sender=args.sender,
        source_subject=args.subject,
        attachment_names=tuple(copied_names),
        review_path=review.path,
        extracted_text=args.text,
        suggested_provider=args.suggested_provider,
    )
    send_unmatched_supplier_email(notice, gmail_config)
    print(review.path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
