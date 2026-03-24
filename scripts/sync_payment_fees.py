from __future__ import annotations

import argparse
from datetime import date, timedelta
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from lector_facturas.api.store import ReviewStore
from lector_facturas.payment_fees import PayPalClient, PaymentFeeService, ShopifyPaymentsClient
from lector_facturas.settings import load_settings


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Shopify and PayPal payment fees into invoices schema.")
    parser.add_argument("--date-from", help="Start date in YYYY-MM-DD format.")
    parser.add_argument("--date-to", help="End date in YYYY-MM-DD format.")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=45,
        help="If no explicit dates are provided, sync from today - lookback_days through today.",
    )
    parser.add_argument(
        "--platform",
        choices=["shopify", "paypal"],
        help="Sync only one platform. Defaults to both.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    settings = load_settings()
    if not settings.shopify_ready and not settings.paypal_ready:
        raise RuntimeError("Neither Shopify nor PayPal payment integrations are configured.")
    date_to = args.date_to or date.today().isoformat()
    date_from = args.date_from or (date.fromisoformat(date_to) - timedelta(days=args.lookback_days)).isoformat()
    service = PaymentFeeService(
        ReviewStore(database_url=os.environ.get("DATABASE_URL")),
        shopify_client=ShopifyPaymentsClient(settings.to_shopify_config()) if settings.shopify_ready else None,
        paypal_client=PayPalClient(settings.to_paypal_config()) if settings.paypal_ready else None,
    )
    results = service.sync(date_from=date_from, date_to=date_to, platform=args.platform)
    for result in results:
        print(
            f"{result.platform}: transactions_upserted={result.transactions_upserted} "
            f"summaries_rebuilt={result.summaries_rebuilt} from={result.date_from} to={result.date_to}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
