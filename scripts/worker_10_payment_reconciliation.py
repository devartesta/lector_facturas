"""Worker 10 — Daily payment reconciliation workbook.

Generates a payment reconciliation Excel for each company (SL, LTD, INC) and
uploads it to Google Drive. Compares finance.informe_vat_gestorias_detalle
against invoices.payment_order_transactions (Shopify Payments + PayPal).

Schedule
--------
Runs once daily at RECON_HOUR:RECON_MINUTE (default 09:30 Europe/Madrid).
Generates the reconciliation for the *current* month. During the first
RECON_CLOSE_DAY days of a new month (default 3), also regenerates the
previous month so late-arriving payments are captured.

Environment variables
---------------------
RECON_HOUR            Run hour, local time        (default: 9)
RECON_MINUTE          Run minute, local time       (default: 30)
RECON_CLOSE_DAY       Keep prev month open ≤ day   (default: 3)
RECON_TIMEZONE        Timezone name                (default: Europe/Madrid)
RECON_COMPANIES       Comma-separated company codes (default: SL,LTD,INC)
RECONCILIATION_DRIVE_FOLDER_ID
                      Google Drive folder for uploads (required)
DATABASE_URL          PostgreSQL connection string  (required)
"""
from __future__ import annotations

import os
import sys
import time
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT  = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

try:
    from lector_facturas.review_notifications import send_worker_failure_alert
    _HAS_ALERT = True
except ImportError:
    _HAS_ALERT = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _prev_month(year: int, month: int) -> tuple[int, int]:
    if month == 1:
        return year - 1, 12
    return year, month - 1


def _months_to_process(ref: date, close_day: int) -> list[str]:
    current = f"{ref.year}{ref.month:02d}"
    py, pm = _prev_month(ref.year, ref.month)
    previous = f"{py}{pm:02d}"
    if ref.day <= close_day:
        return [previous, current]
    return [current]


def _next_run_today_or_tomorrow(*, now: datetime, hour: int, minute: int) -> datetime:
    candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= now:
        candidate += timedelta(days=1)
    return candidate


# ---------------------------------------------------------------------------
# Run logic
# ---------------------------------------------------------------------------

def run_reconciliation(*, companies: list[str], months: list[str]) -> bool:
    from lector_facturas.settings import load_settings
    from lector_facturas.pyg_sync import sync_payment_reconciliation_to_drive

    settings = load_settings()
    ok = True

    for company in companies:
        for period in months:
            try:
                print(f"[10-recon] {company}/{period} building...", flush=True)
                result = sync_payment_reconciliation_to_drive(
                    settings=settings,
                    company_code=company,
                    period_yyyymm=period,
                )
                print(
                    f"[10-recon] {company}/{period} OK "
                    f"| shopify: {result.shopify_only_accounting}+{result.shopify_amount_diff} "
                    f"| paypal: {result.paypal_only_accounting}+{result.paypal_amount_diff} "
                    f"| {result.drive_file_name}",
                    flush=True,
                )
            except Exception as exc:
                print(f"[10-recon] {company}/{period} ERROR: {exc}", flush=True)
                ok = False

    return ok


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    timezone_name  = os.environ.get("RECON_TIMEZONE", "Europe/Madrid")
    hour           = int(os.environ.get("RECON_HOUR", "9"))
    minute         = int(os.environ.get("RECON_MINUTE", "30"))
    close_day      = int(os.environ.get("RECON_CLOSE_DAY", "3"))
    companies_env  = os.environ.get("RECON_COMPANIES", "SL,LTD,INC")
    companies      = [c.strip() for c in companies_env.split(",") if c.strip()]

    timezone = ZoneInfo(timezone_name)
    print(
        f"[10-recon] started | tz={timezone_name} "
        f"| run={hour:02d}:{minute:02d} "
        f"| close_day={close_day} "
        f"| companies={companies}",
        flush=True,
    )

    last_run: date | None = None
    consecutive_failures  = 0

    while True:
        now   = datetime.now(tz=timezone)
        today = now.date()
        run_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

        if last_run != today and now >= run_time:
            months = _months_to_process(today, close_day)
            print(f"[10-recon] running | months={months}", flush=True)
            ok = run_reconciliation(companies=companies, months=months)
            last_run = today
            if ok:
                consecutive_failures = 0
            else:
                consecutive_failures += 1

            if consecutive_failures >= 3 and _HAS_ALERT:
                send_worker_failure_alert(
                    worker_name="10-payment-reconciliation",
                    consecutive_failures=consecutive_failures,
                )

        next_run   = _next_run_today_or_tomorrow(now=now, hour=hour, minute=minute)
        sleep_secs = max(30, int((next_run - now).total_seconds()))
        if last_run != today:
            sleep_secs = min(sleep_secs, 60)

        print(
            f"[10-recon] sleeping {sleep_secs}s "
            f"(next={next_run.strftime('%H:%M')})",
            flush=True,
        )
        time.sleep(sleep_secs)


if __name__ == "__main__":
    main()
