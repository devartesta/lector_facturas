"""Worker 09 — Daily frame stock refresh + Drive upload.

Two-phase daily run:
  Phase 1  (STOCK_DETAIL_DB_HOUR:STOCK_DETAIL_DB_MINUTE, default 07:00)
    → Refresh supply.frame_consumption_valued and supply.frame_stock_monthly
      for current month and (if today <= STOCK_DETAIL_CLOSE_DAY) previous month.

  Phase 2  (STOCK_DETAIL_EXCEL_HOUR:STOCK_DETAIL_EXCEL_MINUTE, default 07:30)
    → Upload per-SKU detail Excel to Drive (expenses/cogs/stock/<month>)
      for the same months as Phase 1.

"Close day" logic
  A month is considered "open" (still being refreshed) until day STOCK_DETAIL_CLOSE_DAY
  (default 2) of the *following* month.  On the 3rd the previous month is dropped.

Fabricantes handled: TGI (Inc) and Proco (Ltd).

Environment variables:
  STOCK_DETAIL_DB_HOUR          Phase-1 run hour,   local time  (default: 7)
  STOCK_DETAIL_DB_MINUTE        Phase-1 run minute              (default: 0)
  STOCK_DETAIL_EXCEL_HOUR       Phase-2 run hour,   local time  (default: 7)
  STOCK_DETAIL_EXCEL_MINUTE     Phase-2 run minute              (default: 30)
  STOCK_DETAIL_CLOSE_DAY        Last day of month to keep prev-month open (default: 2)
  STOCK_DETAIL_TIMEZONE         Timezone name                   (default: Europe/Madrid)
  STOCK_DETAIL_FABRICANTES      Comma-separated list            (default: TGI,Proco)
"""
from __future__ import annotations

import os
import sys
import time
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
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
    """Return the list of yyyymm months to refresh/upload today.

    Rule:
      - Always include the current month.
      - Include the previous month while today.day <= close_day
        (i.e. we're still within the grace period of the new month).
    """
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
# Phase 1: refresh DB tables
# ---------------------------------------------------------------------------

def run_db_refresh(*, fabricantes: list[str], months: list[str]) -> bool:
    import psycopg
    from lector_facturas.supply_stock import refresh_frame_consumption_month

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        # Fallback: read from .env.local (local dev)
        env_local = REPO_ROOT / ".env.local"
        if env_local.exists():
            for line in env_local.read_text().splitlines():
                if line.startswith("DATABASE_URL="):
                    db_url = line.split("=", 1)[1].strip()
                    break

    if not db_url:
        print("[09-stock] ERROR: DATABASE_URL not set", flush=True)
        return False

    ok = True
    with psycopg.connect(db_url) as conn:
        for fabricante in fabricantes:
            for mes in months:
                try:
                    print(f"[09-stock] DB refresh {fabricante}/{mes}...", flush=True)
                    refresh_frame_consumption_month(fabricante, mes, conn)
                    conn.commit()
                    print(f"[09-stock] DB refresh {fabricante}/{mes} OK", flush=True)
                except Exception as exc:
                    print(f"[09-stock] DB refresh {fabricante}/{mes} ERROR: {exc}", flush=True)
                    ok = False
    return ok


# ---------------------------------------------------------------------------
# Phase 2: upload Excels to Drive
# ---------------------------------------------------------------------------

def run_excel_upload(*, fabricantes: list[str], months: list[str]) -> bool:
    from lector_facturas.settings import load_settings
    from lector_facturas.pyg_sync import sync_stock_detail_to_drive

    settings = load_settings()
    ok = True

    for fabricante in fabricantes:
        for mes in months:
            try:
                print(f"[09-stock] Drive upload {fabricante}/{mes}...", flush=True)
                result = sync_stock_detail_to_drive(
                    settings=settings,
                    fabricante=fabricante,
                    mes_yyyymm=mes,
                )
                print(
                    f"[09-stock] Drive upload {fabricante}/{mes} OK "
                    f"| {result.drive_file_name}",
                    flush=True,
                )
            except Exception as exc:
                print(f"[09-stock] Drive upload {fabricante}/{mes} ERROR: {exc}", flush=True)
                ok = False

    return ok


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    timezone_name   = os.environ.get("STOCK_DETAIL_TIMEZONE", "Europe/Madrid")
    db_hour         = int(os.environ.get("STOCK_DETAIL_DB_HOUR", "7"))
    db_minute       = int(os.environ.get("STOCK_DETAIL_DB_MINUTE", "0"))
    excel_hour      = int(os.environ.get("STOCK_DETAIL_EXCEL_HOUR", "7"))
    excel_minute    = int(os.environ.get("STOCK_DETAIL_EXCEL_MINUTE", "30"))
    close_day       = int(os.environ.get("STOCK_DETAIL_CLOSE_DAY", "2"))
    fabricantes_env = os.environ.get("STOCK_DETAIL_FABRICANTES", "TGI,Proco")
    fabricantes     = [f.strip() for f in fabricantes_env.split(",") if f.strip()]

    timezone = ZoneInfo(timezone_name)
    print(
        f"[09-stock] started | tz={timezone_name} "
        f"| db={db_hour:02d}:{db_minute:02d} "
        f"| excel={excel_hour:02d}:{excel_minute:02d} "
        f"| close_day={close_day} "
        f"| fabricantes={fabricantes}",
        flush=True,
    )

    # Track which phases have run today so we don't double-fire
    last_db_run:    date | None = None
    last_excel_run: date | None = None
    consecutive_failures = 0

    while True:
        now   = datetime.now(tz=timezone)
        today = now.date()

        months = _months_to_process(today, close_day)

        ran_something = False

        # ---- Phase 1: DB refresh ----
        if last_db_run != today:
            db_run_time = now.replace(hour=db_hour, minute=db_minute, second=0, microsecond=0)
            if now >= db_run_time:
                print(f"[09-stock] Phase 1 DB refresh | months={months}", flush=True)
                ok = run_db_refresh(fabricantes=fabricantes, months=months)
                last_db_run = today
                ran_something = True
                if not ok:
                    consecutive_failures += 1

        # ---- Phase 2: Excel upload ----
        if last_excel_run != today:
            excel_run_time = now.replace(hour=excel_hour, minute=excel_minute, second=0, microsecond=0)
            if now >= excel_run_time:
                print(f"[09-stock] Phase 2 Excel upload | months={months}", flush=True)
                ok = run_excel_upload(fabricantes=fabricantes, months=months)
                last_excel_run = today
                ran_something = True
                if not ok:
                    consecutive_failures += 1
                else:
                    consecutive_failures = 0

        if consecutive_failures >= 3 and _HAS_ALERT:
            send_worker_failure_alert(
                worker_name="09-stock-detail",
                consecutive_failures=consecutive_failures,
            )

        # Sleep until the next phase trigger
        next_db    = _next_run_today_or_tomorrow(now=now, hour=db_hour,    minute=db_minute)
        next_excel = _next_run_today_or_tomorrow(now=now, hour=excel_hour, minute=excel_minute)
        next_wake  = min(next_db, next_excel)
        sleep_secs = max(30, int((next_wake - now).total_seconds()))

        if not ran_something:
            # Short poll so we don't overshoot the scheduled time
            sleep_secs = min(sleep_secs, 60)

        print(
            f"[09-stock] sleeping {sleep_secs}s "
            f"(next_db={next_db.strftime('%H:%M')}, next_excel={next_excel.strftime('%H:%M')})",
            flush=True,
        )
        time.sleep(sleep_secs)


if __name__ == "__main__":
    main()
