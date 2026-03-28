"""Worker 11 — Daily sales & payment reports via API.

Deployed as Railway service ``lf-10-sales-report``.

Calls the lector_facturas API to regenerate two reports for each company
every day at a configurable time:

  POST /supply/gestoria/sync               → shopify_sales_{company}_{yyyymm}.xlsx
  POST /supply/payment-reconciliation/sync → payment_reconciliation_{company}_{yyyymm}.xlsx

Both reports are built for the current month.  During the first CLOSE_DAY
days of a new month (default 2) the previous month is also regenerated so
that late-arriving data and payment settlements are captured.  After
CLOSE_DAY the previous month is considered closed and only the current
month is updated.

Example timeline (CLOSE_DAY=2)
--------------------------------
  Apr 01 → regenerates Mar + Apr
  Apr 02 → regenerates Mar + Apr   (last regeneration of March)
  Apr 03 → regenerates Apr only
  ...
  May 01 → regenerates Apr + May
  May 02 → regenerates Apr + May   (last regeneration of April)
  May 03 → regenerates May only

Schedule
--------
Runs once daily at REPORTS_HOUR:REPORTS_MINUTE local time.
Deployed with default 08:00 Europe/Madrid so files are fresh before the
business day starts.

Uses only stdlib (``urllib.request``) — no third-party HTTP dependency.

Environment variables
---------------------
API_BASE_URL       Base URL of the lector_facturas API  (required)
                   e.g. https://lector-facturas-api-dev.up.railway.app
API_KEY            Bearer token if the API requires auth (optional)
REPORTS_HOUR       Run hour, local time                 (default: 8)
REPORTS_MINUTE     Run minute, local time               (default: 0)
REPORTS_CLOSE_DAY  Regenerate prev month until this day (default: 2)
REPORTS_TIMEZONE   Timezone name                        (default: Europe/Madrid)
REPORTS_COMPANIES  Comma-separated company codes        (default: SL,LTD,INC)

Alerts
------
If 3 or more consecutive daily runs fail entirely, calls
``send_worker_failure_alert`` (requires lector_facturas package on path).
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

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
# HTTP helpers
# ---------------------------------------------------------------------------

def _api_post(base_url: str, path: str, body: dict, api_key: str | None) -> dict:
    """POST JSON to the API and return the parsed response dict."""
    url = urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))
    data = json.dumps(body).encode("utf-8")
    headers: dict[str, str] = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    req = Request(url, data=data, headers=headers, method="POST")
    try:
        with urlopen(req, timeout=300) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"API {path} → HTTP {exc.code}: {detail}") from exc
    except URLError as exc:
        raise RuntimeError(f"API {path} → connection error: {exc.reason}") from exc


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _prev_month(year: int, month: int) -> tuple[int, int]:
    if month == 1:
        return year - 1, 12
    return year, month - 1


def _months_to_process(ref: date, close_day: int) -> list[str]:
    current = f"{ref.year}{ref.month:02d}"
    py, pm  = _prev_month(ref.year, ref.month)
    previous = f"{py}{pm:02d}"
    if ref.day <= close_day:
        return [previous, current]
    return [current]


def _next_run(*, now: datetime, hour: int, minute: int) -> datetime:
    candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= now:
        candidate += timedelta(days=1)
    return candidate


# ---------------------------------------------------------------------------
# Run logic
# ---------------------------------------------------------------------------

ENDPOINTS = [
    ("/supply/gestoria/sync",               "gestoria"),
    ("/supply/payment-reconciliation/sync", "recon"),
]


def run_reports(
    *,
    base_url: str,
    api_key: str | None,
    companies: list[str],
    months: list[str],
) -> bool:
    ok = True
    for company in companies:
        for period in months:
            for path, label in ENDPOINTS:
                tag = f"[11] {label} {company}/{period}"
                try:
                    print(f"{tag} → calling {path} ...", flush=True)
                    result = _api_post(
                        base_url, path,
                        {"company_code": company, "period_yyyymm": period},
                        api_key,
                    )
                    file_name = result.get("drive_file_name", "?")
                    file_url  = result.get("drive_file_url",  "?")
                    print(f"{tag} ✓  {file_name}  {file_url}", flush=True)
                except Exception as exc:
                    print(f"{tag} ✗  {exc}", flush=True)
                    ok = False
    return ok


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    base_url       = os.environ.get("API_BASE_URL", "").strip()
    api_key        = os.environ.get("API_KEY",       "").strip() or None
    timezone_name  = os.environ.get("REPORTS_TIMEZONE",  "Europe/Madrid")
    hour           = int(os.environ.get("REPORTS_HOUR",       "6"))
    minute         = int(os.environ.get("REPORTS_MINUTE",     "0"))
    close_day      = int(os.environ.get("REPORTS_CLOSE_DAY",  "3"))
    companies_env  = os.environ.get("REPORTS_COMPANIES", "SL,LTD,INC")
    companies      = [c.strip() for c in companies_env.split(",") if c.strip()]

    if not base_url:
        print("[11] ERROR: API_BASE_URL is not set.", flush=True)
        sys.exit(1)

    timezone = ZoneInfo(timezone_name)
    print(
        f"[11] started | api={base_url} | tz={timezone_name} "
        f"| run={hour:02d}:{minute:02d} "
        f"| close_day={close_day} "
        f"| companies={companies}",
        flush=True,
    )

    last_run: date | None = None
    consecutive_failures  = 0

    while True:
        now      = datetime.now(tz=timezone)
        today    = now.date()
        run_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

        if last_run != today and now >= run_time:
            months = _months_to_process(today, close_day)
            print(f"[11] running | months={months}", flush=True)
            ok = run_reports(
                base_url=base_url,
                api_key=api_key,
                companies=companies,
                months=months,
            )
            last_run = today
            if ok:
                consecutive_failures = 0
                print("[11] all reports done ✓", flush=True)
            else:
                consecutive_failures += 1
                print(f"[11] some reports failed (streak={consecutive_failures})", flush=True)

            if consecutive_failures >= 3 and _HAS_ALERT:
                send_worker_failure_alert(
                    worker_name="11-daily-reports",
                    consecutive_failures=consecutive_failures,
                )

        next_run_dt = _next_run(now=now, hour=hour, minute=minute)
        sleep_secs  = max(30, int((next_run_dt - now).total_seconds()))
        if last_run != today:
            sleep_secs = min(sleep_secs, 60)

        print(
            f"[11] sleeping {sleep_secs}s "
            f"(next={next_run_dt.strftime('%H:%M')})",
            flush=True,
        )
        time.sleep(sleep_secs)


if __name__ == "__main__":
    main()
