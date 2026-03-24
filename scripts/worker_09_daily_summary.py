from __future__ import annotations

from datetime import datetime, time as dt_time, timedelta
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo
import json
import os
import time

from lector_facturas.review_notifications import send_worker_failure_alert


def _next_daily_run(*, now: datetime, hour: int, minute: int) -> datetime:
    candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= now:
        candidate += timedelta(days=1)
    return candidate


def main() -> None:
    run_url = os.environ.get(
        "DAILY_REVIEW_EMAIL_RUN_URL",
        "https://lector-facturas-api-dev.up.railway.app/jobs/daily-review-email/run",
    )
    mailbox = os.environ.get("EMAIL_REVIEW_MAILBOX", "andrea@artestastore.com")
    sync_name = os.environ.get("EMAIL_REVIEW_SYNC_NAME", "revision-correo-principal")
    timezone_name = os.environ.get("DAILY_REVIEW_EMAIL_TIMEZONE", "Europe/Madrid")
    run_hour = int(os.environ.get("DAILY_REVIEW_EMAIL_HOUR", "20"))
    run_minute = int(os.environ.get("DAILY_REVIEW_EMAIL_MINUTE", "0"))
    bearer_token = os.environ.get("API_SECRET_KEY", "").strip()

    timezone = ZoneInfo(timezone_name)
    print(
        f"[09-daily-summary] started | tz={timezone_name} | run_at={run_hour:02d}:{run_minute:02d} | mailbox={mailbox}",
        flush=True,
    )
    consecutive_failures = 0
    while True:
        now = datetime.now(tz=timezone)
        next_run = _next_daily_run(now=now, hour=run_hour, minute=run_minute)
        sleep_seconds = max(5, int((next_run - now).total_seconds()))
        print(f"[09-daily-summary] proxima ejecucion {next_run.isoformat()} | sleeping {sleep_seconds}s", flush=True)
        time.sleep(sleep_seconds)
        ok = run_daily_review_email(
            run_url=run_url,
            mailbox=mailbox,
            sync_name=sync_name,
            timezone_name=timezone_name,
            bearer_token=bearer_token,
        )
        if ok:
            consecutive_failures = 0
        else:
            consecutive_failures += 1
            if consecutive_failures >= 3:
                print(f"[09-daily-summary] CRITICAL: {consecutive_failures} consecutive failures", flush=True)
                send_worker_failure_alert(worker_name="09-daily-summary", consecutive_failures=consecutive_failures)


def run_daily_review_email(*, run_url: str, mailbox: str, sync_name: str, timezone_name: str, bearer_token: str = "") -> bool:
    timezone = ZoneInfo(timezone_name)
    local_now = datetime.now(tz=timezone)
    local_start = datetime.combine(local_now.date(), dt_time.min, tzinfo=timezone)
    payload = json.dumps(
        {
            "mailbox": mailbox,
            "sync_name": sync_name,
            "from_at": local_start.isoformat(),
            "to_at": local_now.isoformat(),
            "send_email": True,
        }
    ).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    request = Request(run_url, data=payload, method="POST", headers=headers)
    print(f"[09-daily-summary] >>> generando email resumen diario", flush=True)
    started_at = datetime.now(tz=timezone)
    try:
        with urlopen(request, timeout=600) as response:
            body = response.read().decode("utf-8")
        print(f"[09-daily-summary] <<< ok | body={body}", flush=True)
        return True
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        print(f"[09-daily-summary] <<< ERROR | status={exc.code} | detail={detail}", flush=True)
        return False
    except Exception as exc:  # noqa: BLE001
        print(f"[09-daily-summary] <<< ERROR | {exc}", flush=True)
        return False


if __name__ == "__main__":
    main()
