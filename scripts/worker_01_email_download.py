from __future__ import annotations

from datetime import datetime, timedelta
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo
import json
import os
import time

from lector_facturas.review_notifications import send_worker_failure_alert


def _next_interval(*, now: datetime, interval_minutes: int) -> datetime:
    candidate = now.replace(second=0, microsecond=0)
    minute_bucket = ((candidate.minute + interval_minutes - 1) // interval_minutes) * interval_minutes
    if minute_bucket >= 60:
        candidate = candidate.replace(minute=0) + timedelta(hours=1)
    else:
        candidate = candidate.replace(minute=minute_bucket)
    if candidate <= now:
        candidate += timedelta(minutes=interval_minutes)
    return candidate


def main() -> None:
    run_url = os.environ.get(
        "EMAIL_DOWNLOAD_RUN_URL",
        "https://lector-facturas-api-dev.up.railway.app/jobs/email-download/run",
    )
    mailbox = os.environ.get("EMAIL_REVIEW_MAILBOX", "andrea@artestastore.com")
    sync_name = os.environ.get("EMAIL_DOWNLOAD_SYNC_NAME", "revision-correo-principal")
    timezone_name = os.environ.get("EMAIL_DOWNLOAD_TIMEZONE", "Europe/Madrid")
    interval_minutes = int(os.environ.get("EMAIL_DOWNLOAD_INTERVAL_MINUTES", "30"))
    max_messages = int(os.environ.get("EMAIL_DOWNLOAD_MAX_MESSAGES", "1000"))
    bearer_token = os.environ.get("API_SECRET_KEY", "").strip()

    timezone = ZoneInfo(timezone_name)
    print(
        f"[01-email-download] started | tz={timezone_name} | interval_minutes={interval_minutes} "
        f"| mailbox={mailbox} | sync={sync_name}",
        flush=True,
    )
    consecutive_failures = 0
    while True:
        now = datetime.now(tz=timezone)
        next_run = _next_interval(now=now, interval_minutes=interval_minutes)
        sleep_seconds = max(5, int((next_run - now).total_seconds()))
        print(f"[01-email-download] proxima ejecucion {next_run.isoformat()} | sleeping {sleep_seconds}s", flush=True)
        time.sleep(sleep_seconds)
        ok = run_email_download(
            run_url=run_url,
            mailbox=mailbox,
            sync_name=sync_name,
            max_messages=max_messages,
            timezone_name=timezone_name,
            bearer_token=bearer_token,
        )
        if ok:
            consecutive_failures = 0
        else:
            consecutive_failures += 1
            if consecutive_failures >= 3:
                print(f"[01-email-download] CRITICAL: {consecutive_failures} consecutive failures", flush=True)
                send_worker_failure_alert(worker_name="01-email-download", consecutive_failures=consecutive_failures)


def run_email_download(*, run_url: str, mailbox: str, sync_name: str, max_messages: int, timezone_name: str, bearer_token: str = "") -> bool:
    payload = json.dumps(
        {
            "mailbox": mailbox,
            "sync_name": sync_name,
            "max_messages": max_messages,
            "update_checkpoint": True,
            "send_email": False,
        }
    ).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    print(f"[01-email-download] >>> descargando adjuntos de {mailbox}", flush=True)
    request = Request(run_url, data=payload, method="POST", headers=headers)
    started_at = datetime.now(tz=ZoneInfo(timezone_name))
    try:
        with urlopen(request, timeout=600) as response:
            body = response.read().decode("utf-8")
        print(f"[01-email-download] <<< ok {started_at.isoformat()} | body={body}", flush=True)
        return True
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        print(f"[01-email-download] <<< ERROR | status={exc.code} | detail={detail}", flush=True)
        return False
    except Exception as exc:  # noqa: BLE001
        print(f"[01-email-download] <<< ERROR | {exc}", flush=True)
        return False


if __name__ == "__main__":
    main()
