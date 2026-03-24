from __future__ import annotations

from datetime import datetime, timedelta
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo
import json
import os
import time

from lector_facturas.review_notifications import send_worker_failure_alert
from worker_coordination import begin_invoice_processing, end_invoice_processing


def compute_next_run(*, now: datetime, interval_minutes: int) -> datetime:
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
        "EMAIL_REVIEW_RUN_URL",
        "https://lector-facturas-api-dev.up.railway.app/jobs/email-download/run",
    )
    mailbox = os.environ.get("EMAIL_REVIEW_MAILBOX", "andrea@artestastore.com")
    sync_name = os.environ.get("EMAIL_REVIEW_SYNC_NAME", "revision-correo-principal")
    timezone_name = os.environ.get("EMAIL_REVIEW_TIMEZONE", "Europe/Madrid")
    interval_minutes = int(os.environ.get("EMAIL_REVIEW_INTERVAL_MINUTES", "30"))
    max_messages = int(os.environ.get("EMAIL_REVIEW_MAX_MESSAGES", "1000"))
    bearer_token = os.environ.get("API_SECRET_KEY", "").strip()

    timezone = ZoneInfo(timezone_name)
    print(
        f"[02-email-review] started | tz={timezone_name} | interval={interval_minutes}min "
        f"| mailbox={mailbox} | sync={sync_name}",
        flush=True,
    )
    consecutive_failures = 0
    while True:
        now = datetime.now(tz=timezone)
        next_run = compute_next_run(now=now, interval_minutes=interval_minutes)
        sleep_seconds = max(5, int((next_run - now).total_seconds()))
        print(
            f"[02-email-review] proxima ejecucion {next_run.strftime('%H:%M')} | durmiendo {sleep_seconds}s",
            flush=True,
        )
        time.sleep(sleep_seconds)
        ok = run_email_review(
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
                print(f"[02-email-review] CRITICO: {consecutive_failures} fallos consecutivos", flush=True)
                send_worker_failure_alert(worker_name="02-email-review", consecutive_failures=consecutive_failures)


def run_email_review(
    *,
    run_url: str,
    mailbox: str,
    sync_name: str,
    max_messages: int,
    timezone_name: str,
    bearer_token: str = "",
) -> bool:
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
    request = Request(run_url, data=payload, method="POST", headers=headers)
    started_at = datetime.now(tz=ZoneInfo(timezone_name))
    print(f"[02-email-review] >>> iniciando revision de correo ({mailbox})", flush=True)
    begin_invoice_processing()
    try:
        with urlopen(request, timeout=600) as response:
            body = response.read().decode("utf-8")
        print_run_summary(started_at=started_at, body=body)
        return True
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        print(f"[02-email-review] <<< ERROR {exc.code} | {detail}", flush=True)
        return False
    except Exception as exc:  # noqa: BLE001
        print(f"[02-email-review] <<< ERROR | {exc}", flush=True)
        return False
    finally:
        end_invoice_processing()


def print_run_summary(*, started_at: datetime, body: str) -> None:
    try:
        payload = json.loads(body)
    except Exception:
        print(f"[02-email-review] <<< ok | {body}", flush=True)
        return

    messages = payload.get("messages_scanned", 0)
    new = payload.get("new_attachments", 0)
    duplicates = payload.get("duplicate_attachments", 0)
    to_process = payload.get("sent_to_to_check", 0)
    no_invoice = payload.get("sent_to_no_invoice", 0)
    elapsed = (datetime.now(tz=started_at.tzinfo) - started_at).seconds

    if new == 0:
        print(f"[02-email-review] <<< ok ({elapsed}s) | {messages} emails, sin adjuntos nuevos", flush=True)
    else:
        print(
            f"[02-email-review] <<< ok ({elapsed}s) | {messages} emails | "
            f"{new} nuevos: {to_process} facturas a procesar, {no_invoice} no-factura, {duplicates} duplicados",
            flush=True,
        )


if __name__ == "__main__":
    main()
