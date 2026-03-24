from __future__ import annotations

from datetime import datetime, timedelta
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo
import os
import time

from lector_facturas.review_notifications import send_worker_failure_alert
from worker_coordination import begin_invoice_processing, end_invoice_processing


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


def _next_daily_run(*, now: datetime, hour: int, minute: int) -> datetime:
    candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= now:
        candidate += timedelta(days=1)
    return candidate


def main() -> None:
    run_url = os.environ.get(
        "INVOICE_PROCESSING_RUN_URL",
        "https://lector-facturas-api-dev.up.railway.app/jobs/invoice-processing/run",
    )
    timezone_name = os.environ.get("INVOICE_PROCESSING_TIMEZONE", "Europe/Madrid")
    run_hour = int(os.environ.get("INVOICE_PROCESSING_HOUR", "19"))
    run_minute = int(os.environ.get("INVOICE_PROCESSING_MINUTE", "0"))
    bearer_token = os.environ.get("API_SECRET_KEY", "").strip()
    timezone = ZoneInfo(timezone_name)
    print(
        f"[03-invoice-processing] started | tz={timezone_name} | run_at={run_hour:02d}:{run_minute:02d}",
        flush=True,
    )
    consecutive_failures = 0
    while True:
        now = datetime.now(tz=timezone)
        next_run = _next_daily_run(now=now, hour=run_hour, minute=run_minute)
        sleep_seconds = max(5, int((next_run - now).total_seconds()))
        print(f"[03-invoice-processing] proxima ejecucion {next_run.isoformat()} | sleeping {sleep_seconds}s", flush=True)
        time.sleep(sleep_seconds)
        ok = run_invoice_processing(run_url=run_url, timezone_name=timezone_name, bearer_token=bearer_token)
        if ok:
            consecutive_failures = 0
        else:
            consecutive_failures += 1
            if consecutive_failures >= 3:
                print(f"[03-invoice-processing] CRITICAL: {consecutive_failures} consecutive failures", flush=True)
                send_worker_failure_alert(worker_name="03-invoice-processing", consecutive_failures=consecutive_failures)


def run_invoice_processing(*, run_url: str, timezone_name: str, bearer_token: str = "") -> bool:
    headers: dict[str, str] = {}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    request = Request(run_url, data=b"", method="POST", headers=headers)
    started_at = datetime.now(tz=ZoneInfo(timezone_name))
    print(f"[03-invoice-processing] >>> iniciando procesamiento de facturas", flush=True)
    begin_invoice_processing()
    try:
        with urlopen(request, timeout=1800) as response:
            body = response.read().decode("utf-8")
        print(f"[03-invoice-processing] <<< ok {started_at.isoformat()} | body={body}", flush=True)
        return True
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        print(f"[03-invoice-processing] <<< ERROR {started_at.isoformat()} | status={exc.code} | detail={detail}", flush=True)
        return False
    except Exception as exc:  # noqa: BLE001
        print(f"[03-invoice-processing] <<< ERROR {started_at.isoformat()} | {exc}", flush=True)
        return False
    finally:
        end_invoice_processing()


if __name__ == "__main__":
    main()
