from __future__ import annotations

from datetime import datetime, timedelta
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo
import json
import os
import time

from lector_facturas.review_notifications import send_worker_failure_alert
from worker_coordination import wait_for_invoice_processing


def _next_daily_run(*, now: datetime, hour: int, minute: int) -> datetime:
    candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= now:
        candidate += timedelta(days=1)
    return candidate


def main() -> None:
    run_url = os.environ.get(
        "PYG_SL_RUN_URL",
        "https://lector-facturas-api-dev.up.railway.app/integrations/pyg/sl/sync",
    )
    timezone_name = os.environ.get("PYG_SL_TIMEZONE", "Europe/Madrid")
    run_hour = int(os.environ.get("PYG_SL_HOUR", "20"))
    run_minute = int(os.environ.get("PYG_SL_MINUTE", "0"))
    year = int(os.environ.get("PYG_SL_YEAR", "2026"))
    file_name = os.environ.get("PYG_SL_FILE_NAME", f"pyg_sl_{year}.xlsx").strip() or f"pyg_sl_{year}.xlsx"
    drive_folder_id = os.environ.get("PYG_SL_DRIVE_FOLDER_ID", "").strip()
    bearer_token = os.environ.get("API_SECRET_KEY", "").strip() or os.environ.get("PYG_SL_BEARER_TOKEN", "").strip()

    timezone = ZoneInfo(timezone_name)
    print(
        f"[05-pyg-sl] started | tz={timezone_name} | run_at={run_hour:02d}:{run_minute:02d} "
        f"| year={year} | file_name={file_name}",
        flush=True,
    )
    consecutive_failures = 0
    while True:
        now = datetime.now(tz=timezone)
        next_run = _next_daily_run(now=now, hour=run_hour, minute=run_minute)
        sleep_seconds = max(5, int((next_run - now).total_seconds()))
        print(
            f"[05-pyg-sl] proxima ejecucion {next_run.isoformat()} | sleeping {sleep_seconds}s",
            flush=True,
        )
        time.sleep(sleep_seconds)
        ok = run_pyg_sl_sync(
            run_url=run_url,
            year=year,
            file_name=file_name,
            drive_folder_id=drive_folder_id,
            bearer_token=bearer_token,
            timezone_name=timezone_name,
        )
        if ok:
            consecutive_failures = 0
        else:
            consecutive_failures += 1
            if consecutive_failures >= 3:
                print(f"[05-pyg-sl] CRITICAL: {consecutive_failures} consecutive failures", flush=True)
                send_worker_failure_alert(worker_name="05-pyg-sl", consecutive_failures=consecutive_failures)


def run_pyg_sl_sync(
    *,
    run_url: str,
    year: int,
    file_name: str,
    drive_folder_id: str,
    bearer_token: str,
    timezone_name: str,
) -> bool:
    wait_timeout_seconds = int(os.environ.get("PYG_SL_WAIT_FOR_INVOICES_TIMEOUT_SECONDS", "3600"))
    payload = {"year": year, "file_name": file_name}
    if drive_folder_id:
        payload["drive_folder_id"] = drive_folder_id
    request_headers = {"Content-Type": "application/json"}
    if bearer_token:
        request_headers["Authorization"] = f"Bearer {bearer_token}"
    request = Request(
        run_url,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers=request_headers,
    )
    started_at = datetime.now(tz=ZoneInfo(timezone_name))
    print(f"[05-pyg-sl] >>> generando PyG SL", flush=True)
    try:
        if not wait_for_invoice_processing(timeout_seconds=wait_timeout_seconds):
            print(
                f"[05-pyg-sl] <<< omitido (esperando facturas a procesar)",
                flush=True,
            )
            return True
        with urlopen(request, timeout=1800) as response:
            body = response.read().decode("utf-8")
        print(f"[05-pyg-sl] <<< ok | body={body}", flush=True)
        return True
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        print(
            f"[05-pyg-sl] <<< ERROR | "
            f"status={exc.code} | detail={detail}",
            flush=True,
        )
        return False
    except Exception as exc:  # noqa: BLE001
        print(f"[05-pyg-sl] <<< ERROR | {exc}", flush=True)
        return False


def _next_interval(*, now: datetime, interval_minutes: int) -> datetime:
    next_run = now.replace(second=0, microsecond=0) + timedelta(minutes=1)
    minute_bucket = ((next_run.minute + interval_minutes - 1) // interval_minutes) * interval_minutes
    if minute_bucket >= 60:
        next_run = (next_run + timedelta(hours=1)).replace(minute=0)
        minute_bucket = 0
    return next_run.replace(minute=minute_bucket)


if __name__ == "__main__":
    main()
