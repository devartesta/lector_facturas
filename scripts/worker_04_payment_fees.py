from __future__ import annotations

from datetime import date, datetime, timedelta
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
        "PAYMENT_FEES_RUN_URL",
        "https://lector-facturas-api-dev.up.railway.app/integrations/payment-fees/sync",
    )
    timezone_name = os.environ.get("PAYMENT_FEES_TIMEZONE", "Europe/Madrid")
    run_hour = int(os.environ.get("PAYMENT_FEES_HOUR", "19"))
    run_minute = int(os.environ.get("PAYMENT_FEES_MINUTE", "0"))
    lookback_days = int(os.environ.get("PAYMENT_FEES_LOOKBACK_DAYS", "45"))
    platform = os.environ.get("PAYMENT_FEES_PLATFORM", "").strip() or None
    bearer_token = os.environ.get("API_SECRET_KEY", "").strip() or os.environ.get("PAYMENT_FEES_BEARER_TOKEN", "").strip()

    timezone = ZoneInfo(timezone_name)
    print(
        f"[04-payment-fees] started | tz={timezone_name} | run_at={run_hour:02d}:{run_minute:02d} "
        f"| lookback_days={lookback_days} | platform={platform or 'all'}",
        flush=True,
    )
    consecutive_failures = 0
    while True:
        now = datetime.now(tz=timezone)
        next_run = _next_daily_run(now=now, hour=run_hour, minute=run_minute)
        sleep_seconds = max(5, int((next_run - now).total_seconds()))
        print(
            f"[04-payment-fees] proxima ejecucion {next_run.isoformat()} | sleeping {sleep_seconds}s",
            flush=True,
        )
        time.sleep(sleep_seconds)
        ok = run_payment_fees_sync(
            run_url=run_url,
            timezone_name=timezone_name,
            lookback_days=lookback_days,
            platform=platform,
            bearer_token=bearer_token,
        )
        if ok:
            consecutive_failures = 0
        else:
            consecutive_failures += 1
            if consecutive_failures >= 3:
                print(f"[04-payment-fees] CRITICAL: {consecutive_failures} consecutive failures", flush=True)
                send_worker_failure_alert(worker_name="04-payment-fees", consecutive_failures=consecutive_failures)


def run_payment_fees_sync(
    *,
    run_url: str,
    timezone_name: str,
    lookback_days: int,
    platform: str | None,
    bearer_token: str,
) -> bool:
    local_today = datetime.now(tz=ZoneInfo(timezone_name)).date()
    date_to = local_today.isoformat()
    date_from = (local_today - timedelta(days=lookback_days)).isoformat()
    payload = {
        "date_from": date_from,
        "date_to": date_to,
    }
    if platform:
        payload["platform"] = platform
    request_headers = {"Content-Type": "application/json"}
    if bearer_token:
        request_headers["Authorization"] = f"Bearer {bearer_token}"
    request = Request(
        run_url,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers=request_headers,
    )
    print(f"[04-payment-fees] >>> sincronizando comisiones de pago ({date_from} → {date_to})", flush=True)
    started_at = datetime.now(tz=ZoneInfo(timezone_name))
    try:
        with urlopen(request, timeout=None) as response:
            body = response.read().decode("utf-8")
        print(
            f"[04-payment-fees] <<< ok | "
            f"from={date_from} | to={date_to} | body={body}",
            flush=True,
        )
        return True
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        print(
            f"[04-payment-fees] <<< ERROR | "
            f"from={date_from} | to={date_to} | status={exc.code} | detail={detail}",
            flush=True,
        )
        return False
    except Exception as exc:  # noqa: BLE001
        print(
            f"[04-payment-fees] <<< ERROR | "
            f"from={date_from} | to={date_to} | {exc}",
            flush=True,
        )
        return False


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


if __name__ == "__main__":
    main()
