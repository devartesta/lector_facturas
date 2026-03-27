"""Worker 09 — Daily review email summary.

Runs once per day and sends the review email via the API.

Window logic
  Normal run  : from_at = now - 24 h  (always covers exactly yesterday+today)
  First run after a gap (service was down / redeployed): from_at = last_run_at
    so no hours are left unrecognised.
  Maximum lookback cap: DAILY_REVIEW_MAX_LOOKBACK_HOURS (default 72 h) —
    prevents sending a massive email after a very long outage.

Last-run timestamp is persisted in the DB table
  invoices.worker_last_run (worker_name TEXT PK, last_run_at TIMESTAMPTZ).
  Falls back to 24 h lookback if the DB is unavailable.

Environment variables:
  DAILY_REVIEW_EMAIL_RUN_URL      API endpoint (default: production URL)
  DAILY_REVIEW_EMAIL_HOUR         Run hour, local time (default: 8)
  DAILY_REVIEW_EMAIL_MINUTE       Run minute           (default: 0)
  DAILY_REVIEW_EMAIL_TIMEZONE     Timezone             (default: Europe/Madrid)
  DAILY_REVIEW_MAX_LOOKBACK_HOURS Max gap to recover   (default: 72)
  EMAIL_REVIEW_MAILBOX            Mailbox to summarise
  EMAIL_REVIEW_SYNC_NAME          Sync name
  API_SECRET_KEY                  Bearer token
  DATABASE_URL                    Postgres URL for last-run tracking
"""
from __future__ import annotations

from datetime import datetime, timedelta
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo
import json
import os
import time

from lector_facturas.review_notifications import send_worker_failure_alert

WORKER_NAME = "09-daily-summary"
_CREATE_TABLE_SQL = """
CREATE SCHEMA IF NOT EXISTS invoices;
CREATE TABLE IF NOT EXISTS invoices.worker_last_run (
    worker_name  TEXT PRIMARY KEY,
    last_run_at  TIMESTAMPTZ NOT NULL
);
"""


# ---------------------------------------------------------------------------
# Last-run persistence
# ---------------------------------------------------------------------------

def _db_url() -> str:
    return os.environ.get("DATABASE_URL", "").strip()


def _load_last_run(worker_name: str) -> datetime | None:
    db = _db_url()
    if not db:
        return None
    try:
        import psycopg
        with psycopg.connect(db) as conn:
            conn.execute(_CREATE_TABLE_SQL)
            row = conn.execute(
                "SELECT last_run_at FROM invoices.worker_last_run WHERE worker_name = %s",
                (worker_name,),
            ).fetchone()
            conn.commit()
            return row[0] if row else None
    except Exception as exc:
        print(f"[{WORKER_NAME}] WARNING: could not read last_run from DB: {exc}", flush=True)
        return None


def _save_last_run(worker_name: str, ran_at: datetime) -> None:
    db = _db_url()
    if not db:
        return
    try:
        import psycopg
        with psycopg.connect(db) as conn:
            conn.execute(_CREATE_TABLE_SQL)
            conn.execute(
                """
                INSERT INTO invoices.worker_last_run (worker_name, last_run_at)
                VALUES (%s, %s)
                ON CONFLICT (worker_name) DO UPDATE SET last_run_at = EXCLUDED.last_run_at
                """,
                (worker_name, ran_at),
            )
            conn.commit()
    except Exception as exc:
        print(f"[{WORKER_NAME}] WARNING: could not save last_run to DB: {exc}", flush=True)


# ---------------------------------------------------------------------------
# Window calculation
# ---------------------------------------------------------------------------

def _compute_from_at(
    *,
    now: datetime,
    last_run_at: datetime | None,
    max_lookback_hours: int,
) -> datetime:
    """Return the from_at for this run.

    - No previous run recorded → last 24 h
    - Gap since last run ≤ 25 h → last 24 h  (normal daily cadence)
    - Gap > 25 h (service was down) → from last_run_at, capped at max_lookback_hours
    """
    default = now - timedelta(hours=24)

    if last_run_at is None:
        return default

    gap_hours = (now - last_run_at).total_seconds() / 3600

    if gap_hours <= 25:
        # Normal cadence — just last 24 h
        return default

    # Gap detected — recover from last_run_at but cap at max
    cap = now - timedelta(hours=max_lookback_hours)
    from_at = max(last_run_at, cap)
    print(
        f"[{WORKER_NAME}] Gap detected ({gap_hours:.1f}h since last run). "
        f"Recovering from {from_at.isoformat()} (cap={max_lookback_hours}h).",
        flush=True,
    )
    return from_at


# ---------------------------------------------------------------------------
# Scheduler helper
# ---------------------------------------------------------------------------

def _next_daily_run(*, now: datetime, hour: int, minute: int) -> datetime:
    candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if candidate <= now:
        candidate += timedelta(days=1)
    return candidate


# ---------------------------------------------------------------------------
# API call
# ---------------------------------------------------------------------------

def run_daily_review_email(
    *,
    run_url: str,
    mailbox: str,
    sync_name: str,
    timezone_name: str,
    from_at: datetime,
    to_at: datetime,
    bearer_token: str = "",
) -> bool:
    payload = json.dumps(
        {
            "mailbox": mailbox,
            "sync_name": sync_name,
            "from_at": from_at.isoformat(),
            "to_at": to_at.isoformat(),
            "send_email": True,
        }
    ).encode("utf-8")
    headers = {"Content-Type": "application/json"}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    request = Request(run_url, data=payload, method="POST", headers=headers)
    print(
        f"[{WORKER_NAME}] >>> sending summary | from={from_at.isoformat()} to={to_at.isoformat()}",
        flush=True,
    )
    try:
        with urlopen(request, timeout=600) as response:
            body = response.read().decode("utf-8")
        print(f"[{WORKER_NAME}] <<< ok | body={body}", flush=True)
        return True
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        print(f"[{WORKER_NAME}] <<< ERROR | status={exc.code} | detail={detail}", flush=True)
        return False
    except Exception as exc:  # noqa: BLE001
        print(f"[{WORKER_NAME}] <<< ERROR | {exc}", flush=True)
        return False


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    run_url = os.environ.get(
        "DAILY_REVIEW_EMAIL_RUN_URL",
        "https://api-production-a76b.up.railway.app/jobs/daily-review-email/run",
    )
    mailbox = os.environ.get("EMAIL_REVIEW_MAILBOX", "andrea@artestastore.com")
    sync_name = os.environ.get("EMAIL_REVIEW_SYNC_NAME", "revision-correo-principal")
    timezone_name = os.environ.get("DAILY_REVIEW_EMAIL_TIMEZONE", "Europe/Madrid")
    run_hour = int(os.environ.get("DAILY_REVIEW_EMAIL_HOUR", "8"))
    run_minute = int(os.environ.get("DAILY_REVIEW_EMAIL_MINUTE", "0"))
    max_lookback = int(os.environ.get("DAILY_REVIEW_MAX_LOOKBACK_HOURS", "72"))
    bearer_token = os.environ.get("API_SECRET_KEY", "").strip()

    timezone = ZoneInfo(timezone_name)
    print(
        f"[{WORKER_NAME}] started | tz={timezone_name} "
        f"| run_at={run_hour:02d}:{run_minute:02d} "
        f"| max_lookback={max_lookback}h",
        flush=True,
    )

    consecutive_failures = 0
    while True:
        now = datetime.now(tz=timezone)
        next_run = _next_daily_run(now=now, hour=run_hour, minute=run_minute)
        sleep_seconds = max(5, int((next_run - now).total_seconds()))
        print(
            f"[{WORKER_NAME}] next run {next_run.isoformat()} | sleeping {sleep_seconds}s",
            flush=True,
        )
        time.sleep(sleep_seconds)

        now = datetime.now(tz=timezone)
        last_run_at = _load_last_run(WORKER_NAME)
        from_at = _compute_from_at(
            now=now,
            last_run_at=last_run_at,
            max_lookback_hours=max_lookback,
        )

        ok = run_daily_review_email(
            run_url=run_url,
            mailbox=mailbox,
            sync_name=sync_name,
            timezone_name=timezone_name,
            from_at=from_at,
            to_at=now,
            bearer_token=bearer_token,
        )

        if ok:
            _save_last_run(WORKER_NAME, now)
            consecutive_failures = 0
        else:
            consecutive_failures += 1
            if consecutive_failures >= 3:
                print(
                    f"[{WORKER_NAME}] CRITICAL: {consecutive_failures} consecutive failures",
                    flush=True,
                )
                send_worker_failure_alert(
                    worker_name=WORKER_NAME,
                    consecutive_failures=consecutive_failures,
                )


if __name__ == "__main__":
    main()
