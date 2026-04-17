"""Hourly orchestrator — runs all pipeline steps sequentially via the API.

Steps (in order):
  1. email-download
  2. email-review
  3. invoice-processing
  4. stock-detail/Proco
  5. stock-detail/TGI
  6. sales-reports
  7. payment-fee-detail
  8. pyg

On failure of any step, logs the error and continues with the next step.
Email alerts are disabled by default here because a single transient HTTP 500
should not be treated as a worker outage.

Environment variables:
  API_BASE_URL          Base URL of the lector-facturas API  (required)
  API_SECRET_KEY        Bearer token for the API             (optional)
  EMAIL_REVIEW_MAILBOX  Mailbox for email-download/review    (default: andrea@artestastore.com)
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

REPO_ROOT = __import__("pathlib").Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

TIMEZONE = ZoneInfo("Europe/Madrid")


def _post(url: str, payload: dict | None, bearer_token: str, timeout: int = 600) -> tuple[bool, str]:
    """POST to url, return (ok, detail)."""
    body = json.dumps(payload).encode() if payload is not None else b""
    headers: dict[str, str] = {"Content-Type": "application/json"}
    if bearer_token:
        headers["Authorization"] = f"Bearer {bearer_token}"
    req = Request(url, data=body, method="POST", headers=headers)
    try:
        with urlopen(req, timeout=timeout) as resp:
            return True, resp.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        return False, f"HTTP {exc.code}: {detail}"
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def _summarise(step_name: str, body: str) -> str:
    """Parse JSON response body and return a compact volume summary line."""
    try:
        data = json.loads(body)
    except Exception:
        return ""

    if step_name in ("email-download", "email-review"):
        parts = [
            f"scanned={data.get('messages_scanned', '?')}",
            f"new={data.get('new_attachments', '?')}",
            f"dup={data.get('duplicate_attachments', '?')}",
            f"auto={data.get('auto_processed_attachments', '?')}",
            f"to_check={data.get('sent_to_to_check', '?')}",
            f"processed={data.get('processed_from_to_process', '?')}",
        ]
        return "  " + " | ".join(parts)

    if step_name == "invoice-processing":
        parts = [
            f"processed={data.get('processed_from_to_process', '?')}",
            f"returned={data.get('returned_to_to_check', '?')}",
            f"dup={data.get('ignored_duplicates', '?')}",
        ]
        return "  " + " | ".join(parts)

    if step_name.startswith("stock-detail/"):
        return f"  {data.get('drive_file_name', '')}"

    if step_name.startswith("sales-reports") or step_name.startswith("payment-fee-detail") or step_name == "pyg":
        results = data.get("results", [])
        lines = []
        for r in results:
            mark = "✓" if r.get("status") == "ok" else "✗"
            detail = r.get("detail", "")
            suffix = f"  {detail}" if detail and r.get("status") == "ok" else (f"  ERROR: {detail}" if detail else "")
            lines.append(f"    {mark} {r.get('step', '?')}{suffix}")
        return "\n" + "\n".join(lines) if lines else ""

    return ""


def main() -> None:
    api_base = os.environ.get("API_BASE_URL", "").rstrip("/")
    if not api_base:
        print("[hourly] ERROR: API_BASE_URL not set", flush=True)
        sys.exit(1)

    bearer_token = os.environ.get("API_SECRET_KEY", "").strip()
    mailbox = os.environ.get("EMAIL_REVIEW_MAILBOX", "andrea@artestastore.com")
    sync_name = os.environ.get("EMAIL_DOWNLOAD_SYNC_NAME", "revision-correo-principal")
    alerts_enabled = os.environ.get("HOURLY_FAILURE_ALERTS_ENABLED", "").strip().lower() in {"1", "true", "yes"}

    now = datetime.now(tz=TIMEZONE)
    mes_yyyymm = now.strftime("%Y%m")

    # Previous month (for consolidation during first 2 days of the month)
    if now.month == 1:
        prev_yyyymm = f"{now.year - 1}12"
    else:
        prev_yyyymm = f"{now.year}{now.month - 1:02d}"
    consolidate_prev = now.day <= 2

    steps = [
        ("email-download", f"{api_base}/jobs/email-download/run", {
            "mailbox": mailbox,
            "sync_name": sync_name,
            "max_messages": 500,
            "update_checkpoint": True,
            "send_email": False,
        }),
        ("email-review", f"{api_base}/jobs/email-review/run", {
            "mailbox": mailbox,
            "sync_name": sync_name,
            "max_messages": 500,
            "update_checkpoint": True,
            "send_email": False,
        }),
        ("invoice-processing", f"{api_base}/jobs/invoice-processing/run", None),
        ("stock-detail/Proco", f"{api_base}/supply/frame-stock-detail/sync?fabricante=Proco&mes_yyyymm={mes_yyyymm}", None),
        ("stock-detail/TGI",   f"{api_base}/supply/frame-stock-detail/sync?fabricante=TGI&mes_yyyymm={mes_yyyymm}", None),
        ("sales-reports",      f"{api_base}/jobs/sales-reports/run?period_yyyymm={mes_yyyymm}", None),
        ("payment-fee-detail", f"{api_base}/jobs/payment-fee-detail/run?period_yyyymm={mes_yyyymm}", None),
        ("pyg",                f"{api_base}/jobs/pyg/run", None),
    ]

    # During first 2 days of the month, also consolidate the previous month
    if consolidate_prev:
        steps += [
            (f"stock-detail/Proco [{prev_yyyymm}]", f"{api_base}/supply/frame-stock-detail/sync?fabricante=Proco&mes_yyyymm={prev_yyyymm}", None),
            (f"stock-detail/TGI [{prev_yyyymm}]",   f"{api_base}/supply/frame-stock-detail/sync?fabricante=TGI&mes_yyyymm={prev_yyyymm}", None),
            (f"sales-reports [{prev_yyyymm}]",       f"{api_base}/jobs/sales-reports/run?period_yyyymm={prev_yyyymm}", None),
            (f"payment-fee-detail [{prev_yyyymm}]", f"{api_base}/jobs/payment-fee-detail/run?period_yyyymm={prev_yyyymm}", None),
        ]

    started_at = datetime.now(tz=TIMEZONE)
    print(f"[hourly] === START {started_at.isoformat()} ===", flush=True)

    failures: list[str] = []

    for step_name, url, payload in steps:
        t0 = time.monotonic()
        print(f"[hourly] >>> {step_name} ...", flush=True)
        ok, detail = _post(url, payload, bearer_token)
        elapsed = time.monotonic() - t0
        if ok:
            summary = _summarise(step_name, detail)
            print(f"[hourly] <<< {step_name} OK ({elapsed:.1f}s){summary}", flush=True)
        else:
            print(f"[hourly] <<< {step_name} ERROR ({elapsed:.1f}s): {detail}", flush=True)
            failures.append(step_name)
            if alerts_enabled:
                from lector_facturas.review_notifications import send_worker_failure_alert  # noqa: PLC0415,E402

                send_worker_failure_alert(
                    worker_name=f"hourly/{step_name}",
                    consecutive_failures=1,
                    last_error=detail[:500],
                )

    finished_at = datetime.now(tz=TIMEZONE)
    total = (finished_at - started_at).total_seconds()
    status = "OK" if not failures else f"FAILED steps: {', '.join(failures)}"
    print(f"[hourly] === END {finished_at.isoformat()} | {total:.0f}s | {status} ===", flush=True)


if __name__ == "__main__":
    main()
