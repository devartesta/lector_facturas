from __future__ import annotations

from datetime import UTC, datetime, timedelta
from threading import Event, Lock
import os
import time

try:
    import psycopg
except ImportError:  # pragma: no cover
    psycopg = None


_invoice_processing_lock = Lock()
_invoice_processing_idle = Event()
_invoice_processing_idle.set()
_TABLE_READY = False
_STATE_TABLE_SQL = """
CREATE SCHEMA IF NOT EXISTS invoices;
CREATE TABLE IF NOT EXISTS invoices.worker_coordination (
    job_name TEXT PRIMARY KEY,
    is_running BOOLEAN NOT NULL DEFAULT FALSE,
    started_at TIMESTAMPTZ NULL,
    heartbeat_at TIMESTAMPTZ NULL,
    finished_at TIMESTAMPTZ NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


def _database_url() -> str:
    return os.environ.get("DATABASE_URL", "").strip()


def _can_use_db() -> bool:
    return bool(_database_url() and psycopg is not None)


def _ensure_table() -> bool:
    global _TABLE_READY
    if _TABLE_READY:
        return True
    if not _can_use_db():
        return False
    with psycopg.connect(_database_url()) as conn:
        conn.execute(_STATE_TABLE_SQL)
        conn.commit()
    _TABLE_READY = True
    return True


def begin_invoice_processing() -> None:
    if _ensure_table():
        with psycopg.connect(_database_url()) as conn:
            conn.execute(
                """
                INSERT INTO invoices.worker_coordination (
                    job_name, is_running, started_at, heartbeat_at, finished_at, updated_at
                ) VALUES (
                    'invoice_processing', TRUE, NOW(), NOW(), NULL, NOW()
                )
                ON CONFLICT (job_name) DO UPDATE SET
                    is_running = TRUE,
                    started_at = NOW(),
                    heartbeat_at = NOW(),
                    finished_at = NULL,
                    updated_at = NOW()
                """
            )
            conn.commit()
        return
    _invoice_processing_lock.acquire()
    _invoice_processing_idle.clear()


def end_invoice_processing() -> None:
    if _ensure_table():
        with psycopg.connect(_database_url()) as conn:
            conn.execute(
                """
                INSERT INTO invoices.worker_coordination (
                    job_name, is_running, started_at, heartbeat_at, finished_at, updated_at
                ) VALUES (
                    'invoice_processing', FALSE, NULL, NOW(), NOW(), NOW()
                )
                ON CONFLICT (job_name) DO UPDATE SET
                    is_running = FALSE,
                    heartbeat_at = NOW(),
                    finished_at = NOW(),
                    updated_at = NOW()
                """
            )
            conn.commit()
        return
    _invoice_processing_idle.set()
    _invoice_processing_lock.release()


def wait_for_invoice_processing(*, timeout_seconds: float | None = None) -> bool:
    if _ensure_table():
        stale_after_seconds = int(os.environ.get("INVOICE_PROCESSING_STALE_SECONDS", "14400"))
        started = time.monotonic()
        while True:
            with psycopg.connect(_database_url()) as conn:
                row = conn.execute(
                    """
                    SELECT is_running, heartbeat_at, started_at
                    FROM invoices.worker_coordination
                    WHERE job_name = 'invoice_processing'
                    """
                ).fetchone()
            if row is None:
                return True
            is_running = bool(row[0])
            heartbeat_at = row[1]
            started_at = row[2]
            reference = heartbeat_at or started_at
            stale = False
            if reference is not None:
                stale = (datetime.now(tz=UTC) - reference) > timedelta(seconds=stale_after_seconds)
            if (not is_running) or stale:
                return True
            if timeout_seconds is not None and (time.monotonic() - started) >= timeout_seconds:
                return False
            time.sleep(5)
    return _invoice_processing_idle.wait(timeout=timeout_seconds)
