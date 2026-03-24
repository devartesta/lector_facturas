from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
import json
import os
import uuid

try:
    import psycopg
except ImportError:  # pragma: no cover
    psycopg = None


SCHEMA_NAME = "invoices"
TABLE_SQL = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};
CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.job_run_events (
    id TEXT PRIMARY KEY,
    job_name TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ NULL,
    status TEXT NOT NULL,
    summary_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
    error_text TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS invoices_job_run_events_job_started_idx
ON {SCHEMA_NAME}.job_run_events (job_name, started_at);
"""


@dataclass(frozen=True)
class JobRunEvent:
    id: str
    job_name: str
    started_at: datetime
    finished_at: datetime | None
    status: str
    summary: dict[str, Any]
    error_text: str


def _database_url() -> str:
    return os.environ.get("DATABASE_URL", "").strip()


def _ensure_ready() -> bool:
    if not _database_url() or psycopg is None:
        return False
    with psycopg.connect(_database_url()) as conn:
        conn.execute(TABLE_SQL)
        conn.commit()
    return True


def log_job_run(
    *,
    job_name: str,
    started_at: datetime,
    finished_at: datetime | None,
    status: str,
    summary: dict[str, Any] | None = None,
    error_text: str = "",
) -> None:
    if not _ensure_ready():
        return
    run_id = str(uuid.uuid4())
    with psycopg.connect(_database_url()) as conn:
        conn.execute(
            f"""
            INSERT INTO {SCHEMA_NAME}.job_run_events (
                id, job_name, started_at, finished_at, status, summary_json, error_text, created_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s::jsonb, %s, NOW()
            )
            """,
            (
                run_id,
                job_name,
                started_at,
                finished_at,
                status,
                json.dumps(summary or {}, ensure_ascii=False),
                error_text,
            ),
        )
        conn.commit()


def list_job_runs(*, from_at: datetime, to_at: datetime) -> list[JobRunEvent]:
    if not _ensure_ready():
        return []
    with psycopg.connect(_database_url()) as conn:
        rows = conn.execute(
            f"""
            SELECT id, job_name, started_at, finished_at, status, summary_json, error_text
            FROM {SCHEMA_NAME}.job_run_events
            WHERE started_at >= %s AND started_at <= %s
            ORDER BY started_at, created_at, id
            """,
            (from_at, to_at),
        ).fetchall()
    return [
        JobRunEvent(
            id=str(row[0]),
            job_name=str(row[1]),
            started_at=row[2],
            finished_at=row[3],
            status=str(row[4]),
            summary=row[5] or {},
            error_text=str(row[6] or ""),
        )
        for row in rows
    ]
