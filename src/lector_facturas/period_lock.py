"""Immutable sales-period snapshots used by accounting reports.

The source tables are intentionally mutable while a month is being prepared.
Once a period is frozen, reports read the stored canonical detail and the
database rejects changes to the monthly sales tables.  There is deliberately
no unlock helper: reopening a closed period is an accounting decision that
must be performed manually by an administrator after a controlled review.
"""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any

try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg.types.json import Jsonb
except ImportError:  # pragma: no cover
    psycopg = None  # type: ignore[assignment]
    dict_row = None  # type: ignore[assignment]
    Jsonb = None  # type: ignore[assignment,misc]


LOCK_TABLE = "finance.sales_period_freezes"
SCHEMA_LOCK_NAME = "finance.sales_period_freezes.schema"


def _ensure_period_lock_store(conn: Any) -> None:
    """Create the freeze table only when it is genuinely absent.

    Reports call this helper on their read path. Avoiding repeated
    ``CREATE OR REPLACE`` statements keeps concurrent PYG requests from
    contending on PostgreSQL's system catalog.
    """
    existing = conn.execute("SELECT to_regclass(%s) AS table_name", (LOCK_TABLE,)).fetchone()
    if existing and existing["table_name"]:
        return

    conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (SCHEMA_LOCK_NAME,))
    existing = conn.execute("SELECT to_regclass(%s) AS table_name", (LOCK_TABLE,)).fetchone()
    if existing and existing["table_name"]:
        return

    conn.execute("CREATE SCHEMA IF NOT EXISTS finance")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS finance.sales_period_freezes (
            company_code TEXT NOT NULL,
            period_yyyymm TEXT NOT NULL,
            frozen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            frozen_by TEXT NOT NULL DEFAULT 'system',
            source_hash TEXT NOT NULL,
            detail_rows JSONB NOT NULL,
            totals JSONB NOT NULL DEFAULT '{}'::jsonb,
            PRIMARY KEY (company_code, period_yyyymm),
            CHECK (period_yyyymm ~ '^[0-9]{6}$')
        )
        """
    )


def ensure_period_lock_schema(conn: Any) -> None:
    """Create the lock store and the database-level guard function."""
    conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (SCHEMA_LOCK_NAME,))
    _ensure_period_lock_store(conn)
    conn.execute(
        """
        CREATE OR REPLACE FUNCTION finance.reject_frozen_sales_period_change()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        DECLARE
            payload JSONB;
            period TEXT;
            company TEXT;
        BEGIN
            payload := CASE WHEN TG_OP = 'DELETE' THEN to_jsonb(OLD) ELSE to_jsonb(NEW) END;
            period := payload ->> 'order_month_yyyymm';
            IF period IS NULL AND TG_TABLE_SCHEMA = 'shopify' THEN
                period := substring(TG_TABLE_NAME FROM 'ventas_([0-9]{6})$');
            END IF;
            company := CASE WHEN TG_TABLE_SCHEMA = 'shopify' THEN 'SL' ELSE 'SL' END;
            IF period IS NOT NULL AND EXISTS (
                SELECT 1
                FROM finance.sales_period_freezes f
                WHERE f.company_code = company
                  AND f.period_yyyymm = period
            ) THEN
                RAISE EXCEPTION 'Sales period %/% is frozen and immutable', company, period
                    USING ERRCODE = '55000';
            END IF;
            RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
        END;
        $$
        """
    )
    conn.execute(
        """
        CREATE OR REPLACE FUNCTION finance.reject_frozen_pyg_adjustment_change()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            -- Manual accounting corrections must opt in explicitly. No scheduled
            -- importer or report regeneration sets this session flag.
            IF current_setting('finance.manual_pyg_override', true) = 'on' THEN
                RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
            END IF;

            IF TG_OP IN ('UPDATE', 'DELETE') AND EXISTS (
                SELECT 1
                FROM finance.sales_period_freezes f
                WHERE f.company_code = upper(OLD.company_code)
                  AND f.period_yyyymm = OLD.period_yyyymm
            ) THEN
                RAISE EXCEPTION 'PYG adjustment %/% is frozen and immutable', upper(OLD.company_code), OLD.period_yyyymm
                    USING ERRCODE = '55000';
            END IF;

            IF TG_OP IN ('INSERT', 'UPDATE') AND EXISTS (
                SELECT 1
                FROM finance.sales_period_freezes f
                WHERE f.company_code = upper(NEW.company_code)
                  AND f.period_yyyymm = NEW.period_yyyymm
            ) THEN
                RAISE EXCEPTION 'PYG adjustment %/% is frozen and immutable', upper(NEW.company_code), NEW.period_yyyymm
                    USING ERRCODE = '55000';
            END IF;

            RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
        END;
        $$
        """
    )
    _install_pyg_adjustment_guard(conn)


def is_period_frozen(conn: Any, *, company_code: str, period_yyyymm: str) -> bool:
    _ensure_period_lock_store(conn)
    row = conn.execute(
        f"""
        SELECT 1 FROM {LOCK_TABLE}
        WHERE company_code = %(company)s AND period_yyyymm = %(period)s
        """,
        {"company": company_code.upper(), "period": period_yyyymm},
    ).fetchone()
    return row is not None


def get_period_freeze(conn: Any, *, company_code: str, period_yyyymm: str) -> dict[str, Any] | None:
    _ensure_period_lock_store(conn)
    row = conn.execute(
        f"""
        SELECT company_code, period_yyyymm, frozen_at, frozen_by, source_hash,
               detail_rows, totals
        FROM {LOCK_TABLE}
        WHERE company_code = %(company)s AND period_yyyymm = %(period)s
        """,
        {"company": company_code.upper(), "period": period_yyyymm},
    ).fetchone()
    if not row:
        return None
    result = dict(row)
    result["detail_rows"] = _decode_json(result.get("detail_rows"))
    result["totals"] = _decode_json(result.get("totals"))
    return result


def frozen_periods(conn: Any, *, company_code: str, year: int) -> set[str]:
    _ensure_period_lock_store(conn)
    rows = conn.execute(
        f"""
        SELECT period_yyyymm FROM {LOCK_TABLE}
        WHERE company_code = %(company)s AND period_yyyymm LIKE %(period)s
        """,
        {"company": company_code.upper(), "period": f"{year}%"},
    ).fetchall()
    return {str(row["period_yyyymm"]) for row in rows}


def freeze_sales_period(
    *,
    database_url: str,
    company_code: str,
    period_yyyymm: str,
    frozen_by: str = "system",
) -> dict[str, Any]:
    """Persist the already validated sales report as the period truth."""
    if psycopg is None or Jsonb is None:
        raise RuntimeError("psycopg is not installed.")
    if not period_yyyymm.isdigit() or len(period_yyyymm) != 6:
        raise ValueError("period_yyyymm must be exactly six digits")
    company = company_code.upper()
    if company != "SL":
        raise ValueError("Sales period freezing is currently enabled for SL only")

    # Import lazily to avoid a module cycle during report imports.
    from lector_facturas.gestoria_workbook import collect_gestoria_data

    report = collect_gestoria_data(
        database_url=database_url,
        company_code=company,
        period_yyyymm=period_yyyymm,
    )
    detail_rows = [_jsonable(row) for row in report.detalle_rows]
    canonical = json.dumps(detail_rows, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    source_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    totals = {
        "orders": len(detail_rows),
        "gross_eur": str(sum((_decimal(row.get("shown_gross_presentment")) for row in report.detalle_rows), Decimal("0")).quantize(Decimal("0.01"))),
        "tax_eur": str(sum((_decimal(row.get("shown_tax_presentment")) for row in report.detalle_rows), Decimal("0")).quantize(Decimal("0.01"))),
        "net_eur": str(sum((_decimal(row.get("shown_net_presentment")) for row in report.detalle_rows), Decimal("0")).quantize(Decimal("0.01"))),
    }

    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        ensure_period_lock_schema(conn)
        existing = conn.execute(
            f"SELECT source_hash, frozen_at FROM {LOCK_TABLE} WHERE company_code = %s AND period_yyyymm = %s",
            (company, period_yyyymm),
        ).fetchone()
        if existing:
            raise RuntimeError(
                f"Sales period {company}/{period_yyyymm} is already frozen "
                f"(hash {existing['source_hash']}). No overwrite is allowed."
            )
        conn.execute(
            f"""
            INSERT INTO {LOCK_TABLE}
                (company_code, period_yyyymm, frozen_by, source_hash, detail_rows, totals)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (company, period_yyyymm, frozen_by, source_hash, Jsonb(detail_rows), Jsonb(totals)),
        )
        _install_guards(conn, period_yyyymm)
        conn.commit()

    return {
        "company_code": company,
        "period_yyyymm": period_yyyymm,
        "source_hash": source_hash,
        "frozen_at": datetime.now().astimezone().isoformat(),
        "totals": totals,
    }


def _install_guards(conn: Any, period_yyyymm: str) -> None:
    """Guard the monthly sources used by PYG, VAT and reconciliation."""
    tables = (
        f"shopify.ventas_{period_yyyymm}",
        f"finance.informe_vat_gestorias_detalle_{period_yyyymm}",
        f"finance.informe_vat_gestorias_resumen_{period_yyyymm}",
    )
    for table in tables:
        schema, name = table.split(".", 1)
        exists = conn.execute(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, name),
        ).fetchone()
        if not exists:
            continue
        trigger_name = f"sales_period_freeze_guard_{period_yyyymm}"
        conn.execute(f"DROP TRIGGER IF EXISTS {trigger_name} ON {table}")
        conn.execute(
            f"""
            CREATE TRIGGER {trigger_name}
            BEFORE INSERT OR UPDATE OR DELETE ON {table}
            FOR EACH ROW EXECUTE FUNCTION finance.reject_frozen_sales_period_change()
            """
        )


def _install_pyg_adjustment_guard(conn: Any) -> None:
    """Protect manual PYG adjustments for already frozen sales periods."""
    exists = conn.execute(
        """
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'invoices' AND table_name = 'diferencias_divisas'
        """
    ).fetchone()
    if not exists:
        return
    conn.execute("DROP TRIGGER IF EXISTS pyg_adjustment_freeze_guard ON invoices.diferencias_divisas")
    conn.execute(
        """
        CREATE TRIGGER pyg_adjustment_freeze_guard
        BEFORE INSERT OR UPDATE OR DELETE ON invoices.diferencias_divisas
        FOR EACH ROW EXECUTE FUNCTION finance.reject_frozen_pyg_adjustment_change()
        """
    )


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _decode_json(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value or []


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value or "0"))
