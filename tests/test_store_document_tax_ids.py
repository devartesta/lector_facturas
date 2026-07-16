from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
import sys
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from lector_facturas.api.store import ReviewStore


class _FakeConnection:
    def __init__(self) -> None:
        self.statements: list[tuple[str, tuple]] = []
        self.committed = False

    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def execute(self, sql: str, params: tuple | None = None):
        self.statements.append((sql, params or ()))
        return self

    def commit(self) -> None:
        self.committed = True


def _parsed(**overrides):
    values = {
        "invoice_number": "INV-1",
        "invoice_date": date(2026, 4, 1),
        "issuer_company_name": "Supplier, S.L.",
        "billed_company_name": "ARTESTA STORE, S.L.",
        "supplier_name": "Supplier",
        "billing_period_start": date(2026, 4, 1),
        "billing_period_end": date(2026, 4, 30),
        "vat_percent": Decimal("21"),
        "gross_amount": Decimal("121.00"),
        "vat_amount": Decimal("21.00"),
        "net_amount": Decimal("100.00"),
        "currency_code": "EUR",
        "parser_name": "test_parser",
        "parser_confidence": Decimal("1"),
        "extracted_raw": {},
        "period_yyyymm": "202604",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _store_with_connection(conn: _FakeConnection) -> ReviewStore:
    store = ReviewStore(storage_path=Path("unused.json"))
    store.database_url = "postgres://unused"
    store._connect = lambda: conn  # type: ignore[method-assign]
    store._find_supplier_id = lambda _conn, _company, _supplier: "supplier-id"  # type: ignore[method-assign]
    store._get_supplier_payment_meta = lambda _conn, _company, _supplier: {  # type: ignore[method-assign]
        "is_direct_debit": False,
        "payment_terms_days": 30,
    }
    return store


def _insert_params(conn: _FakeConnection) -> tuple:
    for sql, params in conn.statements:
        if "INSERT INTO invoices.documents" in sql:
            return params
    raise AssertionError("documents insert was not executed")


def test_insert_document_persists_optional_tax_ids() -> None:
    conn = _FakeConnection()
    store = _store_with_connection(conn)

    store.insert_document_from_parsed(
        company_code="SL",
        supplier_code="SUPPLIER",
        parsed=_parsed(issuer_tax_id="ESB12345678", billed_tax_id="ESB98765432"),
        windows_path="",
        drive_url="",
        drive_file_id="",
        original_filename="invoice.pdf",
        source_channel="test",
    )

    params = _insert_params(conn)
    assert params[5] == "ESB12345678"
    assert params[6] == "ESB98765432"
    assert conn.committed is True


def test_insert_document_defaults_missing_tax_ids_to_null() -> None:
    conn = _FakeConnection()
    store = _store_with_connection(conn)

    store.insert_document_from_parsed(
        company_code="SL",
        supplier_code="SUPPLIER",
        parsed=_parsed(),
        windows_path="",
        drive_url="",
        drive_file_id="",
        original_filename="invoice.pdf",
        source_channel="test",
    )

    params = _insert_params(conn)
    assert params[5] is None
    assert params[6] is None
