from __future__ import annotations

from pathlib import Path
from datetime import date
from decimal import Decimal
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from lector_facturas.api.app import create_app
from lector_facturas.api.store import ReviewStore
from lector_facturas.payment_fees import PaymentOrderTransaction
from lector_facturas.settings import AppSettings


class ApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        storage_path = Path(self.tmp.name) / "review_items.json"
        finance_root = Path(self.tmp.name) / "finance"
        app = create_app()
        store = ReviewStore(storage_path=storage_path, finance_root=finance_root)
        app.dependency_overrides.clear()
        from lector_facturas.api.app import get_store

        app.dependency_overrides[get_store] = lambda: store
        self.client = TestClient(app)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_health(self) -> None:
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})

    def test_lists_companies(self) -> None:
        response = self.client.get("/companies")
        self.assertEqual(response.status_code, 200)
        companies = response.json()
        self.assertTrue(any(company["code"] == "SL" for company in companies))

    def test_lists_review_items(self) -> None:
        response = self.client.get("/review-items")
        self.assertEqual(response.status_code, 200)
        self.assertGreaterEqual(len(response.json()), 2)

    def test_resolves_review_item(self) -> None:
        review_items = self.client.get("/review-items").json()
        review_item_id = review_items[0]["id"]
        response = self.client.post(
            f"/review-items/{review_item_id}/resolve",
            json={
                "company": "ARTESTA INC",
                "supplier_code": "SHAREDSERVICESSL",
                "invoice_date": "2026-01-15",
                "invoice_number": "2026-0009",
            },
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "resolved")

    def test_daily_run_without_email_returns_summary(self) -> None:
        response = self.client.post("/jobs/daily-run?send_email=false")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["review_items_open"], 2)
        self.assertFalse(payload["email_sent"])

    def test_google_drive_status_reports_missing_config_cleanly(self) -> None:
        from lector_facturas.api.app import get_settings

        self.client.app.dependency_overrides[get_settings] = lambda: AppSettings()
        response = self.client.get("/integrations/google-drive/status")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertFalse(payload["drive_ready"])
        self.client.app.dependency_overrides.pop(get_settings, None)

    def test_google_drive_bootstrap_requires_oauth_config(self) -> None:
        from lector_facturas.api.app import get_settings

        self.client.app.dependency_overrides[get_settings] = lambda: AppSettings()
        response = self.client.post(
            "/integrations/google-drive/bootstrap",
            json={
                "root_name": "ARTESTA - 6. Finances",
                "year": 2026,
                "start_month": 1,
                "end_month": 12,
                "entities": ["SL"],
            },
        )
        self.assertEqual(response.status_code, 400)
        self.client.app.dependency_overrides.pop(get_settings, None)

    def test_payment_fee_endpoints_return_transactions_and_summary(self) -> None:
        class StubPaymentFeeService:
            def sync(self, *, date_from: str, date_to: str, platform: str | None = None) -> list[object]:
                Result = type("Result", (), {})
                result = Result()
                result.platform = platform or "shopify"
                result.transactions_upserted = 1
                result.summaries_rebuilt = 1
                result.date_from = date_from
                result.date_to = date_to
                return [result]

        transaction = PaymentOrderTransaction(
            id="tx-1",
            platform="shopify",
            company_code="SL",
            market_code="SL-EUR",
            currency_code="EUR",
            order_id="ord-1",
            order_name="#1001",
            external_transaction_id="shopify-1",
            external_payout_id="payout-1",
            transaction_date="2026-03-05T10:00:00Z",
            payout_date="2026-03-07T00:00:00Z",
            transaction_type="charge",
            status="paid",
            gross_amount="100.00",
            fee_amount="3.20",
            net_amount="96.80",
            raw_payload={},
        )
        from lector_facturas.api.app import get_payment_fee_service, get_store

        store = ReviewStore(storage_path=Path(self.tmp.name) / "review_items_alt.json")
        store.upsert_payment_order_transactions([transaction])
        store.rebuild_payment_fee_monthly_summary(company_code="SL", platform="shopify")
        self.client.app.dependency_overrides[get_store] = lambda: store
        self.client.app.dependency_overrides[get_payment_fee_service] = lambda: StubPaymentFeeService()

        sync_response = self.client.post(
            "/integrations/payment-fees/sync",
            json={"date_from": "2026-03-01", "date_to": "2026-03-31", "platform": "shopify"},
        )
        self.assertEqual(sync_response.status_code, 200)
        self.assertEqual(sync_response.json()[0]["transactions_upserted"], 1)

        tx_response = self.client.get("/payment-fees/transactions?platform=shopify&period_yyyymm=202603")
        self.assertEqual(tx_response.status_code, 200)
        tx_payload = tx_response.json()
        self.assertEqual(len(tx_payload), 1)
        self.assertEqual(tx_payload[0]["order_name"], "#1001")
        self.assertEqual(tx_payload[0]["payout_date"], "2026-03-07T00:00:00Z")

        summary_response = self.client.get("/payment-fees/summary?platform=shopify&period_yyyymm=202603")
        self.assertEqual(summary_response.status_code, 200)
        summary_payload = summary_response.json()
        self.assertEqual(len(summary_payload), 1)
        self.assertEqual(summary_payload[0]["company_code"], "SL")
        self.assertEqual(summary_payload[0]["market_code"], "SL-EUR")
        self.assertEqual(summary_payload[0]["payout_count"], 1)

    def test_payment_status_accepts_multiselect_supplier_filter(self) -> None:
        from lector_facturas.api.app import get_store

        class StubStore:
            def __init__(self) -> None:
                self.calls: list[dict] = []

            def list_documents_for_payment_report(self, **kwargs):
                self.calls.append(kwargs)
                return []

        stub_store = StubStore()
        self.client.app.dependency_overrides[get_store] = lambda: stub_store

        response = self.client.get("/documents/payment-status?supplier_code=JONDO&supplier_code=TGI")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), [])
        self.assertEqual(stub_store.calls[0]["supplier_codes"], ["JONDO", "TGI"])

    def test_payment_status_for_jondo_is_normalized_as_paid(self) -> None:
        from lector_facturas.api.app import get_store

        class StubStore:
            def list_documents_for_payment_report(self, **kwargs):
                return [{
                    "id": "doc-1",
                    "company_code": "SL",
                    "supplier_code": "JONDO",
                    "invoice_number": "INV-1",
                    "invoice_date": date(2026, 4, 1),
                    "period_yyyymm": "202604",
                    "gross_amount": Decimal("120.00"),
                    "net_amount": Decimal("100.00"),
                    "currency_code": "EUR",
                    "drive_url": "https://example.test/invoice",
                    "payment_status": "pending",
                    "payment_date": None,
                    "payment_method": "",
                    "payment_amount": None,
                    "payment_due_date": date(2026, 4, 30),
                    "is_direct_debit": False,
                    "document_type": "invoice",
                }]

        self.client.app.dependency_overrides[get_store] = lambda: StubStore()

        response = self.client.get("/documents/payment-status?supplier_code=JONDO")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["supplier_code"], "JONDO")
        self.assertEqual(payload[0]["payment_status"], "paid")
        self.assertEqual(payload[0]["payment_method"], "auto_jondo")
        self.assertEqual(payload[0]["payment_amount"], "120.00")
        self.assertEqual(payload[0]["payment_date"], "2026-04-30")

    def test_pending_filter_excludes_rows_that_render_as_paid(self) -> None:
        store = ReviewStore(storage_path=Path(self.tmp.name) / "review_items_pending_filter.json")

        rows = [
            {
                "id": "doc-jondo",
                "company_code": "SL",
                "supplier_code": "JONDO",
                "invoice_number": "AS-94763",
                "invoice_date": date(2026, 1, 4),
                "period_yyyymm": "202601",
                "gross_amount": Decimal("52.87"),
                "net_amount": Decimal("43.69"),
                "currency_code": "USD",
                "drive_url": "https://example.test/jondo",
                "payment_status": "pending",
                "payment_date": None,
                "payment_method": "",
                "payment_amount": None,
                "payment_due_date": None,
                "is_direct_debit": False,
                "document_type": "invoice",
            },
            {
                "id": "doc-yat",
                "company_code": "LTD",
                "supplier_code": "YOURACCOUNTSTAXES",
                "invoice_number": "INV-0639",
                "invoice_date": date(2025, 12, 11),
                "period_yyyymm": "202512",
                "gross_amount": Decimal("1020.00"),
                "net_amount": Decimal("900.00"),
                "currency_code": "GBP",
                "drive_url": "https://example.test/yat",
                "payment_status": "pending",
                "payment_date": None,
                "payment_method": "",
                "payment_amount": None,
                "payment_due_date": None,
                "is_direct_debit": False,
                "document_type": "invoice",
            },
        ]

        filtered = [
            row for row in store._aggregate_payment_report_rows(rows)
            if store._effective_payment_status(row) == "pending"
        ]

        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["supplier_code"], "YOURACCOUNTSTAXES")

    def test_paid_filter_includes_jondo_effective_paid_rows(self) -> None:
        store = ReviewStore(storage_path=Path(self.tmp.name) / "review_items_paid_filter.json")

        rows = [
            {
                "id": "doc-jondo",
                "company_code": "SL",
                "supplier_code": "JONDO",
                "invoice_number": "AS-94763",
                "invoice_date": date(2026, 1, 4),
                "period_yyyymm": "202601",
                "gross_amount": Decimal("52.87"),
                "net_amount": Decimal("43.69"),
                "currency_code": "USD",
                "drive_url": "https://example.test/jondo",
                "payment_status": "pending",
                "payment_date": None,
                "payment_method": "",
                "payment_amount": None,
                "payment_due_date": None,
                "is_direct_debit": False,
                "document_type": "invoice",
            },
        ]

        filtered = [
            row for row in store._aggregate_payment_report_rows(rows)
            if store._effective_payment_status(row) == "paid"
        ]

        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["supplier_code"], "JONDO")

    def test_payment_report_rows_are_grouped_by_supplier_and_invoice_number(self) -> None:
        store = ReviewStore(storage_path=Path(self.tmp.name) / "review_items_grouped.json")

        rows = [
            {
                "id": "doc-1",
                "company_code": "SL",
                "supplier_code": "JONDO",
                "invoice_number": "AS-100",
                "invoice_date": date(2026, 4, 1),
                "period_yyyymm": "202604",
                "gross_amount": Decimal("10.00"),
                "net_amount": Decimal("8.00"),
                "currency_code": "EUR",
                "drive_url": "https://example.test/1",
                "payment_status": "pending",
                "payment_date": None,
                "payment_method": "",
                "payment_amount": None,
                "payment_due_date": date(2026, 4, 30),
                "is_direct_debit": False,
                "document_type": "invoice",
            },
            {
                "id": "doc-2",
                "company_code": "SL",
                "supplier_code": "JONDO",
                "invoice_number": "AS-100",
                "invoice_date": date(2026, 4, 2),
                "period_yyyymm": "202604",
                "gross_amount": Decimal("15.50"),
                "net_amount": Decimal("12.40"),
                "currency_code": "EUR",
                "drive_url": "",
                "payment_status": "pending",
                "payment_date": None,
                "payment_method": "",
                "payment_amount": None,
                "payment_due_date": date(2026, 5, 2),
                "is_direct_debit": False,
                "document_type": "invoice",
            },
            {
                "id": "doc-3",
                "company_code": "SL",
                "supplier_code": "TGI",
                "invoice_number": "AS-100",
                "invoice_date": date(2026, 4, 2),
                "period_yyyymm": "202604",
                "gross_amount": Decimal("99.00"),
                "net_amount": Decimal("80.00"),
                "currency_code": "EUR",
                "drive_url": "",
                "payment_status": "pending",
                "payment_date": None,
                "payment_method": "",
                "payment_amount": None,
                "payment_due_date": date(2026, 5, 2),
                "is_direct_debit": False,
                "document_type": "invoice",
            },
        ]

        grouped = store._aggregate_payment_report_rows(rows)

        self.assertEqual(len(grouped), 2)
        jondo_row = next(row for row in grouped if row["supplier_code"] == "JONDO")
        self.assertEqual(jondo_row["gross_amount"], Decimal("25.50"))
        self.assertEqual(jondo_row["net_amount"], Decimal("20.40"))
        self.assertEqual(jondo_row["invoice_date"], date(2026, 4, 1))
        self.assertEqual(jondo_row["payment_due_date"], date(2026, 5, 2))
        self.assertEqual(jondo_row["drive_url"], "https://example.test/1")

    def test_payment_fee_detail_job_runs_for_all_companies(self) -> None:
        Result = type("Result", (), {})

        def _fake_sync(*, settings, company_code, period_yyyymm):
            result = Result()
            result.company_code = company_code
            result.period_yyyymm = period_yyyymm
            result.drive_file_name = f"payment_fees_{company_code.lower()}_{period_yyyymm}.xlsx"
            return result

        with patch("lector_facturas.api.app.sync_payment_fee_detail_to_drive", side_effect=_fake_sync):
            response = self.client.post("/jobs/payment-fee-detail/run?period_yyyymm=202603")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["period_yyyymm"], "202603")
        self.assertEqual(
            [item["step"] for item in payload["results"]],
            ["payment_fee_detail/SL", "payment_fee_detail/LTD", "payment_fee_detail/INC"],
        )
        self.assertTrue(all(item["status"] == "ok" for item in payload["results"]))
