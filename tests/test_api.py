from __future__ import annotations

from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

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
        response = self.client.get("/integrations/google-drive/status")
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertFalse(payload["drive_ready"])

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
