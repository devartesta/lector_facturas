from __future__ import annotations

from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from lector_facturas.api.store import ReviewStore
from lector_facturas.payment_fees import (
    PAYPAL_PLATFORM,
    SHOPIFY_PLATFORM,
    PayPalClient,
    PayPalConfig,
    PaymentFeeService,
    PaymentOrderTransaction,
    ShopifyPaymentsConfig,
    build_paypal_transaction_record,
    company_code_for_currency,
    market_code_for_shopify_currency,
    normalize_paypal_dispute,
    normalize_paypal_transaction,
    normalize_shopify_balance_transaction,
    normalize_shopify_dispute,
)


class FakeClient:
    def __init__(
        self,
        transactions: list[PaymentOrderTransaction],
        disputes: list[PaymentOrderTransaction],
        raw_records: list[dict] | None = None,
    ) -> None:
        self.transactions = transactions
        self.disputes = disputes
        self.raw_records = raw_records or []
        self.paypal_order_mapping: dict[str, dict[str, str]] = {}

    def list_transactions(self, *, date_from: str, date_to: str) -> list[PaymentOrderTransaction]:
        return list(self.transactions)

    def list_disputes(self, *, date_from: str, date_to: str) -> list[PaymentOrderTransaction]:
        return list(self.disputes)

    def load_sync_bundle(self, *, date_from: str, date_to: str) -> dict[str, list]:
        return {
            "raw_records": list(self.raw_records),
            "transactions": list(self.transactions),
            "disputes": list(self.disputes),
        }

    def build_paypal_order_mapping(self, *, date_from: str, date_to: str) -> dict[str, dict[str, str]]:
        return dict(self.paypal_order_mapping)


class PaymentFeesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = TemporaryDirectory()
        self.store = ReviewStore(storage_path=Path(self.tmp.name) / "review_items.json")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_market_mapping_uses_currency(self) -> None:
        self.assertEqual(market_code_for_shopify_currency("EUR"), "SL-EUR")
        self.assertEqual(market_code_for_shopify_currency("GBP"), "UK-GBP")
        self.assertEqual(market_code_for_shopify_currency("USD"), "USA-USD")

    def test_company_mapping_uses_currency(self) -> None:
        self.assertEqual(company_code_for_currency("EUR"), "SL")
        self.assertEqual(company_code_for_currency("GBP"), "LTD")
        self.assertEqual(company_code_for_currency("USD"), "INC")

    def test_shopify_config_normalizes_shop_name(self) -> None:
        config = ShopifyPaymentsConfig(
            shop_name="https://artestastore.myshopify.com/",
            client_id="client",
            client_secret="secret",
        )
        self.assertEqual(config.normalized_shop_name, "artestastore")

    def test_normalize_shopify_balance_transaction_sets_order_and_payout_dates(self) -> None:
        transaction = normalize_shopify_balance_transaction(
            {
                "id": "gid://shopify/ShopifyPaymentsBalanceTransaction/1",
                "type": "CHARGE",
                "test": False,
                "amount": {"amount": "100.00", "currencyCode": "GBP"},
                "fee": {"amount": "2.90"},
                "net": {"amount": "97.10"},
                "sourceId": "111",
                "sourceType": "CHARGE",
                "sourceOrderTransactionId": "gid://shopify/OrderTransaction/1",
                "associatedOrder": {"id": "gid://shopify/Order/10"},
                "associatedPayout": {"id": "gid://shopify/ShopifyPaymentsPayout/20", "status": "PAID"},
            },
            order_map={
                "gid://shopify/Order/10": {
                    "id": "gid://shopify/Order/10",
                    "name": "#1001",
                    "cancelledAt": None,
                    "transactions": {
                        "nodes": [
                            {
                                "id": "gid://shopify/OrderTransaction/1",
                                "status": "SUCCESS",
                                "processedAt": "2026-03-05T10:00:00Z",
                            }
                        ]
                    },
                }
            },
            payout_map={"gid://shopify/ShopifyPaymentsPayout/20": {"issuedAt": "2026-03-07T00:00:00Z"}},
        )
        self.assertIsNotNone(transaction)
        assert transaction is not None
        self.assertEqual(transaction.company_code, "LTD")
        self.assertEqual(transaction.market_code, "UK-GBP")
        self.assertEqual(transaction.order_name, "#1001")
        self.assertEqual(transaction.transaction_date, "2026-03-05T10:00:00Z")
        self.assertEqual(transaction.payout_date, "2026-03-07T00:00:00Z")

    def test_normalize_shopify_balance_transaction_marks_cancelled(self) -> None:
        transaction = normalize_shopify_balance_transaction(
            {
                "id": "gid://shopify/ShopifyPaymentsBalanceTransaction/2",
                "type": "CHARGE",
                "test": False,
                "amount": {"amount": "120.00", "currencyCode": "EUR"},
                "fee": {"amount": "3.00"},
                "net": {"amount": "117.00"},
                "sourceId": "112",
                "sourceType": "CHARGE",
                "sourceOrderTransactionId": "gid://shopify/OrderTransaction/2",
                "associatedOrder": {"id": "gid://shopify/Order/11"},
                "associatedPayout": {"id": "gid://shopify/ShopifyPaymentsPayout/21", "status": "PAID"},
            },
            order_map={
                "gid://shopify/Order/11": {
                    "id": "gid://shopify/Order/11",
                    "name": "#1002",
                    "cancelledAt": None,
                    "transactions": {
                        "nodes": [
                            {
                                "id": "gid://shopify/OrderTransaction/2",
                                "status": "VOIDED",
                                "processedAt": "2026-03-05T11:00:00Z",
                            }
                        ]
                    },
                }
            },
            payout_map={},
        )
        self.assertIsNotNone(transaction)
        assert transaction is not None
        self.assertTrue(transaction.is_cancelled)
        self.assertEqual(transaction.company_code, "SL")

    def test_normalize_paypal_transaction_requires_balance_impact(self) -> None:
        transaction = normalize_paypal_transaction(
            {
                "transaction_info": {
                    "transaction_id": "PAYPAL-1",
                    "transaction_status": "V",
                    "transaction_initiation_date": "2026-03-06T10:00:00Z",
                }
            }
        )
        self.assertIsNone(transaction)

    def test_normalize_paypal_transaction_sale_uses_positive_fee_and_net(self) -> None:
        transaction = normalize_paypal_transaction(
            {
                "transaction_info": {
                    "transaction_id": "PAYPAL-2",
                    "transaction_status": "S",
                    "transaction_event_code": "T0006",
                    "transaction_initiation_date": "2026-03-06T10:00:00Z",
                    "transaction_amount": {"value": "37.90", "currency_code": "EUR"},
                    "fee_amount": {"value": "-1.45", "currency_code": "EUR"},
                },
                "payer_info": {"email_address": "buyer@example.com"},
            }
        )
        self.assertIsNotNone(transaction)
        assert transaction is not None
        self.assertEqual(str(transaction.gross_amount), "37.90")
        self.assertEqual(str(transaction.fee_amount), "1.45")
        self.assertEqual(str(transaction.net_amount), "36.45")

    def test_normalize_paypal_transaction_maps_chargeback_and_dispute_fee_codes(self) -> None:
        chargeback = normalize_paypal_transaction(
            {
                "transaction_info": {
                    "transaction_id": "PAYPAL-CB-1",
                    "transaction_status": "S",
                    "transaction_event_code": "T1106",
                    "transaction_initiation_date": "2026-03-06T10:00:00Z",
                    "transaction_amount": {"value": "-59.22", "currency_code": "EUR"},
                    "fee_amount": {"value": "2.07", "currency_code": "EUR"},
                    "paypal_reference_id": "BASE-1",
                },
            }
        )
        dispute_fee = normalize_paypal_transaction(
            {
                "transaction_info": {
                    "transaction_id": "PAYPAL-DF-1",
                    "transaction_status": "S",
                    "transaction_event_code": "T0114",
                    "transaction_initiation_date": "2026-03-06T10:00:00Z",
                    "transaction_amount": {"value": "-14.00", "currency_code": "EUR"},
                    "paypal_reference_id": "BASE-1",
                },
            }
        )
        self.assertIsNotNone(chargeback)
        self.assertIsNotNone(dispute_fee)
        assert chargeback is not None
        assert dispute_fee is not None
        self.assertTrue(chargeback.is_chargeback)
        self.assertEqual(str(chargeback.chargeback_amount), "59.22")
        self.assertEqual(str(chargeback.chargeback_fee_amount), "2.07")
        self.assertEqual(str(dispute_fee.chargeback_fee_amount), "14.00")

    def test_normalize_disputes_handle_lost_and_won_outcomes(self) -> None:
        lost = normalize_shopify_dispute(
            {
                "id": 55,
                "type": "chargeback",
                "initiated_at": "2026-03-10T00:00:00Z",
                "currency": "USD",
                "amount": "30.00",
                "fee_amount": "15.00",
                "status": "lost",
            }
        )
        won = normalize_paypal_dispute(
            {
                "dispute_id": "PP-DSP-1",
                "create_time": "2026-03-11T00:00:00Z",
                "status": "resolved",
                "dispute_outcome": "resolved_seller_favorable",
                "dispute_amount": {"value": "40.00", "currency_code": "EUR"},
                "fee_amount": "10.00",
            }
        )
        self.assertIsNotNone(lost)
        self.assertIsNotNone(won)
        assert lost is not None
        assert won is not None
        self.assertEqual(str(lost.chargeback_amount), "30.00")
        self.assertEqual(str(lost.chargeback_fee_amount), "15.00")
        self.assertEqual(won.chargeback_amount, 0)

    def test_paypal_client_splits_long_ranges_and_caps_future_end_date(self) -> None:
        client = PayPalClient(PayPalConfig(client_id="id", client_secret="secret"))
        windows = client._iter_paypal_windows(date_from="2026-01-01", date_to="2026-12-31")
        self.assertEqual(windows[0][0].strftime("%Y-%m-%d"), "2026-01-01")
        self.assertEqual(windows[0][1].strftime("%Y-%m-%d"), "2026-01-31")
        self.assertEqual(windows[-1][1].strftime("%Y-%m-%d"), "2026-03-23")
        self.assertEqual(len(windows), 3)

    def test_sync_is_idempotent_and_builds_monthly_summary(self) -> None:
        shopify_tx = PaymentOrderTransaction(
            id="1",
            platform=SHOPIFY_PLATFORM,
            company_code="SL",
            market_code="SL-EUR",
            currency_code="EUR",
            order_id="ord-1",
            order_name="#1001",
            external_transaction_id="shp-1",
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
        shopify_dispute = PaymentOrderTransaction(
            id="2",
            platform=SHOPIFY_PLATFORM,
            company_code="SL",
            market_code="SL-EUR",
            currency_code="EUR",
            order_id="ord-1",
            order_name="#1001",
            external_transaction_id="shp-dsp-1",
            external_payout_id="",
            transaction_date="2026-03-10T09:00:00Z",
            transaction_type="chargeback",
            status="lost",
            gross_amount="0",
            fee_amount="0",
            net_amount="-15.00",
            chargeback_amount="10.00",
            chargeback_fee_amount="5.00",
            is_chargeback=True,
            raw_payload={},
        )
        paypal_tx = PaymentOrderTransaction(
            id="3",
            platform=PAYPAL_PLATFORM,
            company_code="SL",
            market_code="SL-EUR",
            currency_code="EUR",
            order_id="web-1",
            order_name="web-1",
            external_transaction_id="pp-1",
            external_payout_id="",
            transaction_date="2026-03-08T12:00:00Z",
            transaction_type="t0006",
            status="s",
            gross_amount="70.00",
            fee_amount="2.50",
            net_amount="67.50",
            raw_payload={},
        )
        service = PaymentFeeService(
            self.store,
            shopify_client=FakeClient([shopify_tx], [shopify_dispute]),
            paypal_client=FakeClient(
                [paypal_tx],
                [],
                raw_records=[
                    {
                        "source_record_id": "pp-1",
                        "transaction_id": "pp-1",
                        "transaction_date": "2026-03-08T12:00:00Z",
                        "tipo": "T0006",
                        "divisa": "EUR",
                        "bruto": "70.00",
                        "tarifa": "-2.60",
                        "neto": "67.40",
                        "company_code": "SL",
                        "market_code": "SL-EUR",
                        "raw_payload": {},
                    }
                ],
            ),
        )

        first = service.sync(date_from="2026-03-01", date_to="2026-03-31")
        second = service.sync(date_from="2026-03-01", date_to="2026-03-31")

        self.assertEqual(sum(item.transactions_upserted for item in first), 3)
        self.assertEqual(sum(item.transactions_upserted for item in second), 3)
        transactions = self.store.list_payment_order_transactions()
        self.assertEqual(len(transactions), 2)
        summary = self.store.list_payment_fee_monthly_summary(period_yyyymm="202603")
        self.assertEqual(len(summary), 2)
        shopify_summary = next(item for item in summary if item.platform == SHOPIFY_PLATFORM)
        paypal_summary = next(item for item in summary if item.platform == PAYPAL_PLATFORM)
        self.assertEqual(shopify_summary.company_code, "SL")
        self.assertEqual(shopify_summary.orders_count, 1)
        self.assertEqual(str(shopify_summary.fee_amount), "3.20")
        self.assertEqual(str(shopify_summary.chargeback_amount), "10.00")
        self.assertEqual(str(shopify_summary.chargeback_fee_amount), "5.00")
        self.assertEqual(str(shopify_summary.total_cost_amount), "8.20")
        self.assertEqual(str(paypal_summary.fee_amount), "-2.60")
        self.assertEqual(str(paypal_summary.total_cost_amount), "-2.60")

    def test_summary_excludes_chargeback_principal_from_total_cost_amount(self) -> None:
        summary = self.store.rebuild_payment_fee_monthly_summary
        rows = [
            PaymentOrderTransaction(
                id="cb-test-1",
                platform=SHOPIFY_PLATFORM,
                company_code="SL",
                market_code="SL-EUR",
                currency_code="EUR",
                order_id="ord-cb-1",
                order_name="#CB1",
                external_transaction_id="cb-1",
                external_payout_id="payout-cb-1",
                transaction_date="2026-03-10T09:00:00Z",
                payout_date="2026-03-11T00:00:00Z",
                transaction_type="chargeback",
                status="lost",
                gross_amount="0",
                fee_amount="0",
                net_amount="-45.00",
                chargeback_amount="30.00",
                chargeback_fee_amount="15.00",
                is_chargeback=True,
                raw_payload={},
            ),
        ]
        self.store.upsert_payment_order_transactions(rows)
        self.store.rebuild_payment_fee_monthly_summary(company_code="SL", platform=SHOPIFY_PLATFORM)

        summary_rows = self.store.list_payment_fee_monthly_summary(platform=SHOPIFY_PLATFORM, period_yyyymm="202603")
        self.assertEqual(len(summary_rows), 1)
        self.assertEqual(str(summary_rows[0].chargeback_amount), "30.00")
        self.assertEqual(str(summary_rows[0].chargeback_fee_amount), "15.00")
        self.assertEqual(str(summary_rows[0].total_cost_amount), "15.00")

    def test_shopify_summary_uses_payout_month_instead_of_transaction_month(self) -> None:
        shopify_tx = PaymentOrderTransaction(
            id="payout-month-1",
            platform=SHOPIFY_PLATFORM,
            company_code="SL",
            market_code="SL-EUR",
            currency_code="EUR",
            order_id="ord-2",
            order_name="#1002",
            external_transaction_id="shp-payout-month-1",
            external_payout_id="payout-2",
            transaction_date="2026-01-31T23:50:00Z",
            payout_date="2026-02-01T00:10:00Z",
            transaction_type="charge",
            status="paid",
            gross_amount="50.00",
            fee_amount="1.50",
            net_amount="48.50",
            raw_payload={},
        )
        service = PaymentFeeService(self.store, shopify_client=FakeClient([shopify_tx], []))

        service.sync(date_from="2026-01-01", date_to="2026-02-28", platform=SHOPIFY_PLATFORM)

        january = self.store.list_payment_fee_monthly_summary(platform=SHOPIFY_PLATFORM, period_yyyymm="202601")
        february = self.store.list_payment_fee_monthly_summary(platform=SHOPIFY_PLATFORM, period_yyyymm="202602")
        self.assertEqual(january, [])
        self.assertEqual(len(february), 1)
        self.assertEqual(str(february[0].gross_amount), "50.00")

    def test_shopify_summary_includes_transactions_without_payout_date_using_transaction_month(self) -> None:
        shopify_tx = PaymentOrderTransaction(
            id="no-payout-1",
            platform=SHOPIFY_PLATFORM,
            company_code="SL",
            market_code="SL-EUR",
            currency_code="EUR",
            order_id="ord-3",
            order_name="#1003",
            external_transaction_id="shp-no-payout-1",
            external_payout_id="",
            transaction_date="2026-03-05T10:00:00Z",
            payout_date="",
            transaction_type="charge",
            status="pending",
            gross_amount="75.00",
            fee_amount="2.00",
            net_amount="73.00",
            raw_payload={},
        )
        service = PaymentFeeService(self.store, shopify_client=FakeClient([shopify_tx], []))

        service.sync(date_from="2026-03-01", date_to="2026-03-31", platform=SHOPIFY_PLATFORM)

        transactions = self.store.list_payment_order_transactions(platform=SHOPIFY_PLATFORM)
        self.assertEqual(transactions, [])
        summary = self.store.list_payment_fee_monthly_summary(platform=SHOPIFY_PLATFORM, period_yyyymm="202603")
        self.assertEqual(len(summary), 1)
        self.assertEqual(str(summary[0].gross_amount), "75.00")
        self.assertEqual(str(summary[0].fee_amount), "2.00")

    def test_sync_persists_platform_raw_tables(self) -> None:
        shopify_tx = PaymentOrderTransaction(
            id="raw-shopify-1",
            platform=SHOPIFY_PLATFORM,
            company_code="SL",
            market_code="SL-EUR",
            currency_code="EUR",
            order_id="ord-raw-1",
            order_name="#2001",
            external_transaction_id="shp-raw-1",
            external_payout_id="payout-raw-1",
            transaction_date="2026-01-05T10:00:00Z",
            payout_date="2026-01-07T00:00:00Z",
            transaction_type="charge",
            status="paid",
            gross_amount="50.00",
            fee_amount="1.50",
            net_amount="48.50",
            raw_payload={},
        )
        paypal_tx = PaymentOrderTransaction(
            id="raw-paypal-1",
            platform=PAYPAL_PLATFORM,
            company_code="SL",
            market_code="SL-EUR",
            currency_code="EUR",
            order_id="inv-raw-1",
            order_name="inv-raw-1",
            external_transaction_id="pp-raw-1",
            external_payout_id="",
            transaction_date="2026-01-08T12:00:00Z",
            transaction_type="t0006",
            status="s",
            gross_amount="37.90",
            fee_amount="1.45",
            net_amount="36.45",
            raw_payload={},
        )
        service = PaymentFeeService(
            self.store,
            shopify_client=FakeClient(
                [shopify_tx],
                [],
                raw_records=[
                    {
                        "source_record_id": "shopify-raw-row-1",
                        "transaction_date": "2026-01-05T10:00:00Z",
                        "type": "charge",
                        "order_id": "ord-raw-1",
                        "order_name": "#2001",
                        "amount": "50.00",
                        "fee": "1.50",
                        "net": "48.50",
                        "currency": "EUR",
                        "company_code": "SL",
                        "market_code": "SL-EUR",
                        "raw_payload": {},
                    }
                ],
            ),
            paypal_client=FakeClient(
                [paypal_tx],
                [],
                raw_records=[
                    {
                        "source_record_id": "paypal-raw-row-1",
                        "transaction_date": "2026-01-08T12:00:00Z",
                        "fecha": "08/01/2026",
                        "hora": "12:00:00",
                        "zona_horaria": "UTC",
                        "nombre": "Buyer",
                        "tipo": "Pago",
                        "estado": "Completado",
                        "divisa": "EUR",
                        "bruto": "37.90",
                        "tarifa": "1.45",
                        "neto": "36.45",
                        "transaction_id": "pp-raw-1",
                        "company_code": "SL",
                        "market_code": "SL-EUR",
                        "raw_payload": {},
                    }
                ],
            ),
        )

        service.sync(date_from="2026-01-01", date_to="2026-01-31")

        shopify_raw = self.store.list_shopify_payout_transactions()
        paypal_raw = self.store.list_paypal_transactions_raw()
        self.assertEqual(len(shopify_raw), 1)
        self.assertEqual(shopify_raw[0]["source_record_id"], "shopify-raw-row-1")
        self.assertEqual(len(paypal_raw), 1)
        self.assertEqual(paypal_raw[0]["source_record_id"], "paypal-raw-row-1")

    def test_paypal_raw_record_extracts_as_order_name_when_present(self) -> None:
        record = build_paypal_transaction_record(
            {
                "transaction_info": {
                    "transaction_id": "PP-AS-1",
                    "transaction_initiation_date": "2026-01-08T12:00:00Z",
                    "transaction_status": "S",
                    "transaction_amount": {"value": "10.00", "currency_code": "EUR"},
                },
                "cart_info": {
                    "invoice_id": "AS-99999",
                },
            }
        )
        self.assertEqual(record["shopify_order_name"], "AS-99999")

    def test_sync_enriches_paypal_with_shopify_order_name_mapping(self) -> None:
        paypal_tx = PaymentOrderTransaction(
            id="paypal-map-1",
            platform=PAYPAL_PLATFORM,
            company_code="SL",
            market_code="SL-EUR",
            currency_code="EUR",
            order_id="rSession1",
            order_name="rSession1",
            external_transaction_id="9Y912477PK820251Y",
            external_payout_id="",
            transaction_date="2026-01-31T21:56:55Z",
            transaction_type="t0006",
            status="s",
            gross_amount="53.46",
            fee_amount="1.50",
            net_amount="51.96",
            raw_payload={
                "transaction_info": {
                    "transaction_id": "9Y912477PK820251Y",
                    "paypal_reference_id": "8LG45095Y2431373W",
                },
                "cart_info": {
                    "invoice_id": "rTH77Cz6LalCKCFyp1dcykEnq",
                    "custom_field": "{\"shop_id\":751927347,\"session_id\":\"rTH77Cz6LalCKCFyp1dcykEnq\"}",
                },
            },
        )
        shopify_client = FakeClient([], [])
        shopify_client.paypal_order_mapping = {
            "authorization_code": {
                "9Y912477PK820251Y": {
                    "order_id": "gid://shopify/Order/7517225615698",
                    "order_name": "AS-97248",
                }
            },
            "gateway_transaction_id": {},
            "payment_id": {},
        }
        paypal_client = FakeClient(
            [paypal_tx],
            [],
            raw_records=[
                {
                    "source_record_id": "9Y912477PK820251Y",
                    "transaction_id": "9Y912477PK820251Y",
                    "reference_transaction_id": "8LG45095Y2431373W",
                    "invoice_number": "rTH77Cz6LalCKCFyp1dcykEnq",
                    "custom_number": "{\"shop_id\":751927347,\"session_id\":\"rTH77Cz6LalCKCFyp1dcykEnq\"}",
                    "order_number": "rTH77Cz6LalCKCFyp1dcykEnq",
                    "transaction_date": "2026-01-31T21:56:55Z",
                    "tipo": "Pago",
                    "divisa": "EUR",
                    "bruto": "53.46",
                    "tarifa": "-1.50",
                    "neto": "51.96",
                    "company_code": "SL",
                    "market_code": "SL-EUR",
                    "raw_payload": {},
                }
            ],
        )
        service = PaymentFeeService(self.store, shopify_client=shopify_client, paypal_client=paypal_client)

        service.sync(date_from="2026-01-01", date_to="2026-01-31", platform=PAYPAL_PLATFORM)

        paypal_raw = self.store.list_paypal_transactions_raw()
        self.assertEqual(paypal_raw[0]["shopify_order_name"], "AS-97248")
        transactions = self.store.list_payment_order_transactions(platform=PAYPAL_PLATFORM)
        self.assertEqual(transactions[0].order_name, "AS-97248")
        self.assertEqual(transactions[0].order_id, "gid://shopify/Order/7517225615698")

    def test_paypal_uses_company_and_market_from_currency(self) -> None:
        payload = {
            "transaction_info": {
                "transaction_id": "PAYPAL-GBP-1",
                "transaction_event_code": "T0006",
                "transaction_status": "S",
                "transaction_initiation_date": "2026-02-10T10:00:00Z",
                "transaction_amount": {"value": "100.00", "currency_code": "GBP"},
                "fee_amount": {"value": "-3.20", "currency_code": "GBP"},
                "net_amount": {"value": "96.80", "currency_code": "GBP"},
            },
            "cart_info": {"invoice_id": "INV-GBP-1"},
        }

        raw_record = build_paypal_transaction_record(payload)
        transaction = normalize_paypal_transaction(payload)

        self.assertEqual(raw_record["company_code"], "LTD")
        self.assertEqual(raw_record["market_code"], "UK-GBP")
        self.assertIsNotNone(transaction)
        assert transaction is not None
        self.assertEqual(transaction.company_code, "LTD")
        self.assertEqual(transaction.market_code, "UK-GBP")


if __name__ == "__main__":
    unittest.main()
