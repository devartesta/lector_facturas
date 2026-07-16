from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path

import pytest
from openpyxl import load_workbook

from lector_facturas.payment_fee_detail_workbook import (
    PaymentFeeDetailBundle,
    build_payment_fee_detail_workbook,
    calculate_payment_fee_metrics,
    collect_payment_fee_detail,
)
from lector_facturas.payment_fees import PaymentFeeSummaryRow, PaymentOrderTransaction


def _sample_bundle(company_code: str = "SL") -> PaymentFeeDetailBundle:
    return PaymentFeeDetailBundle(
        company_code=company_code,
        period_yyyymm="202603",
        summaries=(
            PaymentFeeSummaryRow(
                company_code=company_code,
                period_yyyymm="202603",
                platform="shopify",
                market_code=f"{company_code}-EUR",
                currency_code="EUR",
                orders_count=1,
                transactions_count=1,
                gross_amount=Decimal("100.00"),
                fee_amount=Decimal("5.00"),
                chargeback_amount=Decimal("30.00"),
                chargeback_fee_amount=Decimal("15.00"),
                total_cost_amount=Decimal("20.00"),
                net_amount=Decimal("95.00"),
                payout_count=1,
            ),
            PaymentFeeSummaryRow(
                company_code=company_code,
                period_yyyymm="202603",
                platform="paypal",
                market_code=f"{company_code}-EUR",
                currency_code="EUR",
                orders_count=1,
                transactions_count=1,
                gross_amount=Decimal("50.00"),
                fee_amount=Decimal("2.00"),
                chargeback_amount=Decimal("0.00"),
                chargeback_fee_amount=Decimal("0.00"),
                total_cost_amount=Decimal("2.00"),
                net_amount=Decimal("48.00"),
                payout_count=1,
            ),
        ),
        transactions=(
            PaymentOrderTransaction(
                id="tx-shopify-1",
                platform="shopify",
                company_code=company_code,
                market_code=f"{company_code}-EUR",
                currency_code="EUR",
                order_id="ord-1",
                order_name="AS-1001",
                external_transaction_id="shopify-1",
                external_payout_id="payout-1",
                transaction_date="2026-03-31T10:30:00Z",
                payout_date="2026-04-02T00:00:00Z",
                transaction_type="charge",
                status="paid",
                gross_amount=Decimal("100.00"),
                fee_amount=Decimal("5.00"),
                chargeback_fee_amount=Decimal("15.00"),
                chargeback_amount=Decimal("30.00"),
                net_amount=Decimal("95.00"),
            ),
            PaymentOrderTransaction(
                id="tx-paypal-1",
                platform="paypal",
                company_code=company_code,
                market_code=f"{company_code}-EUR",
                currency_code="EUR",
                order_id="ord-2",
                order_name="AS-1002",
                external_transaction_id="paypal-1",
                external_payout_id="paypal-payout-1",
                transaction_date="2026-03-08T10:00:00Z",
                payout_date="2026-03-08T12:00:00Z",
                transaction_type="payment",
                status="completed",
                gross_amount=Decimal("50.00"),
                fee_amount=Decimal("2.00"),
                chargeback_fee_amount=Decimal("0.00"),
                chargeback_amount=Decimal("0.00"),
                net_amount=Decimal("48.00"),
            ),
        ),
        shopify_raw_rows=(
            {
                "transaction_date": "2026-03-31T10:30:00Z",
                "type": "charge",
                "order_name": "AS-1001",
                "payout_date": "2026-04-02",
                "payout_id": "payout-1",
                "amount": "100.00",
                "fee": "5.00",
                "net": "95.00",
                "payment_method_name": "visa",
                "currency": "EUR",
                "presentment_amount": "100.00",
                "presentment_currency": "EUR",
                "company_code": company_code,
            },
        ),
        paypal_raw_rows=(
            {
                "transaction_date": "2026-03-08",
                "shopify_order_name": "AS-1002",
                "tipo": "Payment Received",
                "estado": "Completed",
                "divisa": "EUR",
                "bruto": "50.00",
                "tarifa": "-2.00",
                "neto": "48.00",
                "transaction_id": "pp-1",
                "reference_transaction_id": "pp-ref-1",
                "company_code": company_code,
            },
        ),
    )


def test_build_payment_fee_detail_workbook_creates_tie_out_summary_and_detail(tmp_path: Path) -> None:
    bundle = _sample_bundle()
    output_path = tmp_path / "payment_fees_sl_202603.xlsx"

    build_payment_fee_detail_workbook(bundle, output_path)

    metrics = calculate_payment_fee_metrics(bundle)
    assert metrics.total_pyg_cost == Decimal("22.00")
    assert metrics.shopify_pyg_cost == Decimal("20.00")
    assert metrics.paypal_pyg_cost == Decimal("2.00")
    assert metrics.tie_out_delta == Decimal("0.00")

    workbook = load_workbook(output_path, data_only=False)
    assert workbook.sheetnames == ["Summary", "Detail", "Shopify Raw", "PayPal Raw"]

    ws_summary = workbook["Summary"]
    assert ws_summary["A1"].value == "Payment Fees Summary"
    assert ws_summary["B2"].value == "Artesta Store, S.L"
    assert ws_summary["B3"].value == "202603"
    assert ws_summary["A6"].value == "Total PYG month"
    assert ws_summary["B6"].value == Decimal("22.00")
    assert ws_summary["A7"].value == "Shopify subtotal"
    assert ws_summary["B7"].value == Decimal("20.00")
    assert ws_summary["A8"].value == "PayPal subtotal"
    assert ws_summary["B8"].value == Decimal("2.00")
    assert ws_summary["A12"].value == "Tie-out delta"
    assert ws_summary["B12"].value == Decimal("0.00")
    assert ws_summary["A15"].value == "Platform"
    assert ws_summary["A16"].value == "SHOPIFY"
    assert ws_summary["A17"].value == "PAYPAL"
    assert ws_summary["A18"].value == "TOTAL"
    assert ws_summary["A21"].value == "Shopify Payout Timing"

    ws_detail = workbook["Detail"]
    assert ws_detail["A2"].value == "SHOPIFY"
    assert ws_detail["C2"].value == "AS-1001"
    assert ws_detail["N2"].value == Decimal("20.00")
    assert ws_detail["A3"].value == "PAYPAL"
    assert ws_detail["C3"].value == "AS-1002"
    assert ws_detail["N3"].value == Decimal("2.00")


@pytest.mark.parametrize(
    ("company_code", "company_label"),
    (
        ("SL", "Artesta Store, S.L"),
        ("LTD", "Artesta Stores (UK) Ltd"),
        ("INC", "Artesta Inc"),
    ),
)
def test_build_payment_fee_detail_workbook_supports_all_companies(company_code: str, company_label: str) -> None:
    workbook = load_workbook(build_payment_fee_detail_workbook(_sample_bundle(company_code)), data_only=False)

    assert workbook.sheetnames == ["Summary", "Detail", "Shopify Raw", "PayPal Raw"]
    assert workbook["Summary"]["B2"].value == company_label
    assert workbook["Summary"]["B12"].value == Decimal("0.00")


def test_collect_payment_fee_detail_uses_period_yyyymm_for_shopify_detail(monkeypatch: pytest.MonkeyPatch) -> None:
    summary = PaymentFeeSummaryRow(
        company_code="SL",
        period_yyyymm="202603",
        platform="shopify",
        market_code="SL-EUR",
        currency_code="EUR",
        orders_count=1,
        transactions_count=1,
        gross_amount=Decimal("100.00"),
        fee_amount=Decimal("5.00"),
        chargeback_amount=Decimal("0.00"),
        chargeback_fee_amount=Decimal("0.00"),
        total_cost_amount=Decimal("5.00"),
        net_amount=Decimal("95.00"),
        payout_count=1,
    )
    march_transaction = PaymentOrderTransaction(
        id="tx-march",
        platform="shopify",
        company_code="SL",
        market_code="SL-EUR",
        currency_code="EUR",
        order_id="ord-1",
        order_name="AS-MARCH",
        external_transaction_id="shopify-march",
        external_payout_id="payout-april",
        transaction_date="2026-03-31T10:30:00Z",
        payout_date="2026-04-02T00:00:00Z",
        transaction_type="charge",
        status="paid",
        gross_amount=Decimal("100.00"),
        fee_amount=Decimal("5.00"),
        net_amount=Decimal("95.00"),
    )
    april_transaction = PaymentOrderTransaction(
        id="tx-april",
        platform="shopify",
        company_code="SL",
        market_code="SL-EUR",
        currency_code="EUR",
        order_id="ord-2",
        order_name="AS-APRIL",
        external_transaction_id="shopify-april",
        external_payout_id="payout-april-2",
        transaction_date="2026-04-01T00:30:00Z",
        payout_date="2026-04-03T00:00:00Z",
        transaction_type="charge",
        status="paid",
        gross_amount=Decimal("100.00"),
        fee_amount=Decimal("5.00"),
        net_amount=Decimal("95.00"),
    )
    captured: dict[str, object] = {}

    class FakeStore:
        def __init__(self, database_url: str | None = None) -> None:
            captured["database_url"] = database_url

        def list_payment_fee_monthly_summary(self, *, company_code: str, period_yyyymm: str):
            assert company_code == "SL"
            assert period_yyyymm == "202603"
            return [summary]

        def list_payment_order_transactions(self, **kwargs):
            captured["transactions_kwargs"] = kwargs
            if kwargs["period_yyyymm"] == "202603":
                return [march_transaction]
            if kwargs["period_yyyymm"] == "202604":
                return [april_transaction]
            return []

        def list_shopify_payout_transactions(self):
            return [
                {"company_code": "SL", "transaction_date": "2026-03-31T10:30:00Z", "type": "charge", "order_name": "AS-MARCH"},
                {"company_code": "SL", "transaction_date": "2026-04-01T00:30:00Z", "type": "charge", "order_name": "AS-APRIL"},
            ]

        def list_paypal_transactions_raw(self):
            return []

    monkeypatch.setattr("lector_facturas.payment_fee_detail_workbook.ReviewStore", FakeStore)

    bundle = collect_payment_fee_detail(company_code="SL", period_yyyymm="202603", database_url="postgresql://example")

    assert captured["database_url"] == "postgresql://example"
    assert captured["transactions_kwargs"] == {
        "company_code": "SL",
        "period_yyyymm": "202603",
        "include_unpaid_shopify": True,
    }
    assert [tx.order_name for tx in bundle.transactions] == ["AS-MARCH"]
    assert [row["order_name"] for row in bundle.shopify_raw_rows] == ["AS-MARCH"]
    assert calculate_payment_fee_metrics(bundle).tie_out_delta == Decimal("0.00")


def test_collect_payment_fee_detail_uses_transaction_month_for_shopify_raw() -> None:
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        return

    bundle = collect_payment_fee_detail(company_code="SL", period_yyyymm="202603", database_url=database_url)

    assert bundle.shopify_raw_rows
    fee_sum = sum((Decimal(str(row.get("fee") or "0")) for row in bundle.shopify_raw_rows), Decimal("0.00"))
    assert fee_sum == Decimal("2910.29")
    payout_months = {str(row.get("payout_date", ""))[:7] for row in bundle.shopify_raw_rows if row.get("payout_date")}
    assert "2026-04" in payout_months
