from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import base64
import json
import uuid


SHOPIFY_PLATFORM = "shopify"
PAYPAL_PLATFORM = "paypal"
API_DECIMAL_ZERO = Decimal("0.00")
SUMMARY_QUANTIZER = Decimal("0.01")
SHOPIFY_DEFAULT_API_VERSION = "2026-01"


@dataclass(frozen=True)
class ShopifyPaymentsConfig:
    shop_name: str
    client_id: str
    client_secret: str
    api_version: str = SHOPIFY_DEFAULT_API_VERSION

    @property
    def normalized_shop_name(self) -> str:
        value = self.shop_name.strip()
        value = value.removeprefix("https://").removeprefix("http://")
        value = value.removesuffix("/")
        if value.endswith(".myshopify.com"):
            value = value[: -len(".myshopify.com")]
        return value


@dataclass(frozen=True)
class PayPalConfig:
    client_id: str
    client_secret: str
    base_url: str = "https://api-m.paypal.com"


@dataclass(frozen=True)
class PaymentOrderTransaction:
    id: str
    platform: str
    company_code: str
    market_code: str
    currency_code: str
    order_id: str
    order_name: str
    external_transaction_id: str
    external_payout_id: str
    transaction_date: str
    payout_date: str = ""
    transaction_type: str = ""
    status: str = ""
    gross_amount: Decimal = API_DECIMAL_ZERO
    fee_amount: Decimal = API_DECIMAL_ZERO
    net_amount: Decimal = API_DECIMAL_ZERO
    chargeback_amount: Decimal = API_DECIMAL_ZERO
    chargeback_fee_amount: Decimal = API_DECIMAL_ZERO
    affects_balance: bool = True
    is_cancelled: bool = False
    is_chargeback: bool = False
    payment_reference: str = ""
    customer_reference: str = ""
    raw_payload: dict[str, Any] | None = None

    @property
    def period_yyyymm(self) -> str:
        return parse_datetime(self.transaction_date).strftime("%Y%m")

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "platform": self.platform,
            "company_code": self.company_code,
            "market_code": self.market_code,
            "currency_code": self.currency_code,
            "order_id": self.order_id,
            "order_name": self.order_name,
            "external_transaction_id": self.external_transaction_id,
            "external_payout_id": self.external_payout_id,
            "transaction_date": self.transaction_date,
            "payout_date": self.payout_date,
            "transaction_type": self.transaction_type,
            "status": self.status,
            "gross_amount": decimal_to_string(self.gross_amount),
            "fee_amount": decimal_to_string(self.fee_amount),
            "net_amount": decimal_to_string(self.net_amount),
            "chargeback_amount": decimal_to_string(self.chargeback_amount),
            "chargeback_fee_amount": decimal_to_string(self.chargeback_fee_amount),
            "affects_balance": self.affects_balance,
            "is_cancelled": self.is_cancelled,
            "is_chargeback": self.is_chargeback,
            "payment_reference": self.payment_reference,
            "customer_reference": self.customer_reference,
            "raw_payload": self.raw_payload or {},
        }

    @classmethod
    def from_json_dict(cls, payload: dict[str, Any]) -> "PaymentOrderTransaction":
        return cls(
            id=str(payload["id"]),
            platform=str(payload["platform"]),
            company_code=str(payload["company_code"]),
            market_code=str(payload["market_code"]),
            currency_code=str(payload["currency_code"]),
            order_id=str(payload.get("order_id", "")),
            order_name=str(payload.get("order_name", "")),
            external_transaction_id=str(payload["external_transaction_id"]),
            external_payout_id=str(payload.get("external_payout_id", "")),
            transaction_date=str(payload["transaction_date"]),
            payout_date=str(payload.get("payout_date", "")),
            transaction_type=str(payload.get("transaction_type", "")),
            status=str(payload.get("status", "")),
            gross_amount=parse_decimal(payload.get("gross_amount")),
            fee_amount=parse_decimal(payload.get("fee_amount")),
            net_amount=parse_decimal(payload.get("net_amount")),
            chargeback_amount=parse_decimal(payload.get("chargeback_amount")),
            chargeback_fee_amount=parse_decimal(payload.get("chargeback_fee_amount")),
            affects_balance=bool(payload.get("affects_balance", True)),
            is_cancelled=bool(payload.get("is_cancelled", False)),
            is_chargeback=bool(payload.get("is_chargeback", False)),
            payment_reference=str(payload.get("payment_reference", "")),
            customer_reference=str(payload.get("customer_reference", "")),
            raw_payload=dict(payload.get("raw_payload", {}) or {}),
        )


@dataclass(frozen=True)
class PaymentFeeSummaryRow:
    company_code: str
    period_yyyymm: str
    platform: str
    market_code: str
    currency_code: str
    orders_count: int
    transactions_count: int
    gross_amount: Decimal
    fee_amount: Decimal
    chargeback_amount: Decimal
    chargeback_fee_amount: Decimal
    total_cost_amount: Decimal
    net_amount: Decimal
    payout_count: int

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "company_code": self.company_code,
            "period_yyyymm": self.period_yyyymm,
            "platform": self.platform,
            "market_code": self.market_code,
            "currency_code": self.currency_code,
            "orders_count": self.orders_count,
            "transactions_count": self.transactions_count,
            "gross_amount": decimal_to_string(self.gross_amount),
            "fee_amount": decimal_to_string(self.fee_amount),
            "chargeback_amount": decimal_to_string(self.chargeback_amount),
            "chargeback_fee_amount": decimal_to_string(self.chargeback_fee_amount),
            "total_cost_amount": decimal_to_string(self.total_cost_amount),
            "net_amount": decimal_to_string(self.net_amount),
            "payout_count": self.payout_count,
        }

    @classmethod
    def from_json_dict(cls, payload: dict[str, Any]) -> "PaymentFeeSummaryRow":
        return cls(
            company_code=str(payload.get("company_code", "SL")),
            period_yyyymm=str(payload["period_yyyymm"]),
            platform=str(payload["platform"]),
            market_code=str(payload["market_code"]),
            currency_code=str(payload["currency_code"]),
            orders_count=int(payload.get("orders_count", 0)),
            transactions_count=int(payload.get("transactions_count", 0)),
            gross_amount=parse_decimal(payload.get("gross_amount")),
            fee_amount=parse_decimal(payload.get("fee_amount")),
            chargeback_amount=parse_decimal(payload.get("chargeback_amount")),
            chargeback_fee_amount=parse_decimal(payload.get("chargeback_fee_amount")),
            total_cost_amount=parse_decimal(payload.get("total_cost_amount")),
            net_amount=parse_decimal(payload.get("net_amount")),
            payout_count=int(payload.get("payout_count", 0)),
        )


@dataclass(frozen=True)
class PaymentFeeSyncResult:
    platform: str
    transactions_upserted: int
    summaries_rebuilt: int
    date_from: str
    date_to: str


def _text(value: Any) -> str:
    if value in (None, ""):
        return ""
    return str(value)


def _money_text(value: Decimal | int | float | str | None) -> str:
    return decimal_to_string(quantize_money(value))


def _negative_money_text(value: Decimal | int | float | str | None) -> str:
    return decimal_to_string(-quantize_money(value))


def company_name_for_code(company_code: str) -> str:
    normalized = company_code.strip().upper()
    if normalized == "LTD":
        return "Artesta Stores (UK) Ltd"
    if normalized == "INC":
        return "Artesta Inc"
    return "Artesta Store SL"


def extract_as_order_name(*values: Any) -> str:
    for value in values:
        text = _text(value).strip()
        if text.upper().startswith("AS-"):
            return text
    return ""


def normalize_lookup_identifier(value: Any) -> str:
    text = _text(value).strip()
    if not text:
        return ""
    lowered = text.lower()
    if "e+" in lowered or "e-" in lowered:
        try:
            numeric = Decimal(lowered)
        except Exception:
            return text
        text = format(numeric.quantize(Decimal("1")), "f")
    if text.endswith(".0") and text.replace(".", "", 1).isdigit():
        text = text[:-2]
    if text.isdigit():
        stripped = text.lstrip("0")
        return stripped or "0"
    return text


def decimal_to_string(value: Decimal) -> str:
    return format(quantize_money(value), "f")


def quantize_money(value: Decimal | int | float | str | None) -> Decimal:
    return parse_decimal(value).quantize(SUMMARY_QUANTIZER, rounding=ROUND_HALF_UP)


def parse_decimal(value: Decimal | int | float | str | None) -> Decimal:
    if value in (None, ""):
        return API_DECIMAL_ZERO
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value).replace(",", ""))


def parse_datetime(raw: str | datetime | date) -> datetime:
    if isinstance(raw, datetime):
        return raw.astimezone(timezone.utc) if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    if isinstance(raw, date):
        return datetime.combine(raw, time.min, tzinfo=timezone.utc)
    value = str(raw).strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    if "T" not in value:
        return datetime.fromisoformat(f"{value}T00:00:00+00:00")
    parsed = datetime.fromisoformat(value)
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def isoformat_utc(raw: str | datetime | date) -> str:
    return parse_datetime(raw).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def market_code_for_shopify_currency(currency_code: str) -> str:
    normalized = currency_code.strip().upper()
    if normalized == "EUR":
        return "SL-EUR"
    if normalized == "GBP":
        return "UK-GBP"
    if normalized == "USD":
        return "USA-USD"
    return f"SL-{normalized}" if normalized else "SL-UNKNOWN"


def market_code_for_platform(platform: str, currency_code: str) -> str:
    if platform in {SHOPIFY_PLATFORM, PAYPAL_PLATFORM}:
        return market_code_for_shopify_currency(currency_code)
    normalized = currency_code.strip().upper()
    return f"SL-{normalized}" if normalized else "SL-UNKNOWN"


def company_code_for_currency(currency_code: str) -> str:
    normalized = currency_code.strip().upper()
    if normalized == "GBP":
        return "LTD"
    if normalized == "USD":
        return "INC"
    return "SL"


def company_code_for_shopify_currency(currency_code: str) -> str:
    return company_code_for_currency(currency_code)


def summary_period_yyyymm(transaction: PaymentOrderTransaction) -> str | None:
    if transaction.platform == SHOPIFY_PLATFORM:
        if transaction.payout_date:
            return parse_datetime(transaction.payout_date).strftime("%Y%m")
        return transaction.period_yyyymm
    return transaction.period_yyyymm


def summarize_payment_transactions(transactions: list[PaymentOrderTransaction]) -> list[PaymentFeeSummaryRow]:
    buckets: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    for transaction in transactions:
        if not transaction.affects_balance or transaction.is_cancelled:
            continue
        period_yyyymm = summary_period_yyyymm(transaction)
        if not period_yyyymm:
            continue
        key = (
            transaction.company_code,
            period_yyyymm,
            transaction.platform,
            transaction.market_code,
            transaction.currency_code,
        )
        bucket = buckets.setdefault(
            key,
            {
                "order_ids": set(),
                "payout_ids": set(),
                "transactions_count": 0,
                "gross_amount": API_DECIMAL_ZERO,
                "fee_amount": API_DECIMAL_ZERO,
                "chargeback_amount": API_DECIMAL_ZERO,
                "chargeback_fee_amount": API_DECIMAL_ZERO,
                "net_amount": API_DECIMAL_ZERO,
            },
        )
        if transaction.order_id:
            bucket["order_ids"].add(transaction.order_id)
        if transaction.external_payout_id:
            bucket["payout_ids"].add(transaction.external_payout_id)
        bucket["transactions_count"] += 1
        bucket["gross_amount"] += quantize_money(transaction.gross_amount)
        bucket["fee_amount"] += quantize_money(transaction.fee_amount)
        bucket["chargeback_amount"] += quantize_money(transaction.chargeback_amount)
        bucket["chargeback_fee_amount"] += quantize_money(transaction.chargeback_fee_amount)
        bucket["net_amount"] += quantize_money(transaction.net_amount)
    summary_rows: list[PaymentFeeSummaryRow] = []
    for (company_code, period_yyyymm, platform, market_code, currency_code), bucket in sorted(buckets.items()):
        total_cost_amount = (
            bucket["fee_amount"]
            + bucket["chargeback_fee_amount"]
        )
        summary_rows.append(
            PaymentFeeSummaryRow(
                company_code=company_code,
                period_yyyymm=period_yyyymm,
                platform=platform,
                market_code=market_code,
                currency_code=currency_code,
                orders_count=len(bucket["order_ids"]),
                transactions_count=int(bucket["transactions_count"]),
                gross_amount=quantize_money(bucket["gross_amount"]),
                fee_amount=quantize_money(bucket["fee_amount"]),
                chargeback_amount=quantize_money(bucket["chargeback_amount"]),
                chargeback_fee_amount=quantize_money(bucket["chargeback_fee_amount"]),
                total_cost_amount=quantize_money(total_cost_amount),
                net_amount=quantize_money(bucket["net_amount"]),
                payout_count=len(bucket["payout_ids"]),
            )
        )
    return summary_rows


class ShopifyPaymentsClient:
    def __init__(self, config: ShopifyPaymentsConfig) -> None:
        self.config = config
        self._access_token = ""
        self._access_token_expires_at = 0.0

    def list_transactions(self, *, date_from: str, date_to: str) -> list[PaymentOrderTransaction]:
        bundle = self.load_sync_bundle(date_from=date_from, date_to=date_to)
        return list(bundle["transactions"])

    def list_disputes(self, *, date_from: str, date_to: str) -> list[PaymentOrderTransaction]:
        bundle = self.load_sync_bundle(date_from=date_from, date_to=date_to)
        return list(bundle["disputes"])

    def load_sync_bundle(self, *, date_from: str, date_to: str) -> dict[str, list[Any]]:
        balance_transactions = self._fetch_all_balance_transactions(date_from=date_from, date_to=date_to)
        order_ids = sorted(
            {
                node["associatedOrder"]["id"]
                for node in balance_transactions
                if isinstance(node.get("associatedOrder"), dict) and node["associatedOrder"].get("id")
            }
        )
        payout_ids = sorted(
            {
                node["associatedPayout"]["id"]
                for node in balance_transactions
                if isinstance(node.get("associatedPayout"), dict) and node["associatedPayout"].get("id")
            }
        )
        order_map = self._fetch_order_details(order_ids)
        payout_map = self._fetch_payout_details(payout_ids)
        raw_records = [
            build_shopify_payout_record(
                node,
                order_map=order_map,
                payout_map=payout_map,
            )
            for node in balance_transactions
        ]
        normalized: list[PaymentOrderTransaction] = []
        for node in balance_transactions:
            transaction = normalize_shopify_balance_transaction(
                node,
                order_map=order_map,
                payout_map=payout_map,
            )
            if not transaction:
                continue
            normalized.append(transaction)
        disputes = self._fetch_all_disputes(date_from=date_from, date_to=date_to)
        dispute_transactions: list[PaymentOrderTransaction] = []
        for dispute in disputes:
            transaction = normalize_shopify_dispute(dispute)
            if not transaction:
                continue
            dispute_transactions.append(transaction)
        return {
            "raw_records": raw_records,
            "transactions": normalized,
            "disputes": dispute_transactions,
        }

    def build_paypal_order_mapping(self, *, date_from: str, date_to: str) -> dict[str, dict[str, dict[str, str]]]:
        query = """
        query GetPayPalOrders($first: Int!, $after: String, $query: String!) {
          orders(first: $first, after: $after, query: $query, sortKey: CREATED_AT, reverse: true) {
            pageInfo {
              hasNextPage
              endCursor
            }
            nodes {
              id
              name
              paymentGatewayNames
              transactions {
                id
                gateway
                kind
                status
                processedAt
                authorizationCode
                paymentId
                receiptJson
              }
            }
          }
        }
        """
        lookback_start = parse_datetime(date_from) - timedelta(days=90)
        search_query = self._build_shopify_time_range_query(
            field="created_at",
            date_from=lookback_start,
            date_to=date_to,
        )
        start_dt = parse_datetime(date_from)
        end_dt = parse_datetime(date_to).replace(hour=23, minute=59, second=59)
        cursor: str | None = None
        mapping: dict[str, dict[str, dict[str, str]]] = {
            "authorization_code": {},
            "gateway_transaction_id": {},
            "payment_id": {},
        }
        while True:
            payload = self._graphql(query, {"first": 100, "after": cursor, "query": search_query})
            connection = payload.get("orders") or {}
            for order in connection.get("nodes") or []:
                if not isinstance(order, dict):
                    continue
                order_id = _text(order.get("id"))
                order_name = _text(order.get("name"))
                for tx in order.get("transactions") or []:
                    if not isinstance(tx, dict):
                        continue
                    if _text(tx.get("gateway")).lower() != "paypal":
                        continue
                    processed_at = _text(tx.get("processedAt"))
                    if not processed_at:
                        continue
                    processed_dt = parse_datetime(processed_at)
                    if processed_dt < start_dt or processed_dt > end_dt:
                        continue
                    self._register_paypal_order_mapping_identifiers(
                        mapping,
                        order_id=order_id,
                        order_name=order_name,
                        transaction=tx,
                    )
            page_info = connection.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
            if not cursor:
                break
        return mapping

    def _register_paypal_order_mapping_identifiers(
        self,
        mapping: dict[str, dict[str, dict[str, str]]],
        *,
        order_id: str,
        order_name: str,
        transaction: dict[str, Any],
    ) -> None:
        match = {"order_id": order_id, "order_name": order_name}

        def register(bucket: str, value: Any) -> None:
            identifier = normalize_lookup_identifier(value)
            if identifier:
                mapping[bucket].setdefault(identifier, match)

        register("authorization_code", transaction.get("authorizationCode"))
        register("payment_id", transaction.get("paymentId"))
        receipt_json = _text(transaction.get("receiptJson"))
        if not receipt_json:
            return
        try:
            receipt = json.loads(receipt_json)
        except json.JSONDecodeError:
            return
        register("gateway_transaction_id", receipt.get("id"))
        purchase_units = receipt.get("purchase_units") or []
        for unit in purchase_units:
            if not isinstance(unit, dict):
                continue
            register("payment_id", unit.get("invoice_id"))
            custom_id = _text(unit.get("custom_id"))
            if custom_id:
                try:
                    custom_payload = json.loads(custom_id)
                except json.JSONDecodeError:
                    custom_payload = {}
                if isinstance(custom_payload, dict):
                    register("payment_id", custom_payload.get("session_id"))
            payments = unit.get("payments") or {}
            for capture in payments.get("captures") or []:
                if isinstance(capture, dict):
                    register("authorization_code", capture.get("id"))

    def _fetch_all_balance_transactions(self, *, date_from: str, date_to: str) -> list[dict[str, Any]]:
        query = """
        query GetBalanceTransactions($first: Int!, $after: String, $query: String!) {
          shopifyPaymentsAccount {
            balanceTransactions(first: $first, after: $after, query: $query, reverse: true, sortKey: PROCESSED_AT) {
              pageInfo {
                hasNextPage
                endCursor
              }
              nodes {
                id
                type
                test
                transactionDate
                amount {
                  amount
                  currencyCode
                }
                fee {
                  amount
                }
                net {
                  amount
                }
                sourceId
                sourceType
                sourceOrderTransactionId
                associatedOrder {
                  id
                }
                associatedPayout {
                  id
                  status
                }
                adjustmentsOrders {
                  orderTransactionId
                  amount {
                    amount
                  }
                  name
                }
                adjustmentReason
              }
            }
          }
        }
        """
        nodes: list[dict[str, Any]] = []
        cursor: str | None = None
        search_query = self._build_shopify_time_range_query(
            field="processed_at",
            date_from=date_from,
            date_to=date_to,
        )
        while True:
            payload = self._graphql(query, {"first": 100, "after": cursor, "query": search_query})
            account = payload.get("shopifyPaymentsAccount") or {}
            connection = account.get("balanceTransactions") or {}
            nodes.extend(connection.get("nodes") or [])
            page_info = connection.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
            if not cursor:
                break
        return nodes

    def _fetch_order_details(self, order_ids: list[str]) -> dict[str, dict[str, Any]]:
        if not order_ids:
            return {}
        order_map: dict[str, dict[str, Any]] = {}
        query = """
        query GetOrders($ids: [ID!]!) {
          nodes(ids: $ids) {
            ... on Order {
              id
              name
              cancelledAt
              transactions {
                id
                kind
                status
                processedAt
                amountSet {
                  shopMoney {
                    amount
                    currencyCode
                  }
                  presentmentMoney {
                    amount
                    currencyCode
                  }
                }
              }
            }
          }
        }
        """
        for index in range(0, len(order_ids), 50):
            batch_ids = order_ids[index:index + 50]
            payload = self._graphql(query, {"ids": batch_ids})
            for node in payload.get("nodes") or []:
                if isinstance(node, dict) and node.get("id"):
                    order_map[str(node["id"])] = node
        return order_map

    def _fetch_payout_details(self, payout_ids: list[str]) -> dict[str, dict[str, Any]]:
        if not payout_ids:
            return {}
        payout_map: dict[str, dict[str, Any]] = {}
        query = """
        query GetPayouts($first: Int!, $after: String) {
          shopifyPaymentsAccount {
            payouts(first: $first, after: $after) {
              pageInfo {
                hasNextPage
                endCursor
              }
              nodes {
                id
                issuedAt
                status
              }
            }
          }
        }
        """
        cursor: str | None = None
        pending_ids = set(payout_ids)
        while pending_ids:
            payload = self._graphql(query, {"first": 100, "after": cursor})
            account = payload.get("shopifyPaymentsAccount") or {}
            connection = account.get("payouts") or {}
            for node in connection.get("nodes") or []:
                payout_id = str(node.get("id", ""))
                if payout_id in pending_ids:
                    payout_map[payout_id] = node
                    pending_ids.remove(payout_id)
            page_info = connection.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
            if not cursor:
                break
        return payout_map

    def _fetch_all_disputes(self, *, date_from: str, date_to: str) -> list[dict[str, Any]]:
        query = """
        query GetDisputes($first: Int!, $after: String, $query: String!) {
          shopifyPaymentsAccount {
            disputes(first: $first, after: $after, query: $query, reverse: true) {
              pageInfo {
                hasNextPage
                endCursor
              }
              nodes {
                id
                initiatedAt
                finalizedOn
                order {
                  id
                  name
                }
                status
                type
                amount {
                  amount
                  currencyCode
                }
              }
            }
          }
        }
        """
        disputes: list[dict[str, Any]] = []
        cursor: str | None = None
        search_query = self._build_shopify_time_range_query(
            field="initiated_at",
            date_from=date_from,
            date_to=date_to,
        )
        while True:
            payload = self._graphql(query, {"first": 100, "after": cursor, "query": search_query})
            account = payload.get("shopifyPaymentsAccount") or {}
            connection = account.get("disputes") or {}
            page_items = connection.get("nodes") or []
            disputes.extend(page_items)
            page_info = connection.get("pageInfo") or {}
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
            if not cursor:
                break
        return disputes

    def _graphql(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        url = f"https://{self.config.normalized_shop_name}.myshopify.com/admin/api/{self.config.api_version}/graphql.json"
        body = json.dumps({"query": query, "variables": variables}).encode("utf-8")
        request = Request(
            url,
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "X-Shopify-Access-Token": self._get_access_token(),
            },
        )
        try:
            with urlopen(request, timeout=60) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Shopify GraphQL request failed: {exc.code} {detail}") from exc
        if payload.get("errors"):
            raise RuntimeError(f"Shopify GraphQL returned errors: {payload['errors']}")
        return payload.get("data") or {}

    def _get_access_token(self) -> str:
        now_ts = datetime.now(timezone.utc).timestamp()
        if self._access_token and now_ts < self._access_token_expires_at - 60:
            return self._access_token
        body = urlencode(
            {
                "grant_type": "client_credentials",
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret,
            }
        ).encode("utf-8")
        request = Request(
            f"https://{self.config.normalized_shop_name}.myshopify.com/admin/oauth/access_token",
            data=body,
            method="POST",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        try:
            with urlopen(request, timeout=30) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Shopify token request failed: {exc.code} {detail}") from exc
        self._access_token = str(payload["access_token"])
        expires_in = int(payload.get("expires_in", 0) or 0)
        self._access_token_expires_at = now_ts + expires_in
        return self._access_token

    def _build_shopify_time_range_query(self, *, field: str, date_from: str, date_to: str) -> str:
        start = isoformat_utc(date_from)
        end = isoformat_utc(parse_datetime(date_to).replace(hour=23, minute=59, second=59))
        return f"{field}:>={start} {field}:<={end}"

class PayPalClient:
    def __init__(self, config: PayPalConfig) -> None:
        self.config = config

    def list_transactions(self, *, date_from: str, date_to: str) -> list[PaymentOrderTransaction]:
        bundle = self.load_sync_bundle(date_from=date_from, date_to=date_to)
        return list(bundle["transactions"])

    def list_disputes(self, *, date_from: str, date_to: str) -> list[PaymentOrderTransaction]:
        bundle = self.load_sync_bundle(date_from=date_from, date_to=date_to)
        return list(bundle["disputes"])

    def load_sync_bundle(self, *, date_from: str, date_to: str) -> dict[str, list[Any]]:
        token = self._get_access_token()
        items = self._fetch_transactions(token=token, date_from=date_from, date_to=date_to)
        raw_records = [build_paypal_transaction_record(item) for item in items]
        normalized_transactions = [tx for tx in (normalize_paypal_transaction(item) for item in items) if tx]
        try:
            dispute_items = self._fetch_disputes(token=token, date_from=date_from, date_to=date_to)
        except RuntimeError as exc:
            if "NOT_AUTHORIZED" in str(exc):
                dispute_items = []
            else:
                raise
        normalized_disputes = [tx for tx in (normalize_paypal_dispute(item) for item in dispute_items) if tx]
        return {
            "raw_records": raw_records,
            "transactions": normalized_transactions,
            "disputes": normalized_disputes,
        }

    def _get_access_token(self) -> str:
        credentials = f"{self.config.client_id}:{self.config.client_secret}".encode("utf-8")
        headers = {
            "Authorization": f"Basic {base64.b64encode(credentials).decode('ascii')}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        request = Request(
            f"{self.config.base_url}/v1/oauth2/token",
            data=b"grant_type=client_credentials",
            method="POST",
            headers=headers,
        )
        try:
            with urlopen(request, timeout=30) as response:
                payload = json.loads(response.read().decode("utf-8"))
                return str(payload["access_token"])
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"PayPal token request failed: {exc.code} {detail}") from exc

    def _fetch_transactions(self, *, token: str, date_from: str, date_to: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for window_start, window_end in self._iter_paypal_windows(date_from=date_from, date_to=date_to):
            page = 1
            while True:
                query = urlencode(
                    {
                        "start_date": isoformat_utc(window_start),
                        "end_date": isoformat_utc(window_end.replace(hour=23, minute=59, second=59)),
                        "fields": "all",
                        "balance_affecting_records_only": "Y",
                        "page_size": 100,
                        "page": page,
                    }
                )
                response = self._json_get(f"{self.config.base_url}/v1/reporting/transactions?{query}", token=token)
                page_items = response.get("transaction_details") or []
                items.extend(page_items)
                total_pages = int(response.get("total_pages", 1) or 1)
                if page >= total_pages or not page_items:
                    break
                page += 1
        return items

    def _fetch_disputes(self, *, token: str, date_from: str, date_to: str) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for window_start, window_end in self._iter_paypal_windows(date_from=date_from, date_to=date_to):
            page = 1
            while True:
                query = urlencode(
                    {
                        "page_size": 50,
                        "page": page,
                        "update_time_after": isoformat_utc(window_start),
                        "update_time_before": isoformat_utc(window_end.replace(hour=23, minute=59, second=59)),
                    }
                )
                response = self._json_get(f"{self.config.base_url}/v1/customer/disputes?{query}", token=token)
                page_items = response.get("items") or response.get("disputes") or []
                items.extend(page_items)
                total_pages = int(response.get("total_pages", 1) or 1)
                if page >= total_pages or not page_items:
                    break
                page += 1
        return items

    def _iter_paypal_windows(self, *, date_from: str, date_to: str) -> list[tuple[datetime, datetime]]:
        start = parse_datetime(date_from)
        requested_end = parse_datetime(date_to).replace(hour=23, minute=59, second=59)
        today_end = datetime.now(timezone.utc).replace(hour=23, minute=59, second=59, microsecond=0)
        end = min(requested_end, today_end)
        if start > end:
            return []
        windows: list[tuple[datetime, datetime]] = []
        cursor = start
        while cursor <= end:
            window_end = min(cursor + timedelta(days=30, hours=23, minutes=59, seconds=59), end)
            windows.append((cursor, window_end))
            cursor = window_end + timedelta(seconds=1)
        return windows

    def _json_get(self, url: str, *, token: str) -> dict[str, Any]:
        request = Request(
            url,
            method="GET",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urlopen(request, timeout=60) as response:
                body = response.read().decode("utf-8")
                return json.loads(body) if body else {}
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"PayPal request failed: {exc.code} {detail}") from exc


def build_shopify_payout_record(
    payload: dict[str, Any],
    *,
    order_map: dict[str, dict[str, Any]],
    payout_map: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    currency_code = _text(((payload.get("amount") or {}).get("currencyCode"))).upper()
    company_code = company_code_for_shopify_currency(currency_code)
    payout = payload.get("associatedPayout") or {}
    payout_id = _text(payout.get("id"))
    payout_details = payout_map.get(payout_id) or {}
    payout_date = _text(payout_details.get("issuedAt"))
    order_id = ""
    order_name = ""
    transaction_date = _text(payload.get("transactionDate"))
    presentment_amount = ""
    presentment_currency = ""
    checkout = ""
    payment_method_name = ""
    associated_order = payload.get("associatedOrder") or {}
    if associated_order.get("id"):
        order_id = _text(associated_order.get("id"))
        order_payload = order_map.get(order_id) or {}
        order_name = _text(order_payload.get("name"))
        transactions_payload = order_payload.get("transactions") or []
        tx_nodes = list(transactions_payload.get("nodes") or []) if isinstance(transactions_payload, dict) else list(transactions_payload)
        matching_tx = next(
            (node for node in tx_nodes if _text(node.get("id")) == _text(payload.get("sourceOrderTransactionId"))),
            None,
        )
        if matching_tx:
            transaction_date = _text(matching_tx.get("processedAt")) or transaction_date
            checkout = _text(matching_tx.get("id"))
            payment_method_name = _text(matching_tx.get("gateway"))
            presentment_money = ((matching_tx.get("amountSet") or {}).get("presentmentMoney") or {})
            presentment_amount = _money_text(presentment_money.get("amount")) if presentment_money.get("amount") not in (None, "") else ""
            presentment_currency = _text(presentment_money.get("currencyCode")).upper()
    amount_payload = payload.get("amount") or {}
    fee_payload = payload.get("fee") or {}
    net_payload = payload.get("net") or {}
    raw_type = _text(payload.get("type")).lower()
    source_type = _text(payload.get("sourceType")).lower()
    return {
        "source_record_id": _text(payload.get("id")),
        "transaction_date": isoformat_utc(transaction_date) if transaction_date else "",
        "type": raw_type or source_type,
        "order_id": order_id,
        "order_name": order_name,
        "card_brand": "",
        "card_source": "",
        "payout_status": _text(payout.get("status")).lower(),
        "payout_date": isoformat_utc(payout_date) if payout_date else "",
        "payout_id": payout_id,
        "available_on": "",
        "amount": _money_text(amount_payload.get("amount")),
        "fee": _money_text(fee_payload.get("amount")),
        "net": _money_text(net_payload.get("amount")),
        "checkout": checkout,
        "payment_method_name": payment_method_name,
        "presentment_amount": presentment_amount,
        "presentment_currency": presentment_currency,
        "currency": currency_code,
        "vat": "0.00",
        "business_entity_name": company_name_for_code(company_code),
        "business_entity_id": "",
        "company_code": company_code,
        "market_code": market_code_for_shopify_currency(currency_code),
        "raw_payload": payload,
    }


def build_paypal_transaction_record(payload: dict[str, Any]) -> dict[str, Any]:
    transaction_info = payload.get("transaction_info") or {}
    payer_info = payload.get("payer_info") or {}
    cart_info = payload.get("cart_info") or {}
    shipping_info = payload.get("shipping_info") or {}
    address_status = _text((shipping_info.get("address_status") or payer_info.get("address_status")))
    address_payload = shipping_info.get("address") or payer_info.get("address") or {}
    transaction_date = _text(transaction_info.get("transaction_initiation_date"))
    transaction_dt = parse_datetime(transaction_date) if transaction_date else None
    gross_value = quantize_money((transaction_info.get("transaction_amount") or {}).get("value"))
    fee_value = quantize_money((transaction_info.get("fee_amount") or {}).get("value"))
    net_value = quantize_money(gross_value + fee_value)
    balance_value = (transaction_info.get("ending_balance") or {}).get("value")
    item_details = (cart_info.get("item_details") or [])
    first_item = item_details[0] if item_details and isinstance(item_details[0], dict) else {}
    order_name = extract_as_order_name(
        cart_info.get("invoice_id"),
        cart_info.get("custom_field"),
        transaction_info.get("invoice_id"),
        payload.get("order_id"),
        payload.get("order_name"),
    )
    currency_code = _text((transaction_info.get("transaction_amount") or {}).get("currency_code")).upper()
    company_code = company_code_for_currency(currency_code)
    return {
        "source_record_id": _text(transaction_info.get("transaction_id")),
        "transaction_date": isoformat_utc(transaction_dt) if transaction_dt else "",
        "fecha": transaction_dt.strftime("%d/%m/%Y") if transaction_dt else "",
        "hora": transaction_dt.strftime("%H:%M:%S") if transaction_dt else "",
        "zona_horaria": "UTC",
        "nombre": _text(payer_info.get("account_holder_name") or payer_info.get("payer_name") or payer_info.get("payer_full_name")),
        "tipo": _text(transaction_info.get("transaction_event_code") or transaction_info.get("transaction_subject") or transaction_info.get("transaction_event_type")),
        "estado": _text(transaction_info.get("transaction_status")).upper(),
        "divisa": currency_code,
        "bruto": _money_text(gross_value),
        "tarifa": _money_text(fee_value),
        "neto": _money_text(net_value),
        "sender_email": _text(payer_info.get("email_address")),
        "recipient_email": _text((payload.get("paypal_account") or {}).get("email_address") or payload.get("receivable_email") or payload.get("payee_email")),
        "transaction_id": _text(transaction_info.get("transaction_id")),
        "shipping_address": _text(shipping_info.get("name")) or _text(payload.get("shipping_info")),
        "address_status": address_status,
        "item_name": _text(first_item.get("item_name") or cart_info.get("item_name") or transaction_info.get("transaction_subject")),
        "item_id": _text(first_item.get("item_code")),
        "shipping_amount": _money_text((transaction_info.get("shipping_info") or {}).get("value") or (transaction_info.get("shipping_amount") or {}).get("value") or 0),
        "insurance_amount": _money_text((transaction_info.get("insurance_amount") or {}).get("value") or 0),
        "sales_tax_amount": _money_text((transaction_info.get("sales_tax_amount") or {}).get("value") or 0),
        "option1_name": "",
        "option1_value": "",
        "option2_name": "",
        "option2_value": "",
        "reference_transaction_id": _text(transaction_info.get("paypal_reference_id") or transaction_info.get("reference_id")),
        "invoice_number": _text(cart_info.get("invoice_id")),
        "custom_number": _text(cart_info.get("custom_field")),
        "quantity": _text(first_item.get("item_quantity") or cart_info.get("item_quantity")),
        "receipt_id": _text(transaction_info.get("transaction_receipt_id")),
        "balance_amount": _money_text(balance_value) if balance_value not in (None, "") else "",
        "address_line_1": _text(address_payload.get("line1")),
        "address_line_2": _text(address_payload.get("line2")),
        "city": _text(address_payload.get("city")),
        "region": _text(address_payload.get("state")),
        "postal_code": _text(address_payload.get("postal_code")),
        "country": _text(address_payload.get("country_name") or address_payload.get("country_code")),
        "contact_phone": _text(payer_info.get("phone_number")),
        "subject": _text(transaction_info.get("transaction_subject")),
        "note": _text(transaction_info.get("transaction_note")),
        "country_code": _text(address_payload.get("country_code")),
        "balance_impact": "Crédito" if gross_value >= API_DECIMAL_ZERO else "Débito",
        "order_number": _text(cart_info.get("invoice_id") or cart_info.get("custom_field")),
        "shopify_order_name": order_name,
        "company_code": company_code,
        "market_code": market_code_for_platform(PAYPAL_PLATFORM, currency_code),
        "raw_payload": payload,
    }


class PaymentFeeService:
    def __init__(
        self,
        store: Any,
        *,
        shopify_client: ShopifyPaymentsClient | None = None,
        paypal_client: PayPalClient | None = None,
    ) -> None:
        self.store = store
        self.shopify_client = shopify_client
        self.paypal_client = paypal_client

    def sync(self, *, date_from: str, date_to: str, platform: str | None = None) -> list[PaymentFeeSyncResult]:
        requested = [platform] if platform else [SHOPIFY_PLATFORM, PAYPAL_PLATFORM]
        results: list[PaymentFeeSyncResult] = []
        paypal_order_mapping = (
            self.shopify_client.build_paypal_order_mapping(date_from=date_from, date_to=date_to)
            if self.shopify_client is not None and PAYPAL_PLATFORM in requested
            else {}
        )
        for item in requested:
            bundle = self._load_platform_bundle(item, date_from=date_from, date_to=date_to)
            if item == PAYPAL_PLATFORM and paypal_order_mapping:
                bundle = self._enrich_paypal_bundle_with_shopify_orders(bundle, paypal_order_mapping)
            transactions = list(bundle["transactions"]) + list(bundle["disputes"])
            self._persist_raw_records(item, date_from=date_from, date_to=date_to, records=list(bundle["raw_records"]))
            self.store.delete_payment_order_transactions_range(
                platform=item,
                date_from=date_from,
                date_to=date_to,
            )
            upserted = self.store.upsert_payment_order_transactions(transactions)
            company_codes = sorted({transaction.company_code for transaction in transactions}) or ["SL"]
            summaries_rebuilt = 0
            for company_code in company_codes:
                summaries_rebuilt += self.store.rebuild_payment_fee_monthly_summary(company_code=company_code, platform=item)
            results.append(
                PaymentFeeSyncResult(
                    platform=item,
                    transactions_upserted=upserted,
                    summaries_rebuilt=summaries_rebuilt,
                    date_from=date_from,
                    date_to=date_to,
                )
            )
        return results

    def _load_platform_bundle(self, platform: str, *, date_from: str, date_to: str) -> dict[str, list[Any]]:
        if platform == SHOPIFY_PLATFORM:
            if self.shopify_client is None:
                raise RuntimeError("Shopify payments integration is not configured.")
            return self.shopify_client.load_sync_bundle(date_from=date_from, date_to=date_to)
        if platform == PAYPAL_PLATFORM:
            if self.paypal_client is None:
                raise RuntimeError("PayPal integration is not configured.")
            return self.paypal_client.load_sync_bundle(date_from=date_from, date_to=date_to)
        raise ValueError(f"Unsupported platform: {platform}")

    def _persist_raw_records(self, platform: str, *, date_from: str, date_to: str, records: list[dict[str, Any]]) -> None:
        if platform == SHOPIFY_PLATFORM:
            self.store.delete_shopify_payout_transactions_range(date_from=date_from, date_to=date_to)
            self.store.upsert_shopify_payout_transactions(records)
            return
        if platform == PAYPAL_PLATFORM:
            self.store.delete_paypal_transactions_range(date_from=date_from, date_to=date_to)
            self.store.upsert_paypal_transactions_raw(records)
            return
        raise ValueError(f"Unsupported platform: {platform}")

    def _enrich_paypal_bundle_with_shopify_orders(
        self,
        bundle: dict[str, list[Any]],
        mapping: dict[str, dict[str, dict[str, str]]],
    ) -> dict[str, list[Any]]:
        raw_records = [self._enrich_paypal_raw_record(record, mapping) for record in bundle["raw_records"]]
        transactions = [self._enrich_paypal_transaction(transaction, mapping) for transaction in bundle["transactions"]]
        disputes = [self._enrich_paypal_transaction(transaction, mapping) for transaction in bundle["disputes"]]
        raw_records, transactions, disputes = self._propagate_paypal_reference_matches(raw_records, transactions, disputes)
        return {
            "raw_records": raw_records,
            "transactions": transactions,
            "disputes": disputes,
        }

    def _propagate_paypal_reference_matches(
        self,
        raw_records: list[dict[str, Any]],
        transactions: list[PaymentOrderTransaction],
        disputes: list[PaymentOrderTransaction],
    ) -> tuple[list[dict[str, Any]], list[PaymentOrderTransaction], list[PaymentOrderTransaction]]:
        matched_by_transaction_id: dict[str, dict[str, str]] = {}
        matched_by_reference_id: dict[str, dict[str, str]] = {}
        for record in raw_records:
            order_name = _text(record.get("shopify_order_name"))
            if not order_name.startswith("AS-"):
                continue
            match = {
                "order_id": _text(record.get("shopify_order_id")),
                "order_name": order_name,
            }
            txid = normalize_lookup_identifier(record.get("transaction_id") or record.get("source_record_id"))
            if txid:
                matched_by_transaction_id.setdefault(txid, match)
            reference_id = normalize_lookup_identifier(record.get("reference_transaction_id"))
            if reference_id:
                matched_by_reference_id.setdefault(reference_id, match)

        def apply_to_raw(record: dict[str, Any]) -> dict[str, Any]:
            if _text(record.get("shopify_order_name")).startswith("AS-"):
                return record
            reference_id = normalize_lookup_identifier(record.get("reference_transaction_id"))
            match = matched_by_reference_id.get(reference_id) or matched_by_transaction_id.get(reference_id)
            if not match:
                return record
            enriched = dict(record)
            enriched["shopify_order_name"] = match["order_name"]
            enriched["shopify_order_id"] = match["order_id"]
            txid = normalize_lookup_identifier(record.get("transaction_id") or record.get("source_record_id"))
            if txid:
                matched_by_transaction_id.setdefault(txid, match)
            if reference_id:
                matched_by_reference_id.setdefault(reference_id, match)
            return enriched

        def apply_to_transaction(transaction: PaymentOrderTransaction) -> PaymentOrderTransaction:
            if transaction.order_name.startswith("AS-"):
                return transaction
            raw_payload = transaction.raw_payload or {}
            transaction_info = raw_payload.get("transaction_info") or {}
            reference_id = normalize_lookup_identifier(
                transaction_info.get("paypal_reference_id")
                or transaction_info.get("reference_id")
            )
            match = matched_by_reference_id.get(reference_id) or matched_by_transaction_id.get(reference_id)
            if not match:
                return transaction
            txid = normalize_lookup_identifier(transaction.external_transaction_id)
            if txid:
                matched_by_transaction_id.setdefault(txid, match)
            if reference_id:
                matched_by_reference_id.setdefault(reference_id, match)
            return replace(
                transaction,
                order_id=match["order_id"] or transaction.order_id,
                order_name=match["order_name"] or transaction.order_name,
            )

        enriched_raw_records = [apply_to_raw(record) for record in raw_records]
        enriched_transactions = [apply_to_transaction(transaction) for transaction in transactions]
        enriched_disputes = [apply_to_transaction(transaction) for transaction in disputes]
        return enriched_raw_records, enriched_transactions, enriched_disputes

    def _enrich_paypal_raw_record(self, record: dict[str, Any], mapping: dict[str, dict[str, dict[str, str]]]) -> dict[str, Any]:
        match = self._resolve_paypal_shopify_order(record, mapping)
        if not match:
            return record
        enriched = dict(record)
        enriched["shopify_order_name"] = match["order_name"]
        enriched["shopify_order_id"] = match["order_id"]
        return enriched

    def _enrich_paypal_transaction(
        self,
        transaction: PaymentOrderTransaction,
        mapping: dict[str, dict[str, dict[str, str]]],
    ) -> PaymentOrderTransaction:
        match = self._resolve_paypal_shopify_order(transaction.raw_payload or {}, mapping)
        if not match:
            return transaction
        return replace(
            transaction,
            order_id=match["order_id"] or transaction.order_id,
            order_name=match["order_name"] or transaction.order_name,
        )

    def _resolve_paypal_shopify_order(
        self,
        payload: dict[str, Any],
        mapping: dict[str, dict[str, dict[str, str]]],
    ) -> dict[str, str] | None:
        transaction_id = normalize_lookup_identifier(payload.get("transaction_id"))
        invoice_number = normalize_lookup_identifier(payload.get("invoice_number"))
        raw_payload = payload.get("raw_payload") if isinstance(payload.get("raw_payload"), dict) else payload
        transaction_info = raw_payload.get("transaction_info") or {}
        cart_info = raw_payload.get("cart_info") or {}
        if not transaction_id:
            transaction_id = normalize_lookup_identifier(payload.get("source_record_id") or transaction_info.get("transaction_id"))
        if not invoice_number:
            invoice_number = normalize_lookup_identifier(cart_info.get("invoice_id") or transaction_info.get("invoice_id"))

        if transaction_id and transaction_id in mapping.get("authorization_code", {}):
            return mapping["authorization_code"][transaction_id]
        if transaction_id and transaction_id in mapping.get("gateway_transaction_id", {}):
            return mapping["gateway_transaction_id"][transaction_id]
        if invoice_number and invoice_number in mapping.get("payment_id", {}):
            return mapping["payment_id"][invoice_number]

        for candidate in self._paypal_candidate_identifiers_from_payload(payload):
            key = normalize_lookup_identifier(candidate)
            if key and key in mapping.get("authorization_code", {}):
                return mapping["authorization_code"][key]
            if key and key in mapping.get("gateway_transaction_id", {}):
                return mapping["gateway_transaction_id"][key]
            if key and key in mapping.get("payment_id", {}):
                return mapping["payment_id"][key]
        return None

    def _paypal_candidate_identifiers_from_payload(self, payload: dict[str, Any]) -> list[str]:
        candidates: list[str] = []
        raw_payload = payload.get("raw_payload") if isinstance(payload.get("raw_payload"), dict) else payload
        transaction_info = raw_payload.get("transaction_info") or {}
        cart_info = raw_payload.get("cart_info") or {}
        candidates.extend(
            [
                _text(transaction_info.get("transaction_id")),
                _text(transaction_info.get("paypal_reference_id")),
                _text(transaction_info.get("reference_id")),
                _text(transaction_info.get("invoice_id")),
                _text(cart_info.get("invoice_id")),
                _text(cart_info.get("custom_field")),
                _text(payload.get("invoice_number")),
                _text(payload.get("custom_number")),
                _text(payload.get("order_number")),
            ]
        )
        custom_values = [_text(cart_info.get("custom_field")), _text(transaction_info.get("custom_field")), _text(payload.get("custom_number"))]
        for custom_value in custom_values:
            if not custom_value:
                continue
            try:
                custom_payload = json.loads(custom_value)
            except json.JSONDecodeError:
                continue
            if isinstance(custom_payload, dict):
                candidates.append(_text(custom_payload.get("session_id")))
        return [candidate for candidate in candidates if candidate]


def normalize_shopify_balance_transaction(
    payload: dict[str, Any],
    *,
    order_map: dict[str, dict[str, Any]],
    payout_map: dict[str, dict[str, Any]],
) -> PaymentOrderTransaction | None:
    if payload.get("test"):
        return None
    transaction_type = str(payload.get("type", "")).upper()
    source_type = str(payload.get("sourceType", "")).upper()
    if transaction_type != "CHARGE" or source_type != "CHARGE":
        return None
    order_id = ""
    order_name = ""
    transaction_date = str(payload.get("transactionDate") or "")
    is_cancelled = False
    associated_order = payload.get("associatedOrder")
    source_order_transaction_id = str(payload.get("sourceOrderTransactionId") or "")
    if isinstance(associated_order, dict) and associated_order.get("id"):
        order_id = str(associated_order["id"])
        order_payload = order_map.get(order_id) or {}
        order_name = str(order_payload.get("name", ""))
        is_cancelled = bool(order_payload.get("cancelledAt"))
        transactions_payload = order_payload.get("transactions") or []
        if isinstance(transactions_payload, dict):
            tx_nodes = list(transactions_payload.get("nodes") or [])
        else:
            tx_nodes = list(transactions_payload)
        matching_node = next((node for node in tx_nodes if str(node.get("id")) == source_order_transaction_id), None)
        if matching_node:
            transaction_date = str(matching_node.get("processedAt", ""))
            tx_status = str(matching_node.get("status", "")).upper()
            tx_kind = str(matching_node.get("kind", "")).upper()
            is_cancelled = is_cancelled or tx_status in {"VOIDED", "FAILURE"}
            if tx_kind and tx_kind not in {"SALE", "CAPTURE"}:
                return None
        elif tx_nodes:
            transaction_date = str(tx_nodes[0].get("processedAt", ""))
    if not transaction_date:
        return None
    currency_code = str(((payload.get("amount") or {}).get("currencyCode")) or "").upper()
    payout = payload.get("associatedPayout") or {}
    payout_id = str(payout.get("id", ""))
    payout_date = str((payout_map.get(payout_id) or {}).get("issuedAt", ""))
    gross_amount = quantize_money((payload.get("amount") or {}).get("amount"))
    fee_amount = quantize_money((payload.get("fee") or {}).get("amount"))
    net_amount = quantize_money((payload.get("net") or {}).get("amount"))
    status = str(payout.get("status") or "")
    affects_balance = source_type not in {"PAYOUT_FAILURE"} and transaction_type not in {"RESERVE_HOLD"}
    return PaymentOrderTransaction(
        id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"payment-tx:{SHOPIFY_PLATFORM}:{payload.get('id')}")),
        platform=SHOPIFY_PLATFORM,
        company_code=company_code_for_shopify_currency(currency_code),
        market_code=market_code_for_shopify_currency(currency_code),
        currency_code=currency_code,
        order_id=order_id,
        order_name=order_name,
        external_transaction_id=str(payload.get("id", "")),
        external_payout_id=payout_id,
        transaction_date=isoformat_utc(transaction_date),
        payout_date=isoformat_utc(payout_date) if payout_date else "",
        transaction_type=transaction_type.lower(),
        status=status.lower(),
        gross_amount=gross_amount,
        fee_amount=fee_amount,
        net_amount=net_amount,
        affects_balance=affects_balance,
        is_cancelled=is_cancelled,
        payment_reference=source_order_transaction_id or str(payload.get("sourceId", "")),
        raw_payload=payload,
    )


def normalize_shopify_dispute(payload: dict[str, Any]) -> PaymentOrderTransaction | None:
    dispute_type = str(payload.get("type", "")).lower()
    if dispute_type not in {"chargeback", "inquiry"}:
        return None
    created_at = payload.get("initiatedAt") or payload.get("initiated_at") or payload.get("created_at")
    if not created_at:
        return None
    currency_code = str(payload.get("currency") or (payload.get("amount") or {}).get("currencyCode") or "").upper()
    amount = payload.get("amount")
    amount_value = amount.get("amount") if isinstance(amount, dict) else amount
    status = str(payload.get("status", "")).lower()
    chargeback_amount = API_DECIMAL_ZERO if status == "won" else quantize_money(amount_value)
    chargeback_fee_amount = quantize_money(
        payload.get("fee_amount")
        or payload.get("chargeback_fee_amount")
        or payload.get("chargebackFeeAmount")
    )
    order_payload = payload.get("order") or {}
    order_id = str(payload.get("order_id") or payload.get("orderId") or order_payload.get("id") or "")
    order_name = str(order_payload.get("name") or "")
    transaction_reference = str(payload.get("transaction_id") or payload.get("id") or "")
    return PaymentOrderTransaction(
        id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"payment-dispute:{SHOPIFY_PLATFORM}:{payload.get('id')}")),
        platform=SHOPIFY_PLATFORM,
        company_code=company_code_for_shopify_currency(currency_code),
        market_code=market_code_for_shopify_currency(currency_code),
        currency_code=currency_code,
        order_id=order_id,
        order_name=order_name,
        external_transaction_id=f"shopify-dispute:{payload.get('id')}",
        external_payout_id="",
        transaction_date=isoformat_utc(created_at),
        payout_date="",
        transaction_type=dispute_type,
        status=status,
        gross_amount=API_DECIMAL_ZERO,
        fee_amount=API_DECIMAL_ZERO,
        net_amount=quantize_money(-(chargeback_amount + chargeback_fee_amount)),
        chargeback_amount=chargeback_amount,
        chargeback_fee_amount=chargeback_fee_amount,
        affects_balance=True,
        is_cancelled=False,
        is_chargeback=True,
        payment_reference=transaction_reference,
        raw_payload=payload,
    )


def normalize_paypal_transaction(payload: dict[str, Any]) -> PaymentOrderTransaction | None:
    transaction_info = payload.get("transaction_info") or {}
    status = str(transaction_info.get("transaction_status") or "").upper()
    if status == "V":
        return None
    transaction_date = str(transaction_info.get("transaction_initiation_date") or "")
    if not transaction_date:
        return None
    external_transaction_id = str(transaction_info.get("transaction_id") or "")
    if not external_transaction_id:
        return None
    event_code = str(transaction_info.get("transaction_event_code") or "").upper()
    currency_code = str((transaction_info.get("transaction_amount") or {}).get("currency_code") or "").upper()
    company_code = company_code_for_currency(currency_code)
    transaction_amount = quantize_money((transaction_info.get("transaction_amount") or {}).get("value"))
    fee_value = quantize_money((transaction_info.get("fee_amount") or {}).get("value"))
    cart_info = payload.get("cart_info") or {}
    invoice_id = str(cart_info.get("invoice_id") or "")
    payment_reference = str(transaction_info.get("paypal_reference_id") or transaction_info.get("reference_id") or "")
    customer_reference = str((payload.get("payer_info") or {}).get("email_address") or "")

    if event_code == "T0006":
        gross_amount = abs(transaction_amount)
        fee_amount = abs(fee_value)
        net_amount_raw = transaction_info.get("net_amount")
        if isinstance(net_amount_raw, dict) and net_amount_raw.get("value") not in (None, ""):
            net_amount = abs(quantize_money(net_amount_raw.get("value")))
        else:
            net_amount = quantize_money(gross_amount - fee_amount)
        return PaymentOrderTransaction(
            id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"payment-tx:{PAYPAL_PLATFORM}:{external_transaction_id}")),
            platform=PAYPAL_PLATFORM,
            company_code=company_code,
            market_code=market_code_for_platform(PAYPAL_PLATFORM, currency_code),
            currency_code=currency_code,
            order_id=invoice_id,
            order_name=invoice_id,
            external_transaction_id=external_transaction_id,
            external_payout_id=str(transaction_info.get("reference_id") or ""),
            transaction_date=isoformat_utc(transaction_date),
            payout_date="",
            transaction_type=event_code.lower(),
            status=status.lower(),
            gross_amount=gross_amount,
            fee_amount=fee_amount,
            net_amount=net_amount,
            affects_balance=True,
            is_cancelled=False,
            is_chargeback=False,
            payment_reference=payment_reference,
            customer_reference=customer_reference,
            raw_payload=payload,
        )

    if event_code in {"T1106", "T1201", "T1207"}:
        chargeback_amount = abs(transaction_amount)
        chargeback_fee_amount = abs(fee_value)
        return PaymentOrderTransaction(
            id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"payment-tx:{PAYPAL_PLATFORM}:{external_transaction_id}")),
            platform=PAYPAL_PLATFORM,
            company_code=company_code,
            market_code=market_code_for_platform(PAYPAL_PLATFORM, currency_code),
            currency_code=currency_code,
            order_id=invoice_id or payment_reference,
            order_name=invoice_id or payment_reference,
            external_transaction_id=external_transaction_id,
            external_payout_id="",
            transaction_date=isoformat_utc(transaction_date),
            payout_date="",
            transaction_type=event_code.lower(),
            status=status.lower(),
            gross_amount=API_DECIMAL_ZERO,
            fee_amount=API_DECIMAL_ZERO,
            net_amount=quantize_money(-(chargeback_amount + chargeback_fee_amount)),
            chargeback_amount=chargeback_amount,
            chargeback_fee_amount=chargeback_fee_amount,
            affects_balance=True,
            is_cancelled=False,
            is_chargeback=True,
            payment_reference=payment_reference,
            customer_reference=customer_reference,
            raw_payload=payload,
        )

    if event_code == "T0114":
        chargeback_fee_amount = abs(transaction_amount)
        return PaymentOrderTransaction(
            id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"payment-tx:{PAYPAL_PLATFORM}:{external_transaction_id}")),
            platform=PAYPAL_PLATFORM,
            company_code=company_code,
            market_code=market_code_for_platform(PAYPAL_PLATFORM, currency_code),
            currency_code=currency_code,
            order_id=invoice_id or payment_reference,
            order_name=invoice_id or payment_reference,
            external_transaction_id=external_transaction_id,
            external_payout_id="",
            transaction_date=isoformat_utc(transaction_date),
            payout_date="",
            transaction_type=event_code.lower(),
            status=status.lower(),
            gross_amount=API_DECIMAL_ZERO,
            fee_amount=API_DECIMAL_ZERO,
            net_amount=quantize_money(-chargeback_fee_amount),
            chargeback_amount=API_DECIMAL_ZERO,
            chargeback_fee_amount=chargeback_fee_amount,
            affects_balance=True,
            is_cancelled=False,
            is_chargeback=True,
            payment_reference=payment_reference,
            customer_reference=customer_reference,
            raw_payload=payload,
        )

    if event_code in {"T1107", "T1110", "T1111", "T1105", "T1503", "T0001", "T0400", "T0403", "T1114"}:
        return None

    return None


def normalize_paypal_dispute(payload: dict[str, Any]) -> PaymentOrderTransaction | None:
    dispute_id = str(payload.get("dispute_id") or "")
    if not dispute_id:
        return None
    create_time = payload.get("create_time") or payload.get("update_time")
    if not create_time:
        return None
    amount_payload = payload.get("dispute_amount") or payload.get("disputed_amount") or {}
    amount_value = amount_payload.get("value") if isinstance(amount_payload, dict) else amount_payload
    currency_code = str(
        amount_payload.get("currency_code") if isinstance(amount_payload, dict) else payload.get("currency_code") or ""
    ).upper()
    company_code = company_code_for_currency(currency_code)
    status = str(payload.get("status") or "").lower()
    dispute_outcome = str(payload.get("dispute_outcome") or status).lower()
    chargeback_amount = API_DECIMAL_ZERO if dispute_outcome in {"resolved_buyer_favorable_reversed", "resolved_seller_favorable", "won"} else quantize_money(amount_value)
    chargeback_fee_amount = quantize_money(payload.get("fee_amount") or payload.get("chargeback_fee_amount"))
    disputed_transactions = payload.get("disputed_transactions") or []
    base_transaction_id = ""
    if disputed_transactions and isinstance(disputed_transactions[0], dict):
        base_transaction_id = str(
            disputed_transactions[0].get("seller_transaction_id") or disputed_transactions[0].get("transaction_id") or ""
        )
    return PaymentOrderTransaction(
        id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"payment-dispute:{PAYPAL_PLATFORM}:{dispute_id}")),
        platform=PAYPAL_PLATFORM,
        company_code=company_code,
        market_code=market_code_for_platform(PAYPAL_PLATFORM, currency_code),
        currency_code=currency_code,
        order_id=base_transaction_id,
        order_name=base_transaction_id,
        external_transaction_id=f"paypal-dispute:{dispute_id}",
        external_payout_id="",
        transaction_date=isoformat_utc(create_time),
        payout_date="",
        transaction_type="chargeback",
        status=status,
        gross_amount=API_DECIMAL_ZERO,
        fee_amount=API_DECIMAL_ZERO,
        net_amount=quantize_money(-(chargeback_amount + chargeback_fee_amount)),
        chargeback_amount=chargeback_amount,
        chargeback_fee_amount=chargeback_fee_amount,
        affects_balance=True,
        is_cancelled=False,
        is_chargeback=True,
        payment_reference=base_transaction_id or dispute_id,
        raw_payload=payload,
    )
