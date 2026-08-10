from __future__ import annotations

from decimal import Decimal
from typing import Any

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover
    psycopg = None  # type: ignore[assignment]
    dict_row = None  # type: ignore[assignment]

from lector_facturas.sales_normalization import normalize_sl_sales_detail_row


def _period_table(schema: str, prefix: str, period_yyyymm: str) -> str:
    if len(period_yyyymm) != 6 or not period_yyyymm.isdigit():
        raise ValueError("period_yyyymm must be exactly six digits")
    return f"{schema}.{prefix}_{period_yyyymm}"


def normalize_sl_sales_period_in_database(*, database_url: str, period_yyyymm: str) -> dict[str, Any]:
    """Persist the shared SL normalization before reports are generated."""
    if psycopg is None:
        raise RuntimeError("psycopg is not installed")

    ventas_table = _period_table("shopify", "ventas", period_yyyymm)
    detail_table = _period_table("finance", "informe_vat_gestorias_detalle", period_yyyymm)
    summary_table = _period_table("finance", "informe_vat_gestorias_resumen", period_yyyymm)

    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        frozen = conn.execute(
            """
            SELECT 1 FROM finance.sales_period_freezes
            WHERE company_code = 'SL' AND period_yyyymm = %s
            """,
            (period_yyyymm,),
        ).fetchone()
        if frozen:
            return {"period_yyyymm": period_yyyymm, "status": "frozen", "updated_orders": 0}

        rows = conn.execute(
            f"""
            SELECT
                d.*,
                j.raw_json AS _raw_json,
                v.same_month_refund_yyyymm AS _same_month_refund_yyyymm,
                v.same_month_refund_amount_presentment AS _same_month_refund_amount_presentment,
                v.gross_presentment_original AS _gross_presentment_original
            FROM {detail_table} d
            JOIN {ventas_table} v
              ON v.order_name = d.order_name
             AND v.payment_currency = d.payment_currency
            LEFT JOIN shopify.json_orders j
              ON j.raw_json ->> 'name' = d.order_name
            WHERE COALESCE(d.shipping_country_code, 'XX') NOT IN ('GB', 'US')
              AND COALESCE(d.is_hannun_tag, 0) = 0
            ORDER BY d.order_name
            """
        ).fetchall()

        changed: list[dict[str, Any]] = []
        for source in rows:
            normalized = normalize_sl_sales_detail_row(source)
            source_values = (
                Decimal(str(source["shown_gross_presentment"])),
                Decimal(str(source["shown_tax_presentment"])),
                Decimal(str(source["shown_net_presentment"])),
                Decimal(str(source["tax_rate"] or 0)),
            )
            normalized_values = (
                Decimal(str(normalized["shown_gross_presentment"])),
                Decimal(str(normalized["shown_tax_presentment"])),
                Decimal(str(normalized["shown_net_presentment"])),
                Decimal(str(normalized["tax_rate"] or 0)),
            )
            if source_values != normalized_values:
                changed.append(normalized)

        for row in changed:
            values = (
                row["shown_gross_presentment"],
                row["shown_tax_presentment"],
                row["shown_net_presentment"],
                row["tax_rate"],
                row["descuadre"],
                row["order_name"],
            )
            conn.execute(
                f"""
                UPDATE {ventas_table}
                SET shown_gross_presentment = %s,
                    shown_tax_presentment = %s,
                    shown_net_presentment = %s,
                    tax_rate = %s
                WHERE order_name = %s
                """,
                values[:4] + values[5:],
            )
            conn.execute(
                f"""
                UPDATE {detail_table}
                SET shown_gross_presentment = %s,
                    shown_tax_presentment = %s,
                    shown_net_presentment = %s,
                    tax_rate = %s,
                    descuadre = %s
                WHERE order_name = %s
                """,
                values,
            )
            conn.execute(
                """
                UPDATE finance.informe_vat_gestorias_detalle
                SET shown_gross_presentment = %s,
                    shown_tax_presentment = %s,
                    shown_net_presentment = %s,
                    tax_rate = %s,
                    descuadre = %s
                WHERE order_month_yyyymm = %s AND order_name = %s
                """,
                values[:5] + (period_yyyymm, row["order_name"]),
            )

        # Rebuild only the SL/EUR slice of the monthly summary.
        conn.execute(
            f"""
            DELETE FROM {summary_table}
            WHERE payment_currency = 'EUR'
              AND COALESCE(country, 'XX') NOT IN ('GB', 'US')
            """
        )
        conn.execute(
            f"""
            INSERT INTO {summary_table} (
                order_month_yyyymm, country, shipping_state_code, is_hannun_tag,
                payment_method, tax_rate_teorical, tax_rate_shopify,
                tax_rate_calculated, payment_currency, num_orders,
                imp_sales_tax, imp_sales_gross, imp_sales_net
            )
            SELECT
                order_month_yyyymm,
                COALESCE(shipping_country_code, 'XX'),
                shipping_state_code,
                is_hannun_tag,
                payment_gateway_names,
                standard_rate,
                tax_rate,
                CASE WHEN SUM(shown_net_presentment) <> 0
                     THEN SUM(shown_tax_presentment) / SUM(shown_net_presentment)
                     ELSE 0 END,
                payment_currency,
                COUNT(*),
                SUM(shown_tax_presentment),
                SUM(shown_gross_presentment),
                SUM(shown_net_presentment)
            FROM {detail_table}
            WHERE payment_currency = 'EUR'
              AND COALESCE(shipping_country_code, 'XX') NOT IN ('GB', 'US')
            GROUP BY order_month_yyyymm, COALESCE(shipping_country_code, 'XX'),
                     shipping_state_code, is_hannun_tag, payment_gateway_names,
                     standard_rate, tax_rate, payment_currency
            """
        )

        # ventas_pyg is a direct aggregation of the normalized monthly source.
        conn.execute("DELETE FROM finance.ventas_pyg WHERE order_month_yyyymm = %s", (period_yyyymm,))
        conn.execute(
            f"""
            INSERT INTO finance.ventas_pyg (
                order_month_yyyymm, shipping_country_code, payment_currency,
                is_rever_tag, is_hannun_tag, is_mirakl_tag,
                tax, gross, net, shipping_country_code_raw,
                source_payment_currency, is_choose_tag, is_toasty_tag
            )
            SELECT
                order_month_yyyymm,
                COALESCE(shipping_country_code, 'XX'),
                payment_currency,
                is_rever_tag,
                is_hannun_tag,
                is_mirakl_tag,
                SUM(shown_tax_presentment),
                SUM(shown_gross_presentment),
                SUM(shown_net_presentment),
                NULL,
                NULL,
                is_choose_tag,
                is_toasty_tag
            FROM {ventas_table}
            GROUP BY order_month_yyyymm, COALESCE(shipping_country_code, 'XX'),
                     payment_currency, is_rever_tag, is_hannun_tag, is_mirakl_tag,
                     is_choose_tag, is_toasty_tag
            """
        )

        totals = conn.execute(
            f"""
            SELECT COUNT(*) AS orders,
                   SUM(d.shown_gross_presentment) AS gross,
                   SUM(d.shown_tax_presentment) AS tax,
                   SUM(d.shown_net_presentment) AS net
            FROM {detail_table} d
            LEFT JOIN {ventas_table} v
              ON v.order_name = d.order_name AND v.payment_currency = d.payment_currency
            WHERE COALESCE(d.shipping_country_code, 'XX') NOT IN ('GB', 'US')
              AND COALESCE(d.is_hannun_tag, 0) = 0
              AND COALESCE(v.is_choose_tag, 0) = 0
              AND COALESCE(v.is_toasty_tag, 0) = 0
              AND (
                    COALESCE(d.is_rever_tag, 0) = 0
                    OR COALESCE(d.payment_gateway_names, '[]'::jsonb)
                       @> '["shopify_payments"]'::jsonb
                  )
            """
        ).fetchone()
        conn.commit()

    return {
        "period_yyyymm": period_yyyymm,
        "status": "updated",
        "updated_orders": len(changed),
        "orders": int(totals["orders"] or 0),
        "gross": str(totals["gross"] or 0),
        "tax": str(totals["tax"] or 0),
        "net": str(totals["net"] or 0),
    }
