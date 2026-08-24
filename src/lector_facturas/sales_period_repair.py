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


def rebuild_frozen_ventas_pyg(conn: Any, *, period_yyyymm: str) -> None:
    """Mirror the immutable SL snapshot into ``finance.ventas_pyg``.

    Frozen detail is already the final Shopify scope: marketplace-tagged
    orders and non-Shopify Rever movements were resolved before closing.
    Rebuilding from that JSON keeps direct SQL queries aligned with every
    report without reopening the monthly source tables.
    """
    conn.execute("DELETE FROM finance.ventas_pyg WHERE order_month_yyyymm = %s", (period_yyyymm,))
    conn.execute(
        """
        INSERT INTO finance.ventas_pyg (
            order_month_yyyymm, shipping_country_code, payment_currency,
            is_rever_tag, is_hannun_tag, is_mirakl_tag,
            tax, gross, net, shipping_country_code_raw,
            source_payment_currency, is_choose_tag, is_toasty_tag
        )
        SELECT
            f.period_yyyymm,
            COALESCE(NULLIF(d.shipping_country_code, ''), 'XX'),
            COALESCE(NULLIF(d.payment_currency, ''), 'EUR'),
            COALESCE(d.is_rever_tag, 0),
            COALESCE(d.is_hannun_tag, 0),
            COALESCE(d.is_mirakl_tag, 0),
            SUM(COALESCE(d.shown_tax_presentment, 0)),
            SUM(COALESCE(d.shown_gross_presentment, 0)),
            SUM(COALESCE(d.shown_net_presentment, 0)),
            NULL,
            COALESCE(
                NULLIF(d._source_payment_currency, ''),
                NULLIF(d.payment_currency, ''),
                'EUR'
            ),
            0,
            0
        FROM finance.sales_period_freezes f
        CROSS JOIN LATERAL jsonb_to_recordset(f.detail_rows) AS d(
            shipping_country_code text,
            payment_currency text,
            _source_payment_currency text,
            is_rever_tag integer,
            is_hannun_tag integer,
            is_mirakl_tag integer,
            shown_tax_presentment numeric,
            shown_gross_presentment numeric,
            shown_net_presentment numeric
        )
        WHERE f.company_code = 'SL'
          AND f.period_yyyymm = %s
        GROUP BY
            f.period_yyyymm,
            COALESCE(NULLIF(d.shipping_country_code, ''), 'XX'),
            COALESCE(NULLIF(d.payment_currency, ''), 'EUR'),
            COALESCE(d.is_rever_tag, 0),
            COALESCE(d.is_hannun_tag, 0),
            COALESCE(d.is_mirakl_tag, 0),
            COALESCE(
                NULLIF(d._source_payment_currency, ''),
                NULLIF(d.payment_currency, ''),
                'EUR'
            )
        """,
        (period_yyyymm,),
    )


def normalize_sl_sales_period_in_database(*, database_url: str, period_yyyymm: str) -> dict[str, Any]:
    """Persist the shared SL normalization before reports are generated."""
    if psycopg is None:
        raise RuntimeError("psycopg is not installed")

    ventas_table = _period_table("shopify", "ventas", period_yyyymm)
    detail_table = _period_table("finance", "informe_vat_gestorias_detalle", period_yyyymm)
    summary_table = _period_table("finance", "informe_vat_gestorias_resumen", period_yyyymm)

    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        # Hourly reports and an on-demand PYG can overlap. Serialize repairs
        # for the same month so both jobs cannot rebuild the same rows at once.
        conn.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (f"normalize-sl-sales:{period_yyyymm}",),
        )
        frozen = conn.execute(
            """
            SELECT totals FROM finance.sales_period_freezes
            WHERE company_code = 'SL' AND period_yyyymm = %s
            """,
            (period_yyyymm,),
        ).fetchone()
        if frozen:
            rebuild_frozen_ventas_pyg(conn, period_yyyymm=period_yyyymm)
            conn.commit()
            totals = frozen.get("totals") or {}
            return {
                "period_yyyymm": period_yyyymm,
                "status": "frozen",
                "updated_orders": 0,
                "orders": int(totals.get("orders") or 0),
                "gross": str(totals.get("gross_eur") or 0),
                "tax": str(totals.get("tax_eur") or 0),
                "net": str(totals.get("net_eur") or 0),
            }

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
                Decimal(str(source["descuadre"] or 0)),
                str(source["payment_currency"] or "").upper(),
            )
            normalized_values = (
                Decimal(str(normalized["shown_gross_presentment"])),
                Decimal(str(normalized["shown_tax_presentment"])),
                Decimal(str(normalized["shown_net_presentment"])),
                Decimal(str(normalized["tax_rate"] or 0)),
                Decimal(str(normalized["descuadre"] or 0)),
                str(normalized["payment_currency"] or "").upper(),
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
                row["payment_currency"],
                row["order_name"],
            )
            conn.execute(
                f"""
                UPDATE {ventas_table}
                SET shown_gross_presentment = %s,
                    shown_tax_presentment = %s,
                    shown_net_presentment = %s,
                    tax_rate = %s,
                    payment_currency = %s,
                    gross_presentment_original = %s,
                    same_month_refund_amount_presentment = %s
                WHERE order_name = %s
                """,
                values[:4]
                + (
                    values[5],
                    row.get("_gross_presentment_original"),
                    row.get("_same_month_refund_amount_presentment"),
                    values[6],
                ),
            )
            conn.execute(
                f"""
                UPDATE {detail_table}
                SET shown_gross_presentment = %s,
                    shown_tax_presentment = %s,
                    shown_net_presentment = %s,
                    tax_rate = %s,
                    descuadre = %s,
                    payment_currency = %s
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
                    descuadre = %s,
                    payment_currency = %s
                WHERE order_month_yyyymm = %s AND order_name = %s
                """,
                values[:6] + (period_yyyymm, row["order_name"]),
            )

        # Rebuild only the SL/EUR slice of the monthly summary.
        conn.execute(
            f"""
            DELETE FROM {summary_table}
            WHERE COALESCE(country, 'XX') NOT IN ('GB', 'US')
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
                d.order_month_yyyymm,
                COALESCE(d.shipping_country_code, 'XX'),
                d.payment_currency,
                d.is_rever_tag,
                d.is_hannun_tag,
                d.is_mirakl_tag,
                SUM(d.shown_tax_presentment),
                SUM(d.shown_gross_presentment),
                SUM(d.shown_net_presentment),
                NULL,
                COALESCE(NULLIF(j.raw_json ->> 'presentment_currency', ''), d.payment_currency),
                v.is_choose_tag,
                v.is_toasty_tag
            FROM {detail_table} d
            JOIN {ventas_table} v
              ON v.order_name = d.order_name
            LEFT JOIN shopify.json_orders j
              ON j.raw_json ->> 'name' = d.order_name
            WHERE COALESCE(d.is_rever_tag, 0) = 0
               OR COALESCE(d.payment_gateway_names, '[]'::jsonb)
                  @> '["shopify_payments"]'::jsonb
            GROUP BY d.order_month_yyyymm, COALESCE(d.shipping_country_code, 'XX'),
                     d.payment_currency, d.is_rever_tag, d.is_hannun_tag, d.is_mirakl_tag,
                     v.is_choose_tag, v.is_toasty_tag,
                     COALESCE(NULLIF(j.raw_json ->> 'presentment_currency', ''), d.payment_currency)
            """
        )

        integrity = conn.execute(
            f"""
            SELECT
                COUNT(*) FILTER (WHERE d.payment_currency <> 'EUR') AS foreign_rows,
                COUNT(*) FILTER (WHERE ABS(COALESCE(d.descuadre, 0)) > 0.01) AS unbalanced_rows
            FROM {detail_table} d
            WHERE COALESCE(d.shipping_country_code, 'XX') NOT IN ('GB', 'US')
            """
        ).fetchone()
        if int(integrity["foreign_rows"] or 0):
            raise RuntimeError(
                f"SL sales normalization left {integrity['foreign_rows']} foreign-currency "
                f"row(s) in {period_yyyymm}"
            )
        if int(integrity["unbalanced_rows"] or 0):
            raise RuntimeError(
                f"SL sales normalization left {integrity['unbalanced_rows']} unbalanced "
                f"row(s) in {period_yyyymm}"
            )

        totals = conn.execute(
            f"""
            SELECT COUNT(*) AS orders,
                   SUM(d.shown_gross_presentment) AS gross,
                   SUM(d.shown_tax_presentment) AS tax,
                   SUM(d.shown_net_presentment) AS net
            FROM {detail_table} d
            LEFT JOIN {ventas_table} v
              ON v.order_name = d.order_name
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
        pyg_totals = conn.execute(
            """
            SELECT
                SUM(gross) AS gross,
                SUM(tax) AS tax,
                SUM(net) AS net
            FROM finance.ventas_pyg
            WHERE order_month_yyyymm = %s
              AND payment_currency = 'EUR'
              AND COALESCE(shipping_country_code, 'XX') NOT IN ('GB', 'US')
              AND COALESCE(is_hannun_tag, 0) = 0
              AND COALESCE(is_choose_tag, 0) = 0
              AND COALESCE(is_toasty_tag, 0) = 0
            """,
            (period_yyyymm,),
        ).fetchone()
        for field in ("gross", "tax", "net"):
            canonical_value = Decimal(str(totals[field] or 0))
            pyg_value = Decimal(str(pyg_totals[field] or 0))
            if abs(canonical_value - pyg_value) > Decimal("0.01"):
                raise RuntimeError(
                    f"SL sales normalization mismatch in {period_yyyymm}: "
                    f"detail {field}={canonical_value}, ventas_pyg {field}={pyg_value}"
                )
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
