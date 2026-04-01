"""
Sync finance.informe_vat_gestorias_detalle for 202603
from the partition finance.informe_vat_gestorias_detalle_202603.

Equivalent to running 007_refresh_informe_vat_gestorias_detalle.sql
for yyyymm = 202603.
"""
from __future__ import annotations

import sys
import os
import psycopg2
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from lector_facturas.settings import load_settings

YYYYMM = "202603"

settings = load_settings()
conn = psycopg2.connect(os.environ["DATABASE_URL"])
conn.autocommit = False

try:
    with conn.cursor() as cur:
        # Check current state
        cur.execute(
            "SELECT COUNT(*), SUM(shown_net_presentment) FROM finance.informe_vat_gestorias_detalle WHERE order_month_yyyymm = %s",
            (YYYYMM,),
        )
        before = cur.fetchone()
        print(f"Before: {before[0]} rows, net={before[1]}", flush=True)

        # Delete existing rows for the month
        cur.execute(
            "DELETE FROM finance.informe_vat_gestorias_detalle WHERE order_month_yyyymm = %s",
            (YYYYMM,),
        )
        print(f"Deleted {cur.rowcount} rows", flush=True)

        # Re-insert from partition, joining shopify.ventas_202603 to get order_date
        cur.execute(
            f"""
            INSERT INTO finance.informe_vat_gestorias_detalle (
                order_month_yyyymm,
                order_date,
                order_name,
                shipping_country_code,
                shipping_state_code,
                payment_gateway_names,
                is_rever_tag,
                is_hannun_tag,
                is_mirakl_tag,
                standard_rate,
                payment_currency,
                tax_rate,
                shown_tax_presentment,
                shown_gross_presentment,
                shown_net_presentment,
                tags,
                descuadre
            )
            SELECT
                d.order_month_yyyymm,
                v.order_date,
                d.order_name,
                d.shipping_country_code,
                d.shipping_state_code,
                d.payment_gateway_names,
                d.is_rever_tag,
                d.is_hannun_tag,
                d.is_mirakl_tag,
                d.standard_rate,
                d.payment_currency,
                d.tax_rate,
                d.shown_tax_presentment,
                d.shown_gross_presentment,
                d.shown_net_presentment,
                d.tags,
                d.descuadre
            FROM finance.informe_vat_gestorias_detalle_{YYYYMM} d
            LEFT JOIN shopify.ventas_{YYYYMM} v
                ON d.order_name = v.order_name
                AND d.payment_currency = v.payment_currency
            """
        )
        inserted = cur.rowcount
        print(f"Inserted {inserted} rows from partition", flush=True)

        # Verify
        cur.execute(
            "SELECT COUNT(*), SUM(shown_net_presentment) FROM finance.informe_vat_gestorias_detalle WHERE order_month_yyyymm = %s",
            (YYYYMM,),
        )
        after = cur.fetchone()
        print(f"After: {after[0]} rows, net={after[1]}", flush=True)

    conn.commit()
    print("Committed OK", flush=True)

except Exception as exc:
    conn.rollback()
    print(f"ERROR - rolled back: {exc}", flush=True)
    sys.exit(1)
finally:
    conn.close()

# Now regenerate the payment reconciliation + gestoria for SL, LTD, INC
print("\nRegenerating reports...", flush=True)
from lector_facturas.pyg_sync import (
    sync_gestoria_to_drive,
    sync_payment_reconciliation_to_drive,
)

for company in ["SL", "LTD", "INC"]:
    for label, fn in [
        ("gestoria",             lambda c=company: sync_gestoria_to_drive(settings=settings, company_code=c, period_yyyymm=YYYYMM)),
        ("payment_reconciliation", lambda c=company: sync_payment_reconciliation_to_drive(settings=settings, company_code=c, period_yyyymm=YYYYMM)),
    ]:
        print(f"\n>>> {label} {company}/{YYYYMM} ...", flush=True)
        try:
            result = fn()
            print(f"<<< {label} {company} OK -> {result.drive_file_url}", flush=True)
        except Exception as exc:
            print(f"<<< {label} {company} ERROR: {exc}", flush=True)

print("\nDone.", flush=True)
