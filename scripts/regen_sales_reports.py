"""Regenerate gestoria (sales) and payment reconciliation reports for 202603."""
from __future__ import annotations

import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from lector_facturas.settings import load_settings
from lector_facturas.pyg_sync import (
    sync_gestoria_to_drive,
    sync_payment_reconciliation_to_drive,
)

PERIOD = "202603"
COMPANIES = ["SL", "LTD", "INC"]

settings = load_settings()

for company in COMPANIES:
    # --- Gestoria (shopify_sales) ---
    print(f"\n[regen] >>> gestoria {company}/{PERIOD} ...", flush=True)
    try:
        result = sync_gestoria_to_drive(
            settings=settings,
            company_code=company,
            period_yyyymm=PERIOD,
        )
        print(f"[regen] <<< gestoria {company}/{PERIOD} OK -> {result.drive_file_url}", flush=True)
    except Exception as exc:
        print(f"[regen] <<< gestoria {company}/{PERIOD} ERROR: {exc}", flush=True)

    # --- Payment reconciliation (cotejo_pagos) ---
    print(f"\n[regen] >>> payment_reconciliation {company}/{PERIOD} ...", flush=True)
    try:
        result = sync_payment_reconciliation_to_drive(
            settings=settings,
            company_code=company,
            period_yyyymm=PERIOD,
        )
        print(f"[regen] <<< payment_reconciliation {company}/{PERIOD} OK -> {result.drive_file_url}", flush=True)
    except Exception as exc:
        print(f"[regen] <<< payment_reconciliation {company}/{PERIOD} ERROR: {exc}", flush=True)

print("\n[regen] Done.", flush=True)
