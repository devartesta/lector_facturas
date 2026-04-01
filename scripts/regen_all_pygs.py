"""Regenerate all PYGs (SL, LTD, INC, Consolidated) for 2026."""
from __future__ import annotations

import sys
import os
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from lector_facturas.settings import load_settings
from lector_facturas.pyg_sync import (
    sync_pyg_sl_to_drive,
    sync_pyg_ltd_to_drive,
    sync_pyg_inc_to_drive,
    sync_pyg_consolidated_to_drive,
)

YEAR = 2026

settings = load_settings()

tasks = [
    ("PYG SL",          lambda: sync_pyg_sl_to_drive(settings=settings, year=YEAR)),
    ("PYG LTD",         lambda: sync_pyg_ltd_to_drive(settings=settings, year=YEAR)),
    ("PYG INC",         lambda: sync_pyg_inc_to_drive(settings=settings, year=YEAR)),
    ("PYG Consolidated",lambda: sync_pyg_consolidated_to_drive(settings=settings, year=YEAR)),
]

for name, fn in tasks:
    print(f"\n[regen] >>> {name} ...", flush=True)
    try:
        result = fn()
        print(f"[regen] <<< {name} OK -> {getattr(result, 'drive_file_url', result)}", flush=True)
    except Exception as exc:
        print(f"[regen] <<< {name} ERROR: {exc}", flush=True)

print("\n[regen] Done.", flush=True)
