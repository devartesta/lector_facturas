"""Frame stock WAC computation and materialization.

Architecture
------------
Raw sources (read-only):
  supply.consumo_marcos_diario        daily consumption from production
  supply.frame_purchases              purchase header (manual entry via API)
  supply.frame_purchase_lines         purchase lines with unit prices

Materialized (written by this module):
  supply.frame_sku_wac                WAC history: one row per purchase event per SKU
  supply.frame_consumption_valued     daily consumption per SKU with WAC valued at that day
  supply.frame_consumption_override   monthly override table (separate from daily data)
  supply.frame_stock_monthly          monthly summary: opening/consumed/closing

Public API
----------
  populate_sku_wac_for_purchase(purchase_id, conn) -> list[str]
      Called after inserting a purchase. Rebuilds WAC history for affected SKUs
      and returns sorted list of yyyymm months to refresh.

  refresh_frame_consumption_month(fabricante, mes_yyyymm, conn) -> None
      Recomputes frame_consumption_valued (daily rows) + frame_stock_monthly for one month.
      Reads overrides from frame_consumption_override (never modifies them).

  set_frame_consumption_override(fabricante, mes_yyyymm, frame_color, frame_size,
                                  quantity_override, notes, conn) -> None
      Writes/updates an override in frame_consumption_override, storing the opening WAC
      at the time of the override for consistent valuation.

  refresh_frame_consumption_months(fabricante, months, database_url) -> None
      Convenience wrapper for multiple months in one DB connection.

  get_frame_stock_by_year(fabricante, year, database_url) -> dict[str, FrameStockSummary]
      Reads from frame_stock_monthly (requires refresh to have been run first).

  get_frame_stock_summary(fabricante, yyyymm, database_url) -> FrameStockSummary
      Single-month convenience wrapper.

WAC rules
---------
  new_wac = (prev_units * prev_wac + qty * price) / (prev_units + qty)
  Consumption never changes WAC, only lowers stock.
  Same-day events: purchases processed BEFORE consumption.
  Mid-month purchase (day 15): days 1-14 use pre-purchase WAC, 15+ use new WAC.
  Each daily row in frame_consumption_valued stores the exact WAC in effect that day.
"""
from __future__ import annotations

import calendar
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover
    psycopg = None  # type: ignore[assignment]
    dict_row = None  # type: ignore[assignment]


FABRICANTE_CURRENCY: dict[str, str] = {
    "Proco": "GBP",
    "TGI": "USD",
}


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FrameStockSummary:
    fabricante: str
    yyyymm: str
    currency: str
    opening_units: int
    opening_value: Decimal
    consumed_units: int
    consumed_value: Decimal
    purchased_units: int
    closing_units: int
    closing_value: Decimal


@dataclass
class _SkuState:
    units: int = 0
    wac: Decimal = field(default_factory=lambda: Decimal("0"))

    def copy(self) -> "_SkuState":
        return _SkuState(units=self.units, wac=self.wac)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_date(d) -> date:
    if isinstance(d, date):
        return d
    if hasattr(d, "date"):
        return d.date()
    return date.fromisoformat(str(d))


def _apply_buy(state: _SkuState, qty: int, price: Decimal) -> None:
    total = state.units + qty
    if total > 0:
        state.wac = (state.units * state.wac + qty * price) / Decimal(total)
    else:
        state.wac = price
    state.units = total


def _apply_consume(state: _SkuState, qty: int) -> None:
    state.units = max(0, state.units - qty)


def _build_events(
    purchases: list,
    consumption: list,
) -> dict[tuple[str, str], list[tuple[date, str, int, Decimal]]]:
    """Build sorted event list per SKU: (date, type, qty, price).
    On same day, 'buy' events sort before 'consume'.
    """
    skus: set[tuple[str, str]] = set()
    for p in purchases:
        skus.add((p["frame_color"], p["frame_size"]))
    for c in consumption:
        skus.add((c["frame_color"], c["frame_size"]))

    events: dict[tuple[str, str], list[tuple[date, str, int, Decimal]]] = {
        sku: [] for sku in skus
    }
    for p in purchases:
        d = _to_date(p["purchase_date"])
        sku = (p["frame_color"], p["frame_size"])
        events[sku].append((d, "buy", int(p["quantity"]), Decimal(str(p["unit_price"]))))
    for c in consumption:
        d = _to_date(c["fecha_ddmmaaaa"])
        sku = (c["frame_color"], c["frame_size"])
        events[sku].append((d, "consume", int(c["quantity"]), Decimal("0")))

    for sku in skus:
        events[sku].sort(key=lambda e: (e[0], 0 if e[1] == "buy" else 1))

    return events


# ---------------------------------------------------------------------------
# WAC history population
# ---------------------------------------------------------------------------

def populate_sku_wac_for_purchase(purchase_id: int, conn) -> list[str]:
    """Rebuild WAC history for all SKUs in this purchase.

    For each SKU, replays ALL purchases + ALL consumption chronologically
    and records a frame_sku_wac entry after each purchase event.

    Handles backdated purchases: existing WAC entries are replaced with
    the newly-computed values.

    Returns sorted list of yyyymm months that need to be refreshed
    (from the earliest purchase date for the affected SKUs onwards).
    """
    purchase_rows = conn.execute(
        """
        SELECT fp.fabricante, fp.purchase_date,
               fpl.frame_color, fpl.frame_size, fpl.quantity, fpl.unit_price
        FROM supply.frame_purchases fp
        JOIN supply.frame_purchase_lines fpl ON fpl.purchase_id = fp.id
        WHERE fp.id = %s
        """,
        (purchase_id,),
    ).fetchall()

    if not purchase_rows:
        return []

    fabricante: str = purchase_rows[0]["fabricante"]
    months_to_refresh: set[str] = set()

    for row in purchase_rows:
        color: str = row["frame_color"]
        size: str = row["frame_size"]

        # All purchases for this SKU
        sku_purchases = conn.execute(
            """
            SELECT fp.id, fp.purchase_date, fpl.quantity, fpl.unit_price
            FROM supply.frame_purchases fp
            JOIN supply.frame_purchase_lines fpl ON fpl.purchase_id = fp.id
            WHERE fp.fabricante = %s AND fpl.frame_color = %s AND fpl.frame_size = %s
            ORDER BY fp.purchase_date, fp.id
            """,
            (fabricante, color, size),
        ).fetchall()

        # All consumption for this SKU
        sku_consumption = conn.execute(
            """
            SELECT fecha_ddmmaaaa, SUM(quantity) AS quantity
            FROM supply.consumo_marcos_diario
            WHERE fabricante = %s AND frame_color = %s AND frame_size = %s
            GROUP BY fecha_ddmmaaaa
            ORDER BY fecha_ddmmaaaa
            """,
            (fabricante, color, size),
        ).fetchall()

        # Build sorted events: (date, type, qty, price, purchase_id)
        events: list[tuple[date, str, int, Decimal, int | None]] = []
        for p in sku_purchases:
            events.append(
                (_to_date(p["purchase_date"]), "buy", int(p["quantity"]),
                 Decimal(str(p["unit_price"])), int(p["id"]))
            )
        for c in sku_consumption:
            events.append(
                (_to_date(c["fecha_ddmmaaaa"]), "consume", int(c["quantity"]),
                 Decimal("0"), None)
            )
        events.sort(key=lambda e: (e[0], 0 if e[1] == "buy" else 1))

        # Replay and record WAC snapshot after each purchase
        state = _SkuState()
        wac_entries: list[tuple[int, date, Decimal, int]] = []  # (purchase_id, eff_date, wac, units)

        for evt_date, evt_type, qty, price, pid in events:
            if evt_type == "buy":
                _apply_buy(state, qty, price)
                wac_entries.append((pid, evt_date, state.wac, state.units))  # type: ignore[arg-type]
            else:
                _apply_consume(state, qty)

        # Replace WAC history for this SKU
        conn.execute(
            "DELETE FROM supply.frame_sku_wac WHERE fabricante=%s AND frame_color=%s AND frame_size=%s",
            (fabricante, color, size),
        )
        for pid, eff_date, wac, units in wac_entries:
            conn.execute(
                """
                INSERT INTO supply.frame_sku_wac
                    (fabricante, frame_color, frame_size, effective_from, purchase_id, wac, units_on_hand)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (fabricante, color, size, eff_date, pid, wac, units),
            )

        # Collect months to refresh: from earliest purchase date onwards
        if sku_purchases:
            earliest = _to_date(sku_purchases[0]["purchase_date"])
            d = earliest.replace(day=1)
            today = date.today()
            while d <= today.replace(day=1):
                months_to_refresh.add(f"{d.year}{d.month:02d}")
                if d.month == 12:
                    d = date(d.year + 1, 1, 1)
                else:
                    d = date(d.year, d.month + 1, 1)

    return sorted(months_to_refresh)


# ---------------------------------------------------------------------------
# Monthly refresh — daily granularity
# ---------------------------------------------------------------------------

def refresh_frame_consumption_month(fabricante: str, mes_yyyymm: str, conn) -> None:
    """Recompute frame_consumption_valued (daily rows) + frame_stock_monthly for one month.

    frame_consumption_valued stores one row per (fabricante, fecha, frame_color, frame_size)
    with the WAC that was in effect on that specific day. This gives full audit trail:
    if a purchase arrived mid-month, consumption before that date uses the old WAC and
    consumption after uses the new blended WAC.

    Overrides are read from supply.frame_consumption_override (separate table, keyed by
    fabricante + mes_yyyymm + frame_color + frame_size). This function NEVER modifies
    overrides; they only affect the consumed_* columns in frame_stock_monthly.
    """
    year = int(mes_yyyymm[:4])
    month = int(mes_yyyymm[4:])
    month_start = date(year, month, 1)
    month_end = date(year, month, calendar.monthrange(year, month)[1])
    currency = FABRICANTE_CURRENCY.get(fabricante, "")

    # All purchases up to end of month (full history needed to compute WAC correctly)
    purchases = conn.execute(
        """
        SELECT fp.purchase_date, fp.id AS purchase_id,
               fpl.frame_color, fpl.frame_size, fpl.quantity, fpl.unit_price
        FROM supply.frame_purchases fp
        JOIN supply.frame_purchase_lines fpl ON fpl.purchase_id = fp.id
        WHERE fp.fabricante = %s AND fp.purchase_date <= %s
        ORDER BY fp.purchase_date, fp.id
        """,
        (fabricante, month_end),
    ).fetchall()

    # All consumption up to end of month (history needed for opening state computation)
    all_consumption = conn.execute(
        """
        SELECT fecha_ddmmaaaa, frame_color, frame_size, SUM(quantity) AS quantity
        FROM supply.consumo_marcos_diario
        WHERE fabricante = %s AND fecha_ddmmaaaa <= %s
        GROUP BY fecha_ddmmaaaa, frame_color, frame_size
        ORDER BY fecha_ddmmaaaa
        """,
        (fabricante, month_end),
    ).fetchall()

    month_purchases = [p for p in purchases
                       if month_start <= _to_date(p["purchase_date"]) <= month_end]
    month_consumption = [c for c in all_consumption
                         if month_start <= _to_date(c["fecha_ddmmaaaa"]) <= month_end]

    # Overrides for this month (read-only; never modified here)
    overrides: dict[tuple[str, str], dict] = {
        (r["frame_color"], r["frame_size"]): dict(r)
        for r in conn.execute(
            """
            SELECT frame_color, frame_size, quantity_override, opening_wac
            FROM supply.frame_consumption_override
            WHERE fabricante = %s AND mes_yyyymm = %s
            """,
            (fabricante, mes_yyyymm),
        ).fetchall()
    }

    # Build sorted event lists per SKU (full history up to month_end)
    events_by_sku = _build_events(purchases, all_consumption)
    all_skus: set[tuple[str, str]] = set(events_by_sku.keys())

    # Compute opening state per SKU: replay all events strictly before month_start
    opening_states: dict[tuple[str, str], _SkuState] = {}
    for sku, evts in events_by_sku.items():
        state = _SkuState()
        for evt_date, evt_type, qty, price in evts:
            if evt_date >= month_start:
                break
            if evt_type == "buy":
                _apply_buy(state, qty, price)
            else:
                _apply_consume(state, qty)
        opening_states[sku] = state.copy()

    # Adjust opening states for prior-month overrides.
    # Raw replay uses consumo_marcos_diario (system) quantities. If a prior month had a
    # manual override, effective units consumed differed from system quantities, so the
    # opening unit count for the current month must be corrected.
    prior_overrides = conn.execute(
        """
        SELECT o.frame_color, o.frame_size,
               COALESCE(SUM(cmd.quantity), 0) AS qty_system,
               o.quantity_override
        FROM supply.frame_consumption_override o
        LEFT JOIN supply.consumo_marcos_diario cmd ON (
            cmd.fabricante = o.fabricante
            AND cmd.mes_yyyymm = o.mes_yyyymm
            AND cmd.frame_color = o.frame_color
            AND cmd.frame_size = o.frame_size
        )
        WHERE o.fabricante = %s AND o.mes_yyyymm < %s
        GROUP BY o.frame_color, o.frame_size, o.quantity_override
        """,
        (fabricante, mes_yyyymm),
    ).fetchall()
    for r in prior_overrides:
        sku = (r["frame_color"], r["frame_size"])
        # diff > 0 → system consumed MORE than effective → add units back to opening
        # diff < 0 → system consumed LESS than effective → remove units from opening
        diff = int(r["qty_system"]) - int(r["quantity_override"])
        if diff != 0:
            st = opening_states.get(sku)
            if st is not None:
                st.units = max(0, st.units + diff)

    # Active SKUs this month: consumption, purchases, or overrides
    active_skus: set[tuple[str, str]] = set()
    for c in month_consumption:
        active_skus.add((c["frame_color"], c["frame_size"]))
    for p in month_purchases:
        active_skus.add((p["frame_color"], p["frame_size"]))
    for sku in overrides:
        active_skus.add(sku)

    # Delete existing daily rows for this month before re-inserting
    conn.execute(
        "DELETE FROM supply.frame_consumption_valued WHERE fabricante = %s AND mes_yyyymm = %s",
        (fabricante, mes_yyyymm),
    )

    # Process each active SKU: write one row per consumption day with WAC at that moment
    # Also accumulate system totals for frame_stock_monthly
    sku_system_totals: dict[tuple[str, str], tuple[int, Decimal]] = {}

    for sku in active_skus:
        open_state = opening_states.get(sku, _SkuState())
        state = open_state.copy()
        qty_sys_total = 0
        amt_sys_total = Decimal("0")

        for evt_date, evt_type, qty, price in events_by_sku.get(sku, []):
            if evt_date > month_end:
                break
            if evt_date < month_start:
                continue
            if evt_type == "buy":
                _apply_buy(state, qty, price)
            else:
                # WAC in effect at this exact moment (after any same-day purchase)
                daily_amount = Decimal(qty) * state.wac
                conn.execute(
                    """
                    INSERT INTO supply.frame_consumption_valued
                        (fabricante, fecha, mes_yyyymm, frame_color, frame_size,
                         quantity, unit_wac, amount, wac_calculated_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (fabricante, fecha, frame_color, frame_size) DO UPDATE SET
                        quantity          = EXCLUDED.quantity,
                        unit_wac          = EXCLUDED.unit_wac,
                        amount            = EXCLUDED.amount,
                        wac_calculated_at = NOW(),
                        updated_at        = NOW()
                    """,
                    (fabricante, evt_date, mes_yyyymm, sku[0], sku[1],
                     qty, state.wac, daily_amount),
                )
                qty_sys_total += qty
                amt_sys_total += daily_amount
                _apply_consume(state, qty)

        sku_system_totals[sku] = (qty_sys_total, amt_sys_total)

    # ---- frame_stock_monthly aggregation ----

    opening_units_total = sum(s.units for s in opening_states.values())
    opening_value_total = sum(s.units * s.wac for s in opening_states.values())

    purchased_units_total = sum(int(p["quantity"]) for p in month_purchases)
    purchased_value_total = sum(
        int(p["quantity"]) * Decimal(str(p["unit_price"])) for p in month_purchases
    )

    # Consumed (effective): use override × opening_wac when present, else sum of daily amounts
    consumed_units_eff = 0
    consumed_value_eff = Decimal("0")
    for sku, (qty_sys, amt_sys) in sku_system_totals.items():
        ov = overrides.get(sku)
        if ov is not None:
            consumed_units_eff += int(ov["quantity_override"])
            consumed_value_eff += (
                Decimal(str(ov["quantity_override"])) * Decimal(str(ov["opening_wac"]))
            )
        else:
            consumed_units_eff += qty_sys
            consumed_value_eff += amt_sys
    # Override-only SKUs (override set but no system consumption this month)
    for sku, ov in overrides.items():
        if sku not in sku_system_totals:
            consumed_units_eff += int(ov["quantity_override"])
            consumed_value_eff += (
                Decimal(str(ov["quantity_override"])) * Decimal(str(ov["opening_wac"]))
            )

    # Closing: replay full month per SKU, then adjust for overrides
    closing_units_total = 0
    closing_value_total = Decimal("0")
    for sku in all_skus:
        open_state = opening_states.get(sku, _SkuState())
        state = open_state.copy()

        for evt_date, evt_type, qty, price in events_by_sku.get(sku, []):
            if evt_date > month_end:
                break
            if evt_date < month_start:
                continue
            if evt_type == "buy":
                _apply_buy(state, qty, price)
            else:
                _apply_consume(state, qty)

        # Adjust closing units: system consumed qty_sys, effective consumed override
        ov = overrides.get(sku)
        sys_qty = sku_system_totals.get(sku, (0, Decimal("0")))[0]
        if ov is not None:
            eff_override = int(ov["quantity_override"])
            state.units = max(0, state.units + sys_qty - eff_override)

        closing_units_total += state.units
        closing_value_total += state.units * state.wac

    conn.execute(
        """
        INSERT INTO supply.frame_stock_monthly
            (fabricante, mes_yyyymm, currency,
             opening_units, opening_value,
             purchased_units, purchased_value,
             consumed_units, consumed_value,
             closing_units, closing_value,
             calculated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (fabricante, mes_yyyymm) DO UPDATE SET
            currency         = EXCLUDED.currency,
            opening_units    = EXCLUDED.opening_units,
            opening_value    = EXCLUDED.opening_value,
            purchased_units  = EXCLUDED.purchased_units,
            purchased_value  = EXCLUDED.purchased_value,
            consumed_units   = EXCLUDED.consumed_units,
            consumed_value   = EXCLUDED.consumed_value,
            closing_units    = EXCLUDED.closing_units,
            closing_value    = EXCLUDED.closing_value,
            calculated_at    = NOW()
        """,
        (fabricante, mes_yyyymm, currency,
         opening_units_total, opening_value_total,
         purchased_units_total, purchased_value_total,
         consumed_units_eff, consumed_value_eff,
         closing_units_total, closing_value_total),
    )


# ---------------------------------------------------------------------------
# Override management
# ---------------------------------------------------------------------------

def set_frame_consumption_override(
    fabricante: str,
    mes_yyyymm: str,
    frame_color: str,
    frame_size: str,
    quantity_override: int,
    notes: str,
    conn,
) -> None:
    """Insert or update a manual consumption override for a SKU+month.

    Looks up the WAC in effect at the start of the month from frame_sku_wac and stores
    it as opening_wac so that valuation is consistent even if WAC history changes later.
    """
    year = int(mes_yyyymm[:4])
    month = int(mes_yyyymm[4:])
    month_start = date(year, month, 1)

    # Look up opening WAC: last frame_sku_wac entry with effective_from < month_start
    wac_row = conn.execute(
        """
        SELECT wac FROM supply.frame_sku_wac
        WHERE fabricante = %s AND frame_color = %s AND frame_size = %s
          AND effective_from < %s
        ORDER BY effective_from DESC, id DESC
        LIMIT 1
        """,
        (fabricante, frame_color, frame_size, month_start),
    ).fetchone()
    opening_wac = Decimal(str(wac_row["wac"])) if wac_row else Decimal("0")

    conn.execute(
        """
        INSERT INTO supply.frame_consumption_override
            (fabricante, mes_yyyymm, frame_color, frame_size,
             quantity_override, opening_wac, notes, set_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (fabricante, mes_yyyymm, frame_color, frame_size) DO UPDATE SET
            quantity_override = EXCLUDED.quantity_override,
            opening_wac       = EXCLUDED.opening_wac,
            notes             = EXCLUDED.notes,
            set_at            = NOW()
        """,
        (fabricante, mes_yyyymm, frame_color, frame_size,
         quantity_override, opening_wac, notes),
    )


def refresh_frame_consumption_months(
    fabricante: str,
    months: list[str],
    database_url: str,
) -> None:
    """Refresh multiple months in one DB connection (ordered by month)."""
    if psycopg is None or not database_url:
        return
    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        for mes_yyyymm in sorted(months):
            refresh_frame_consumption_month(fabricante, mes_yyyymm, conn)
        conn.commit()


# ---------------------------------------------------------------------------
# Read API (from materialized tables)
# ---------------------------------------------------------------------------

def get_frame_stock_by_year(
    *,
    fabricante: str,
    year: int,
    database_url: str,
) -> dict[str, FrameStockSummary]:
    """Read monthly stock summaries from frame_stock_monthly.

    Returns dict keyed by yyyymm for all 12 months; missing months get zeros.
    Requires refresh_frame_consumption_month to have been called for each month.
    """
    if psycopg is None or not database_url:
        return _empty_year(fabricante, year)

    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        rows = conn.execute(
            """
            SELECT fabricante, mes_yyyymm, currency,
                   opening_units, opening_value,
                   purchased_units, purchased_value,
                   consumed_units, consumed_value,
                   closing_units, closing_value
            FROM supply.frame_stock_monthly
            WHERE fabricante = %s AND mes_yyyymm LIKE %s
            ORDER BY mes_yyyymm
            """,
            (fabricante, f"{year}%"),
        ).fetchall()

    by_month = {r["mes_yyyymm"]: r for r in rows}
    result: dict[str, FrameStockSummary] = {}
    currency = FABRICANTE_CURRENCY.get(fabricante, "")

    for m in range(1, 13):
        yyyymm = f"{year}{m:02d}"
        if yyyymm in by_month:
            r = by_month[yyyymm]
            result[yyyymm] = FrameStockSummary(
                fabricante=r["fabricante"],
                yyyymm=yyyymm,
                currency=r["currency"],
                opening_units=int(r["opening_units"]),
                opening_value=Decimal(str(r["opening_value"])),
                consumed_units=int(r["consumed_units"]),
                consumed_value=Decimal(str(r["consumed_value"])),
                purchased_units=int(r["purchased_units"]),
                closing_units=int(r["closing_units"]),
                closing_value=Decimal(str(r["closing_value"])),
            )
        else:
            result[yyyymm] = _zero_summary(fabricante, yyyymm, currency)

    return result


def get_frame_stock_summary(
    *,
    fabricante: str,
    yyyymm: str,
    database_url: str,
) -> FrameStockSummary:
    """Read a single month's stock summary from frame_stock_monthly."""
    year = int(yyyymm[:4])
    summaries = get_frame_stock_by_year(fabricante=fabricante, year=year, database_url=database_url)
    return summaries.get(yyyymm, _zero_summary(fabricante, yyyymm, FABRICANTE_CURRENCY.get(fabricante, "")))


# Backwards-compatible aliases used by PyG workbooks
compute_frame_stock_by_year = get_frame_stock_by_year
compute_frame_stock_summary = get_frame_stock_summary


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _zero_summary(fabricante: str, yyyymm: str, currency: str) -> FrameStockSummary:
    return FrameStockSummary(
        fabricante=fabricante, yyyymm=yyyymm, currency=currency,
        opening_units=0, opening_value=Decimal("0"),
        consumed_units=0, consumed_value=Decimal("0"),
        purchased_units=0, closing_units=0, closing_value=Decimal("0"),
    )


def _empty_year(fabricante: str, year: int) -> dict[str, FrameStockSummary]:
    currency = FABRICANTE_CURRENCY.get(fabricante, "")
    return {
        f"{year}{m:02d}": _zero_summary(fabricante, f"{year}{m:02d}", currency)
        for m in range(1, 13)
    }
