"""Frame stock and WAC (Weighted Average Cost) computation module.

Proco  → LTD (GBP)
TGI    → INC (USD)

WAC rules:
- On purchase date D: new_wac = (units_before × wac_before + bought × price) / (units_before + bought)
- On consumption: units decrease, WAC unchanged.
- Mid-month purchase on day 15: days 1-14 use pre-purchase WAC, days 15-31 use new WAC.
"""
from __future__ import annotations

import calendar
from dataclasses import dataclass
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


FABRICANTE_CURRENCY = {
    "Proco": "GBP",
    "TGI": "USD",
}


@dataclass(frozen=True)
class FrameStockSummary:
    fabricante: str
    yyyymm: str
    currency: str
    opening_units: int        # stock at start of month
    opening_value: Decimal    # opening_units × WAC at start of month
    consumed_units: int       # total units consumed during the month
    consumed_value: Decimal   # Σ(daily_units × WAC_effective_that_day)
    purchased_units: int      # total units purchased during the month
    closing_units: int        # = opening + purchased - consumed
    closing_value: Decimal    # closing_units × WAC at end of month


# ---------------------------------------------------------------------------
# Internal WAC state machine
# ---------------------------------------------------------------------------

@dataclass
class _SkuState:
    units: int = 0
    wac: Decimal = Decimal("0")


def _build_wac_timeline(
    purchases: list[dict],   # [{purchase_date, frame_color, frame_size, quantity, unit_price}]
    consumption: list[dict], # [{fecha_ddmmaaaa, frame_color, frame_size, quantity}]
    up_to_date: date,
) -> dict[tuple[str, str], dict[date, tuple[int, Decimal]]]:
    """Build per-SKU daily WAC snapshot.

    Returns: {(frame_color, frame_size): {date: (units_at_start_of_day, wac_at_start_of_day)}}
    Only dates with consumption or purchases are populated. Dates with no events inherit
    the previous state.
    """
    # Collect all SKUs
    skus: set[tuple[str, str]] = set()
    for p in purchases:
        skus.add((p["frame_color"], p["frame_size"]))
    for c in consumption:
        skus.add((c["frame_color"], c["frame_size"]))

    # Build sorted event list per SKU
    # Events: (date, type, qty, price)  type: 'buy' | 'consume'
    events_by_sku: dict[tuple[str, str], list[tuple[date, str, int, Decimal]]] = {
        sku: [] for sku in skus
    }
    for p in purchases:
        d = p["purchase_date"] if isinstance(p["purchase_date"], date) else p["purchase_date"].date() if hasattr(p["purchase_date"], "date") else date.fromisoformat(str(p["purchase_date"]))
        if d <= up_to_date:
            events_by_sku[(p["frame_color"], p["frame_size"])].append(
                (d, "buy", int(p["quantity"]), Decimal(str(p["unit_price"])))
            )
    for c in consumption:
        d = c["fecha_ddmmaaaa"] if isinstance(c["fecha_ddmmaaaa"], date) else c["fecha_ddmmaaaa"].date() if hasattr(c["fecha_ddmmaaaa"], "date") else date.fromisoformat(str(c["fecha_ddmmaaaa"]))
        if d <= up_to_date:
            events_by_sku[(c["frame_color"], c["frame_size"])].append(
                (d, "consume", int(c["quantity"]), Decimal("0"))
            )

    # Sort events: on same day, purchases come BEFORE consumption
    for sku in skus:
        events_by_sku[sku].sort(key=lambda e: (e[0], 0 if e[1] == "buy" else 1))

    # Simulate state machine and record (units, wac) at start of each day
    timeline: dict[tuple[str, str], dict[date, tuple[int, Decimal]]] = {}
    for sku in skus:
        state = _SkuState()
        day_snapshots: dict[date, tuple[int, Decimal]] = {}
        prev_date: date | None = None

        for evt_date, evt_type, qty, price in events_by_sku[sku]:
            # Record snapshot at START of this day (before applying this event)
            # Only record once per day (first event of the day captures start-of-day state)
            if evt_date not in day_snapshots:
                day_snapshots[evt_date] = (state.units, state.wac)

            if evt_type == "buy":
                total_units = state.units + qty
                if total_units > 0:
                    state.wac = (state.units * state.wac + qty * price) / Decimal(total_units)
                else:
                    state.wac = price
                state.units = total_units
            else:  # consume
                state.units = max(0, state.units - qty)

            prev_date = evt_date

        timeline[sku] = day_snapshots

    return timeline


def _get_wac_at(
    timeline: dict[tuple[str, str], dict[date, tuple[int, Decimal]]],
    sku: tuple[str, str],
    target_date: date,
) -> tuple[int, Decimal]:
    """Return (units, wac) effective at the START of target_date for the given SKU."""
    snapshots = timeline.get(sku, {})
    if not snapshots:
        return (0, Decimal("0"))
    # Find the most recent snapshot on or before target_date
    candidates = [d for d in snapshots if d <= target_date]
    if not candidates:
        return (0, Decimal("0"))
    return snapshots[max(candidates)]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute_frame_stock_by_year(
    *,
    fabricante: str,
    year: int,
    database_url: str,
) -> dict[str, FrameStockSummary]:
    """Compute FrameStockSummary for each month of the year.

    Returns dict keyed by yyyymm (e.g. "202601"). Months with no data
    still appear with zero values.
    """
    if psycopg is None or not database_url:
        return {
            f"{year}{m:02d}": FrameStockSummary(
                fabricante=fabricante,
                yyyymm=f"{year}{m:02d}",
                currency=FABRICANTE_CURRENCY.get(fabricante, ""),
                opening_units=0, opening_value=Decimal("0"),
                consumed_units=0, consumed_value=Decimal("0"),
                purchased_units=0,
                closing_units=0, closing_value=Decimal("0"),
            )
            for m in range(1, 13)
        }

    year_start = date(year, 1, 1)
    year_end = date(year, 12, 31)

    with psycopg.connect(database_url, row_factory=dict_row) as conn:
        # All purchases for this fabricante up to end of year
        purchases = conn.execute(
            """
            SELECT fp.purchase_date, fpl.frame_color, fpl.frame_size,
                   fpl.quantity, fpl.unit_price, fp.currency
            FROM supply.frame_purchases fp
            JOIN supply.frame_purchase_lines fpl ON fpl.purchase_id = fp.id
            WHERE fp.fabricante = %s AND fp.purchase_date <= %s
            ORDER BY fp.purchase_date, fpl.frame_color, fpl.frame_size
            """,
            (fabricante, year_end),
        ).fetchall()

        # Daily consumption for this fabricante for the full year
        # (plus all prior history to build correct opening stock)
        consumption = conn.execute(
            """
            SELECT fecha_ddmmaaaa, frame_color, frame_size,
                   COALESCE(SUM(quantity), 0) AS quantity
            FROM supply.consumo_marcos_diario
            WHERE fabricante = %s AND fecha_ddmmaaaa <= %s
            GROUP BY fecha_ddmmaaaa, frame_color, frame_size
            ORDER BY fecha_ddmmaaaa
            """,
            (fabricante, year_end),
        ).fetchall()

    currency = FABRICANTE_CURRENCY.get(fabricante, "")
    result: dict[str, FrameStockSummary] = {}

    for month in range(1, 13):
        yyyymm = f"{year}{month:02d}"
        month_start = date(year, month, 1)
        month_end = date(year, month, calendar.monthrange(year, month)[1])

        # Build WAC timeline up to end of month
        timeline = _build_wac_timeline(purchases, consumption, month_end)

        # Opening: state at START of month_start (using timeline up to day before)
        day_before = date(year, month, 1)  # we use snapshots strictly before month_start
        skus: set[tuple[str, str]] = set(timeline.keys())

        # Compute opening state (units & wac at start of month_start)
        # = state after processing all events strictly before month_start
        opening_by_sku: dict[tuple[str, str], tuple[int, Decimal]] = {}
        for sku in skus:
            snapshots = timeline[sku]
            candidates = [d for d in snapshots if d < month_start]
            if candidates:
                opening_by_sku[sku] = snapshots[max(candidates)]
            else:
                opening_by_sku[sku] = (0, Decimal("0"))

        # But if there's a purchase on month_start, opening is BEFORE the purchase.
        # The _build_wac_timeline records start-of-day state, so we need to check:
        # For month_start, start-of-day = before any events that day, which is already
        # captured as the snapshot at month_start (before buy events on that day).
        # We need to re-examine: the snapshot at month_start captures the state
        # BEFORE events on month_start. That's exactly the opening state.
        for sku in skus:
            snapshots = timeline[sku]
            if month_start in snapshots:
                opening_by_sku[sku] = snapshots[month_start]

        opening_units = sum(u for u, _ in opening_by_sku.values())
        opening_value = sum(u * w for u, w in opening_by_sku.values())

        # Consumed during the month
        month_consumption = [
            c for c in consumption
            if month_start <= (c["fecha_ddmmaaaa"] if isinstance(c["fecha_ddmmaaaa"], date) else c["fecha_ddmmaaaa"].date() if hasattr(c["fecha_ddmmaaaa"], "date") else date.fromisoformat(str(c["fecha_ddmmaaaa"]))) <= month_end
        ]

        consumed_units_total = 0
        consumed_value_total = Decimal("0")
        for c in month_consumption:
            c_date = c["fecha_ddmmaaaa"] if isinstance(c["fecha_ddmmaaaa"], date) else c["fecha_ddmmaaaa"].date() if hasattr(c["fecha_ddmmaaaa"], "date") else date.fromisoformat(str(c["fecha_ddmmaaaa"]))
            sku = (c["frame_color"], c["frame_size"])
            qty = int(c["quantity"])
            # WAC effective at start of c_date
            snapshots = timeline.get(sku, {})
            candidates = [d for d in snapshots if d <= c_date]
            _, wac = snapshots[max(candidates)] if candidates else (0, Decimal("0"))
            consumed_units_total += qty
            consumed_value_total += Decimal(qty) * wac

        # Purchased during the month
        month_purchases = [
            p for p in purchases
            if month_start <= (p["purchase_date"] if isinstance(p["purchase_date"], date) else p["purchase_date"].date() if hasattr(p["purchase_date"], "date") else date.fromisoformat(str(p["purchase_date"]))) <= month_end
        ]
        purchased_units_total = sum(int(p["quantity"]) for p in month_purchases)

        # Closing: state after all events up to month_end
        closing_by_sku: dict[tuple[str, str], tuple[int, Decimal]] = {}
        for sku in skus:
            snapshots = timeline[sku]
            candidates = [d for d in snapshots if d <= month_end]
            if candidates:
                # This is the state at START of the last event day.
                # We need state AFTER all events up to month_end.
                # Re-simulate from snapshots isn't enough — we need to apply events.
                pass
            closing_by_sku[sku] = (0, Decimal("0"))

        # For closing value, compute by re-simulating up to month_end
        closing_timeline = _build_wac_timeline(purchases, consumption, month_end)
        # The closing state = opening_units - consumed + purchased, with WAC after last purchase
        # Easier: simulate final state by replaying all events up to month_end
        final_states = _compute_final_states(purchases, consumption, month_end)
        closing_units_total = sum(s.units for s in final_states.values())
        closing_value_total = sum(s.units * s.wac for s in final_states.values())

        result[yyyymm] = FrameStockSummary(
            fabricante=fabricante,
            yyyymm=yyyymm,
            currency=currency,
            opening_units=opening_units,
            opening_value=opening_value,
            consumed_units=consumed_units_total,
            consumed_value=consumed_value_total,
            purchased_units=purchased_units_total,
            closing_units=closing_units_total,
            closing_value=closing_value_total,
        )

    return result


def _compute_final_states(
    purchases: list[dict],
    consumption: list[dict],
    up_to_date: date,
) -> dict[tuple[str, str], _SkuState]:
    """Replay all events up to and including up_to_date, return final state per SKU."""
    skus: set[tuple[str, str]] = set()
    for p in purchases:
        skus.add((p["frame_color"], p["frame_size"]))
    for c in consumption:
        skus.add((c["frame_color"], c["frame_size"]))

    events_by_sku: dict[tuple[str, str], list[tuple[date, str, int, Decimal]]] = {
        sku: [] for sku in skus
    }
    for p in purchases:
        d = p["purchase_date"] if isinstance(p["purchase_date"], date) else p["purchase_date"].date() if hasattr(p["purchase_date"], "date") else date.fromisoformat(str(p["purchase_date"]))
        if d <= up_to_date:
            events_by_sku[(p["frame_color"], p["frame_size"])].append(
                (d, "buy", int(p["quantity"]), Decimal(str(p["unit_price"])))
            )
    for c in consumption:
        d = c["fecha_ddmmaaaa"] if isinstance(c["fecha_ddmmaaaa"], date) else c["fecha_ddmmaaaa"].date() if hasattr(c["fecha_ddmmaaaa"], "date") else date.fromisoformat(str(c["fecha_ddmmaaaa"]))
        if d <= up_to_date:
            events_by_sku[(c["frame_color"], c["frame_size"])].append(
                (d, "consume", int(c["quantity"]), Decimal("0"))
            )

    for sku in skus:
        events_by_sku[sku].sort(key=lambda e: (e[0], 0 if e[1] == "buy" else 1))

    states: dict[tuple[str, str], _SkuState] = {}
    for sku in skus:
        state = _SkuState()
        for _, evt_type, qty, price in events_by_sku[sku]:
            if evt_type == "buy":
                total = state.units + qty
                if total > 0:
                    state.wac = (state.units * state.wac + qty * price) / Decimal(total)
                else:
                    state.wac = price
                state.units = total
            else:
                state.units = max(0, state.units - qty)
        states[sku] = state

    return states


def compute_frame_stock_summary(
    *,
    fabricante: str,
    yyyymm: str,
    database_url: str,
) -> FrameStockSummary:
    """Compute FrameStockSummary for a single month."""
    year = int(yyyymm[:4])
    summaries = compute_frame_stock_by_year(
        fabricante=fabricante, year=year, database_url=database_url
    )
    if yyyymm in summaries:
        return summaries[yyyymm]
    currency = FABRICANTE_CURRENCY.get(fabricante, "")
    return FrameStockSummary(
        fabricante=fabricante, yyyymm=yyyymm, currency=currency,
        opening_units=0, opening_value=Decimal("0"),
        consumed_units=0, consumed_value=Decimal("0"),
        purchased_units=0, closing_units=0, closing_value=Decimal("0"),
    )
