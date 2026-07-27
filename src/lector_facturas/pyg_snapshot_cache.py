"""Small in-process cache preventing duplicate PYG calculations.

The web UI can request the same snapshot more than once while the page and
its detail widgets mount. A single-flight cache lets one request calculate
the snapshot while the others wait for that result instead of opening many
Postgres connections and repeating the expensive year-wide aggregation.
"""
from __future__ import annotations

from dataclasses import dataclass
from threading import Event, RLock
from time import monotonic
from typing import Callable

from lector_facturas.pyg_snapshot import PygCompany, PygSnapshot
from lector_facturas.settings import AppSettings


@dataclass(frozen=True)
class _CacheEntry:
    snapshot: PygSnapshot
    expires_at: float


_CACHE_TTL_SECONDS = 45.0
_LOCK = RLock()
_CACHE: dict[tuple[object, ...], _CacheEntry] = {}
_IN_FLIGHT: dict[tuple[object, ...], Event] = {}


def get_cached_pyg_snapshot(
    *,
    company: PygCompany,
    months: list[str],
    database_url: str,
    settings: AppSettings | None,
    builder: Callable[..., PygSnapshot],
) -> PygSnapshot:
    key = (company, tuple(months), database_url)
    while True:
        with _LOCK:
            entry = _CACHE.get(key)
            if entry and entry.expires_at > monotonic():
                return entry.snapshot
            waiter = _IN_FLIGHT.get(key)
            if waiter is None:
                waiter = Event()
                _IN_FLIGHT[key] = waiter
                owner = True
            else:
                owner = False
        if owner:
            break
        # Another request is already doing the expensive work. It is safer to
        # wait than to launch a duplicate calculation that can starve Railway.
        waiter.wait(timeout=120.0)

    try:
        snapshot = builder(
            company=company,
            months=months,
            database_url=database_url,
            settings=settings,
        )
        with _LOCK:
            _CACHE[key] = _CacheEntry(snapshot=snapshot, expires_at=monotonic() + _CACHE_TTL_SECONDS)
        return snapshot
    finally:
        with _LOCK:
            event = _IN_FLIGHT.pop(key, None)
            if event:
                event.set()


def clear_pyg_snapshot_cache() -> None:
    with _LOCK:
        _CACHE.clear()
