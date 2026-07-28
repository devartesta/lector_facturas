"""Short-lived cache for the data bundles used by PYG snapshots and details."""
from __future__ import annotations

from dataclasses import dataclass
from threading import Event, RLock
from time import monotonic
from typing import Callable, TypeVar


T = TypeVar("T")


@dataclass(frozen=True)
class _CacheEntry:
    value: object
    expires_at: float


_CACHE_TTL_SECONDS = 45.0
_LOCK = RLock()
_CACHE: dict[tuple[object, ...], _CacheEntry] = {}
_IN_FLIGHT: dict[tuple[object, ...], Event] = {}


def get_cached_pyg_bundle(
    *,
    company: str,
    year: int,
    database_url: str,
    builder: Callable[..., T],
) -> T:
    """Build one company/year bundle at most once during the cache window."""
    key = (company, year, database_url, id(builder))
    while True:
        with _LOCK:
            entry = _CACHE.get(key)
            if entry and entry.expires_at > monotonic():
                return entry.value  # type: ignore[return-value]
            waiter = _IN_FLIGHT.get(key)
            if waiter is None:
                waiter = Event()
                _IN_FLIGHT[key] = waiter
                owner = True
            else:
                owner = False
        if owner:
            break
        waiter.wait(timeout=120.0)

    try:
        value = builder(year=year, database_url=database_url)
        with _LOCK:
            _CACHE[key] = _CacheEntry(value=value, expires_at=monotonic() + _CACHE_TTL_SECONDS)
        return value
    finally:
        with _LOCK:
            event = _IN_FLIGHT.pop(key, None)
            if event:
                event.set()


def clear_pyg_data_cache() -> None:
    with _LOCK:
        _CACHE.clear()
