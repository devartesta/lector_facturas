from __future__ import annotations

import unittest

from lector_facturas.pyg_data_cache import clear_pyg_data_cache, get_cached_pyg_bundle


class PygDataCacheTests(unittest.TestCase):
    def tearDown(self) -> None:
        clear_pyg_data_cache()

    def test_reuses_bundle_for_same_company_and_year(self) -> None:
        calls = 0

        def builder(*, year: int, database_url: str) -> dict:
            nonlocal calls
            calls += 1
            return {"year": year, "database_url": database_url}

        first = get_cached_pyg_bundle(company="sl", year=2026, database_url="db", builder=builder)
        second = get_cached_pyg_bundle(company="sl", year=2026, database_url="db", builder=builder)

        self.assertIs(first, second)
        self.assertEqual(calls, 1)

    def test_does_not_mix_companies(self) -> None:
        calls: list[str] = []

        def builder(*, year: int, database_url: str) -> str:
            calls.append(database_url)
            return database_url

        get_cached_pyg_bundle(company="sl", year=2026, database_url="db", builder=builder)
        get_cached_pyg_bundle(company="ltd", year=2026, database_url="db", builder=builder)

        self.assertEqual(calls, ["db", "db"])
