from __future__ import annotations

from datetime import date
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from lector_facturas.folder_structure import BootstrapConfig, bootstrap_structure, ensure_next_month


class FolderStructureTests(unittest.TestCase):
    def test_bootstrap_creates_expected_leaf(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = BootstrapConfig(
                root=root,
                entities=("SL", "Ltd", "Inc"),
                year=2026,
                start_month=3,
                end_month=3,
            )
            bootstrap_structure(config)

            self.assertTrue(root.joinpath("Artesta Store, S.L", "2026", "202603", "expenses", "opex", "technology").is_dir())
            self.assertTrue(root.joinpath("Artesta Stores (UK) Ltd", "2026", "202603", "expenses", "opex", "shared-services").is_dir())
            self.assertTrue(root.joinpath("Artesta Inc", "2026", "202603", "statements", "payment_platforms").is_dir())

    def test_bootstrap_is_idempotent(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = BootstrapConfig(
                root=root,
                entities=("SL",),
                year=2026,
                start_month=3,
                end_month=3,
            )
            first = bootstrap_structure(config)
            second = bootstrap_structure(config)

            self.assertGreater(len(first), 0)
            self.assertEqual(second, [])

    def test_next_month_rollover(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            created = ensure_next_month(root, entities=("SL",), reference=date(2026, 12, 15))

            self.assertGreater(len(created), 0)
            self.assertTrue(root.joinpath("Artesta Store, S.L", "2027", "202701", "reports").is_dir())

    def test_invalid_template_fails_before_creation(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = BootstrapConfig(
                root=root,
                entities=("SL",),
                year=2026,
                start_month=3,
                end_month=3,
            )

            with self.assertRaises(ValueError):
                bootstrap_structure(config, templates={"SL": {"bad/name": {}}})

            self.assertFalse(root.joinpath("SL").exists())

    def test_entity_aliases_create_real_company_names(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = BootstrapConfig(
                root=root,
                entities=("SL", "Ltd", "Inc"),
                year=2026,
                start_month=3,
                end_month=3,
            )

            bootstrap_structure(config)

            self.assertTrue(root.joinpath("Artesta Store, S.L", "2026", "202603").is_dir())
            self.assertTrue(root.joinpath("Artesta Stores (UK) Ltd", "2026", "202603").is_dir())
            self.assertTrue(root.joinpath("Artesta Inc", "2026", "202603").is_dir())

    def test_file_conflict_uses_new_suffix(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            year_root = root / "Artesta Store, S.L" / "2026"
            year_root.mkdir(parents=True)
            (year_root / "202603").write_text("occupied by file", encoding="utf-8")
            config = BootstrapConfig(
                root=root,
                entities=("SL",),
                year=2026,
                start_month=3,
                end_month=3,
            )

            bootstrap_structure(config)

            self.assertTrue(year_root.joinpath("202603_new", "reports").is_dir())


if __name__ == "__main__":
    unittest.main()
