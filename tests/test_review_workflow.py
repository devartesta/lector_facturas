from __future__ import annotations

from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from lector_facturas.review_workflow import ReviewDecision, apply_review_decision, get_provider


class ReviewWorkflowTests(unittest.TestCase):
    def test_get_provider_finds_shared_service_record(self) -> None:
        record = get_provider("ARTESTA INC", "SHAREDSERVICESSL")
        self.assertEqual(record.provider_name, "ARTESTA STORE, S.L.")

    def test_apply_review_decision_moves_file_to_destination(self) -> None:
        with TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root / "Artesta Inc" / "2026" / "202601" / "validation" / "pending_supplier_review" / "Factura_2026-0009.pdf"
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_text("dummy", encoding="utf-8")

            result = apply_review_decision(
                ReviewDecision(
                    root=root,
                    source_file=source,
                    company="ARTESTA INC",
                    supplier_code="SHAREDSERVICESSL",
                    invoice_date="2026-01-15",
                    invoice_number="2026-0009",
                )
            )

            self.assertFalse(source.exists())
            self.assertTrue(result.destination_file.exists())
            self.assertTrue(
                str(result.destination_file).endswith(
                    "Artesta Inc\\2026\\202601\\expenses\\opex\\shared-services\\SHAREDSERVICESSL_20260115_2026-0009.pdf"
                )
            )


if __name__ == "__main__":
    unittest.main()
