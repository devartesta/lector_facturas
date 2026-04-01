from __future__ import annotations

from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from lector_facturas.provider_catalog import ensure_pending_supplier_review, find_provider_match, load_provider_catalog
from lector_facturas.review_notifications import (
    GmailConfig,
    HistoricalInvoiceNotice,
    NightlyReviewDigest,
    UnmatchedInvoiceNotice,
    build_nightly_review_digest_email,
    build_unmatched_supplier_email,
)


class ProviderCatalogTests(unittest.TestCase):
    def test_catalog_loads_records(self) -> None:
        records = load_provider_catalog()
        self.assertGreater(len(records), 10)

    def test_match_by_company_and_folder(self) -> None:
        matches = find_provider_match("ARTESTA STORE, S.L.", folder_hint="Meta")
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0].supplier_code, "METAADS")

    def test_google_returns_multiple_matches(self) -> None:
        matches = find_provider_match("ARTESTA STORE, S.L.", folder_hint="Google")
        self.assertEqual({match.supplier_code for match in matches}, {"GOOGLEWORKSPACE", "GOOGLEADS"})

    def test_review_folder_is_created(self) -> None:
        with TemporaryDirectory() as tmp:
            location = ensure_pending_supplier_review(Path(tmp), "Artesta Store, S.L", 2026, "202603")
            self.assertTrue(location.path.is_dir())
            self.assertTrue(str(location.path).endswith("validation\\pending_supplier_review"))

    def test_unmatched_supplier_email_contains_context(self) -> None:
        config = GmailConfig(
            client_id="client-id",
            client_secret="client-secret",
            refresh_token="refresh-token",
            sender="bot@example.com",
            recipients=("me@example.com", "ops@example.com"),
        )
        notice = UnmatchedInvoiceNotice(
            company="Artesta Store, S.L",
            period_yyyymm="202603",
            source_sender="billing@example.com",
            source_subject="Invoice attached",
            attachment_names=("invoice.pdf",),
            review_path=Path("C:/review/path"),
            extracted_text="Factura ejemplo",
            suggested_provider="META PLATFORMS IRELAND LIMITED",
        )

        message = build_unmatched_supplier_email(notice, config)

        self.assertEqual(message["To"], "me@example.com, ops@example.com")
        body = message.get_body(preferencelist=("plain",)).get_content()
        html_body = message.get_body(preferencelist=("html",)).get_content()
        self.assertEqual(message["Subject"], "[LF] Revisar proveedor - Artesta Store, S.L - 202603")
        self.assertIn("Factura sin proveedor reconocido", body)
        self.assertIn("Proveedor sugerido: META PLATFORMS IRELAND LIMITED", body)
        self.assertIn("Ubicacion temporal: path", body)
        self.assertIn("Lector Facturas", html_body)
        self.assertIn("Proveedor", html_body)

    def test_nightly_digest_groups_multiple_items(self) -> None:
        config = GmailConfig(
            client_id="client-id",
            client_secret="client-secret",
            refresh_token="refresh-token",
            sender="bot@example.com",
            recipients=("me@example.com",),
        )
        digest = NightlyReviewDigest(
            company="Artesta Store, S.L",
            period_yyyymm="202603",
            unmatched_supplier_items=(
                UnmatchedInvoiceNotice(
                    company="Artesta Store, S.L",
                    period_yyyymm="202603",
                    source_sender="billing@example.com",
                    source_subject="Invoice A",
                    attachment_names=("a.pdf",),
                    review_path=Path("C:/review/path"),
                ),
                UnmatchedInvoiceNotice(
                    company="Artesta Store, S.L",
                    period_yyyymm="202603",
                    source_sender="ops@example.com",
                    source_subject="Invoice B",
                    attachment_names=("b.pdf",),
                    review_path=Path("C:/review/path"),
                ),
            ),
            historical_invoice_items=(
                HistoricalInvoiceNotice(
                    company="Artesta Store, S.L",
                    invoice_year=2025,
                    expected_year_from=2026,
                    source_sender="legacy@example.com",
                    source_subject="Invoice 2025",
                    attachment_names=("legacy.pdf",),
                    review_path=Path("C:/review/path"),
                    invoice_number="2025-1",
                ),
            ),
            missing_expected_items=("META ADS", "SHOPIFY"),
        )

        message = build_nightly_review_digest_email(digest, config)

        self.assertEqual(message["Subject"], "[LF] Resumen nocturno - Artesta Store, S.L - 202603")
        body = message.get_body(preferencelist=("plain",)).get_content()
        html_body = message.get_body(preferencelist=("html",)).get_content()
        self.assertIn("Sin proveedor reconocido (2):", body)
        self.assertIn("Facturas historicas (1):", body)
        self.assertIn("Facturas esperadas pendientes (2):", body)
        self.assertIn("Incidencias pendientes de revision", html_body)


if __name__ == "__main__":
    unittest.main()
