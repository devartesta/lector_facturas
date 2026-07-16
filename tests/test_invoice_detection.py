from __future__ import annotations

from lector_facturas.invoice_ingestion import detect_parser_rule


def test_receipt_filename_with_railway_text_detects_railway() -> None:
    rule = detect_parser_rule(
        filename="Receipt-2812-9345.pdf",
        sender_email="invoice+statements@acct.stripe.com",
        subject="Receipt-2812-9345.pdf",
        pdf_text="Receipt\nInvoice number 1602C2F5-0019\nRailway Corporation\n",
    )

    assert rule is not None
    assert rule.supplier_code == "RAILWAY"


def test_hushed_receipt_still_detects_from_sender() -> None:
    rule = detect_parser_rule(
        filename="Receipt-2260-8475.pdf",
        sender_email="invoice+statements@hushed.com",
        subject="Your receipt from HUSHED.COM #2260-8475",
        pdf_text="Receipt\nHushed c/o AffinityClick Inc.\n",
    )

    assert rule is not None
    assert rule.supplier_code == "HUSHED"


def test_openai_receipt_still_detects_from_text() -> None:
    rule = detect_parser_rule(
        filename="Receipt-1234-5678.pdf",
        sender_email="invoice+statements@stripe.com",
        subject="Your receipt from OpenAI",
        pdf_text="Receipt\nOpenAI, L.L.C.\nChatGPT\n",
    )

    assert rule is not None
    assert rule.supplier_code == "OPENAI"
