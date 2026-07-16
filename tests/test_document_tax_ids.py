from lector_facturas.document_tax_ids import extract_document_tax_ids, normalize_tax_id


def test_extracts_tax_ids_near_issuer_and_billed_company() -> None:
    text = """
    Supplier Co SL
    CIF: ESB12345678

    Bill to:
    ARTESTA STORE, S.L.
    NIF ESB87654321
    """

    result = extract_document_tax_ids(
        text,
        issuer_company_name="Supplier Co SL",
        billed_company_name="ARTESTA STORE, S.L.",
    )

    assert result.issuer_tax_id == "B12345678"
    assert result.billed_tax_id == "B87654321"
    assert result.confidence == "high"


def test_leaves_unassigned_when_tax_ids_are_not_near_company_names() -> None:
    text = "VAT ID: GB123456789\n" + ("lorem ipsum\n" * 80) + "ARTESTA STORES (UK) LTD"

    result = extract_document_tax_ids(
        text,
        issuer_company_name="Supplier",
        billed_company_name="ARTESTA STORES (UK) LTD",
    )

    assert result.issuer_tax_id is None
    assert result.billed_tax_id is None
    assert result.confidence == "low"


def test_normalize_tax_id_handles_common_formats() -> None:
    assert normalize_tax_id("ES B-12345678") == "B12345678"
    assert normalize_tax_id("GB 123 456 789") == "GB123456789"
    assert normalize_tax_id("12-3456789") == "12-3456789"
