from __future__ import annotations

from lector_facturas.pyg_inc_workbook import _filter_periodified_documents as filter_inc_documents
from lector_facturas.pyg_ltd_workbook import _filter_periodified_documents as filter_ltd_documents
from lector_facturas.pyg_sl_workbook import _filter_periodified_documents as filter_sl_documents


def test_youraccountstaxes_final_invoice_stays_out_of_ltd_pyg() -> None:
    rows = [
        {
            "supplier_code": "YOURACCOUNTSTAXES",
            "invoice_number": "INV-1001",
            "parser_name": "youraccountstaxes",
        },
        {
            "supplier_code": "YOURACCOUNTSTAXES",
            "invoice_number": "INV-1001_PERIODIFICADA_202601",
            "parser_name": "manual_periodificada",
        },
        {
            "supplier_code": "CONTINUUM",
            "invoice_number": "INV-2001",
            "parser_name": "continuum",
        },
    ]

    filtered = filter_ltd_documents(rows)

    assert [row["invoice_number"] for row in filtered] == [
        "INV-1001_PERIODIFICADA_202601",
        "INV-2001",
    ]


def test_youraccountstaxes_final_invoice_stays_out_of_inc_pyg_even_without_periodification() -> None:
    rows = [
        {
            "supplier_code": "YOURACCOUNTSTAXES",
            "invoice_number": "INV-3001",
            "parser_name": "youraccountstaxes",
        },
        {
            "supplier_code": "REGUS",
            "invoice_number": "INV-4001",
            "parser_name": "regus",
        },
    ]

    filtered = filter_inc_documents(rows)

    assert [row["invoice_number"] for row in filtered] == ["INV-4001"]


def test_non_periodified_suppliers_keep_existing_sl_behavior() -> None:
    rows = [
        {
            "supplier_code": "CLARIS",
            "invoice_number": "INV-5001",
            "parser_name": "claris",
        },
        {
            "supplier_code": "CLARIS",
            "invoice_number": "INV-5001_PERIODIFICADA_202601",
            "parser_name": "manual_periodificada",
        },
    ]

    filtered = filter_sl_documents(rows)

    assert [row["invoice_number"] for row in filtered] == ["INV-5001_PERIODIFICADA_202601"]
