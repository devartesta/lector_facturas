"""Declarative folder templates for each legal entity."""

from __future__ import annotations


TEMPLATES: dict[str, dict[str, object]] = {
    "SL": {
        "income": {
            "sales": {
                "shopify": {},
                "marketplaces": {},
            },
            "shared-services": {},
            "other_income": {
                "supplies": {},
                "rappels": {},
                "other": {},
            },
        },
        "expenses": {
            "cogs": {
                "manufacturing": {},
                "logistics": {},
                "royalties": {},
                "payment_fees": {},
                "other": {},
            },
            "opex": {
                "marketing": {},
                "staff": {},
                "administration": {},
                "technology": {},
                "amortization": {},
            },
            "other": {
                "bank_fees": {},
                "interest": {},
                "exchange_differences": {},
            },
        },
        "statements": {
            "bank": {},
            "payment_platforms": {},
        },
        "reports": {},
        "taxes": {},
        "validation": {},
    },
    "Ltd": {
        "income": {
            "sales": {},
            "other_income": {
                "supplies": {},
                "rappels": {},
                "other": {},
            },
        },
        "expenses": {
            "cogs": {
                "manufacturing-logistics": {},
                "payment_fees": {},
                "stock": {},
                "other": {},
            },
            "opex": {
                "administration": {},
                "shared-services": {},
            },
            "other": {
                "bank_fees": {},
                "interest": {},
                "exchange_differences": {},
            },
        },
        "statements": {
            "bank": {},
            "payment_platforms": {},
        },
        "reports": {},
        "taxes": {},
        "validation": {},
    },
    "Inc": {
        "income": {
            "sales": {},
        },
        "expenses": {
            "cogs": {
                "manufacturing-logistics": {},
                "payment_fees": {},
                "stock": {},
                "other": {},
            },
            "opex": {
                "administration": {},
                "shared-services": {},
            },
            "other": {
                "bank_fees": {},
                "interest": {},
                "exchange_differences": {},
            },
        },
        "statements": {
            "bank": {},
            "payment_platforms": {},
        },
        "reports": {},
        "taxes": {},
        "validation": {},
    },
}

