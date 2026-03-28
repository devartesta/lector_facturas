-- Migration: add period_yyyymm generated column to shopify_payout_transactions
-- Based on transaction_date AT TIME ZONE 'Europe/Madrid' (same convention as PayPal)
--
-- Run once against the production database:
--   psql $DATABASE_URL -f scripts/migrate_shopify_payout_period_yyyymm.sql

ALTER TABLE invoices.shopify_payout_transactions
    ADD COLUMN IF NOT EXISTS period_yyyymm text
        GENERATED ALWAYS AS (
            to_char(transaction_date AT TIME ZONE 'Europe/Madrid', 'YYYYMM')
        ) STORED;

CREATE INDEX IF NOT EXISTS idx_spt_company_period
    ON invoices.shopify_payout_transactions (company_code, period_yyyymm);
