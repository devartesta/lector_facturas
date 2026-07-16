-- Create the read-only JDBC surface for Velneo.
--
-- Usage:
--   1. Replace <GENERAR_PASSWORD_SEGURO> before running, or run the ALTER ROLE
--      statement separately with the real password.
--   2. Apply with:
--        psql "$DATABASE_URL" -f scripts/create_velneo_read_access.sql
--
-- The real password must not be committed to the repository.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_roles
        WHERE rolname = 'velneo_read_access'
    ) THEN
        CREATE ROLE velneo_read_access
            LOGIN
            PASSWORD '<GENERAR_PASSWORD_SEGURO>'
            NOSUPERUSER
            NOCREATEDB
            NOCREATEROLE;
    END IF;
END
$$;

ALTER ROLE velneo_read_access
    LOGIN
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE;

ALTER ROLE velneo_read_access SET search_path = velneo;

CREATE SCHEMA IF NOT EXISTS velneo;

REVOKE ALL ON SCHEMA velneo FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA velneo FROM PUBLIC;

-- Keep the login isolated from the source schemas. The views below are owned by
-- the applying database role and expose only the curated read surface.
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA invoices FROM velneo_read_access;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA supply FROM velneo_read_access;
REVOKE USAGE ON SCHEMA invoices FROM velneo_read_access;
REVOKE USAGE ON SCHEMA supply FROM velneo_read_access;

CREATE OR REPLACE VIEW velneo.documents AS
SELECT
    id,
    invoice_number,
    invoice_date,
    issuer_company_name,
    billed_company_name,
    supplier_name,
    company_code,
    windows_path,
    drive_url,
    vat_percent,
    gross_amount,
    vat_amount,
    net_amount,
    supplier_id,
    supplier_code,
    currency_code,
    drive_file_id,
    storage_root,
    document_type,
    status,
    source_channel,
    email_message_id,
    email_thread_id,
    attachment_original_name,
    parser_name,
    parser_confidence,
    extracted_raw,
    review_notes,
    created_at,
    updated_at,
    source_sender,
    source_subject,
    period_yyyymm,
    received_at,
    sender_email,
    original_filename,
    billing_period_start,
    billing_period_end,
    division_invoice,
    payment_status,
    payment_date,
    payment_method,
    payment_amount,
    payment_due_date,
    issuer_tax_id,
    billed_tax_id
FROM invoices.documents;

CREATE OR REPLACE VIEW velneo.document_breakdowns AS
SELECT *
FROM invoices.document_breakdowns;

CREATE OR REPLACE VIEW velneo.payroll_documents AS
SELECT *
FROM invoices.payroll_documents;

CREATE OR REPLACE VIEW velneo.artist_royalties_documents AS
SELECT *
FROM invoices.artist_royalties_documents;

CREATE OR REPLACE VIEW velneo.artist_royalties_monthly_summary AS
SELECT *
FROM invoices.artist_royalties_monthly_summary;

CREATE OR REPLACE VIEW velneo.payment_fee_monthly_summary AS
SELECT *
FROM invoices.payment_fee_monthly_summary;

CREATE OR REPLACE VIEW velneo.payment_order_transactions AS
SELECT *
FROM invoices.payment_order_transactions;

CREATE OR REPLACE VIEW velneo.frame_stock_monthly AS
SELECT *
FROM supply.frame_stock_monthly;

CREATE OR REPLACE VIEW velneo.frame_purchases AS
SELECT *
FROM supply.frame_purchases;

CREATE OR REPLACE VIEW velneo.frame_purchase_lines AS
SELECT *
FROM supply.frame_purchase_lines;

CREATE OR REPLACE VIEW velneo.purchase_invoices_pyg AS
WITH base AS (
    SELECT
        d.id,
        d.invoice_number,
        d.invoice_date,
        d.issuer_company_name,
        d.billed_company_name,
        d.issuer_tax_id,
        d.billed_tax_id,
        d.supplier_name,
        d.company_code,
        d.windows_path,
        d.drive_url,
        d.vat_percent,
        d.gross_amount,
        d.vat_amount,
        d.net_amount,
        d.supplier_code,
        d.currency_code,
        d.drive_file_id,
        d.document_type,
        d.status,
        d.parser_name,
        d.review_notes,
        d.extracted_raw,
        d.period_yyyymm AS source_period_yyyymm,
        d.original_filename,
        d.billing_period_end,
        d.division_invoice,
        d.payment_status,
        d.payment_date,
        d.payment_method,
        d.payment_due_date,
        CASE d.company_code
            WHEN 'SL' THEN 'EU_SL'
            WHEN 'LTD' THEN 'UK_LTD'
            WHEN 'INC' THEN 'USA_INC'
            ELSE d.company_code
        END AS market_code,
        CASE
            WHEN d.company_code = 'SL'
                 AND d.supplier_code = 'RAILWAY'
                 AND d.invoice_date IS NOT NULL
                THEN to_char(d.invoice_date, 'YYYYMM')
            WHEN d.company_code = 'SL'
                 AND d.supplier_code = 'HANNUN'
                 AND lower(coalesce(d.division_invoice, '')) IN ('administration', 'office', 'services')
                 AND d.billing_period_end IS NOT NULL
                THEN to_char(d.billing_period_end, 'YYYYMM')
            ELSE d.period_yyyymm
        END AS period_yyyymm,
        replace(coalesce(d.windows_path, ''), E'\\', '/') AS normalized_path,
        upper(coalesce(d.supplier_code, '')) AS supplier_code_upper,
        lower(coalesce(d.parser_name, '')) AS parser_name_lower,
        lower(coalesce(d.division_invoice, '')) AS division_invoice_lower,
        btrim(coalesce(d.invoice_number, '')) AS invoice_number_trimmed
    FROM invoices.documents d
    WHERE d.status = 'classified'
      AND d.document_type IN ('invoice', 'credit_note')
),
periodified_roots AS (
    SELECT DISTINCT
        company_code,
        split_part(invoice_number_trimmed, '_PERIODIFICADA_', 1) AS invoice_root
    FROM base
    WHERE invoice_number_trimmed LIKE '%_PERIODIFICADA_%'
),
artlink_stock_refs AS (
    SELECT
        left(d.period_yyyymm, 4) AS ref_year,
        btrim(coalesce(d.invoice_number, '')) AS invoice_number_trimmed,
        d.net_amount
    FROM invoices.documents d
    WHERE d.company_code = 'LTD'
      AND d.supplier_code = 'ARTLINK'
      AND d.status = 'classified'
      AND (
        coalesce(d.review_notes, '') ILIKE '%stock purchase%'
        OR coalesce(d.review_notes, '') ILIKE '%stock cost%'
        OR lower(coalesce(d.extracted_raw->>'manual_override_stock_purchase', '')) IN ('true', '1')
      )
),
path_parts AS (
    SELECT
        b.*,
        split_part(split_part(b.normalized_path, '/' || b.source_period_yyyymm || '/', 2), '/', 1) AS path_root,
        split_part(split_part(b.normalized_path, '/' || b.source_period_yyyymm || '/', 2), '/', 2) AS path_bucket_raw,
        split_part(split_part(b.normalized_path, '/' || b.source_period_yyyymm || '/', 2), '/', 3) AS path_subcategory_raw,
        split_part(split_part(b.normalized_path, '/' || b.source_period_yyyymm || '/', 2), '/', 4) AS path_detail_raw
    FROM base b
),
classified AS (
    SELECT
        p.*,
        replace(coalesce(p.path_bucket_raw, ''), '-', '_') AS path_bucket,
        replace(coalesce(p.path_subcategory_raw, ''), '-', '_') AS path_subcategory,
        replace(coalesce(p.path_detail_raw, ''), '-', '_') AS path_detail,
        CASE
            WHEN p.company_code = 'LTD' THEN
                CASE
                    WHEN p.supplier_code_upper = 'ARTLINK' THEN ''
                    WHEN p.supplier_code_upper = 'JONDO' THEN 'manufacturing'
                    WHEN p.supplier_code_upper = 'PORTCLEARANCE' THEN 'logistics'
                    WHEN p.supplier_code_upper = 'PROCO' AND p.division_invoice_lower IN ('manufacturing', 'logistics') THEN p.division_invoice_lower
                    WHEN p.supplier_code_upper = 'PROCO' THEN 'manufacturing'
                    WHEN p.supplier_code_upper = 'SHAREDSERVICESSL' THEN 'shared_services'
                    WHEN p.supplier_code_upper = 'YOURACCOUNTSTAXES' THEN 'administration'
                    WHEN p.supplier_code_upper = 'REVER' THEN 'technology'
                    WHEN replace(coalesce(p.path_subcategory_raw, ''), '-', '_') = 'manufacturing_logistics' THEN 'logistics'
                    ELSE replace(coalesce(p.path_subcategory_raw, ''), '-', '_')
                END
            WHEN p.company_code = 'INC' THEN
                CASE
                    WHEN p.supplier_code_upper = 'ARTLINK' THEN ''
                    WHEN p.supplier_code_upper = 'TGI' AND p.division_invoice_lower IN ('manufacturing', 'logistics') THEN p.division_invoice_lower
                    WHEN p.supplier_code_upper = 'TGI' THEN 'manufacturing'
                    WHEN p.supplier_code_upper = 'CONTINUUM' THEN 'administration'
                    WHEN p.supplier_code_upper = 'REGUS' THEN 'administration'
                    WHEN p.supplier_code_upper = 'JONDO' THEN 'manufacturing'
                    WHEN p.supplier_code_upper = 'PORTCLEARANCE' THEN 'logistics'
                    WHEN p.supplier_code_upper = 'PROCO' AND p.division_invoice_lower IN ('manufacturing', 'logistics') THEN p.division_invoice_lower
                    WHEN p.supplier_code_upper = 'PROCO' THEN 'manufacturing'
                    WHEN p.supplier_code_upper = 'SHAREDSERVICESSL' THEN 'shared_services'
                    WHEN p.supplier_code_upper = 'YOURACCOUNTSTAXES' THEN 'administration'
                    WHEN p.supplier_code_upper = 'REVER' THEN 'technology'
                    WHEN replace(coalesce(p.path_subcategory_raw, ''), '-', '_') = 'manufacturing_logistics' THEN 'logistics'
                    ELSE replace(coalesce(p.path_subcategory_raw, ''), '-', '_')
                END
            ELSE replace(coalesce(p.path_subcategory_raw, ''), '-', '_')
        END AS pyg_subcategory
    FROM path_parts p
),
breakdown_summary AS (
    SELECT
        db.document_id,
        count(*) AS breakdown_count,
        string_agg(db.breakdown_code, ' | ' ORDER BY db.breakdown_code) AS breakdown_codes
    FROM invoices.document_breakdowns db
    GROUP BY db.document_id
),
filtered AS (
    SELECT
        c.*,
        CASE
            WHEN c.company_code IN ('LTD', 'INC') THEN
                CASE
                    WHEN c.pyg_subcategory IN ('shared_services', 'administration', 'technology', 'staff', 'otros_gastos') THEN 'opex'
                    ELSE 'cogs'
                END
            ELSE c.path_bucket
        END AS pyg_category
    FROM classified c
    WHERE c.path_root = 'expenses'
      AND c.pyg_subcategory <> ''
      AND NOT (
        c.supplier_code_upper = 'YOURACCOUNTSTAXES'
        AND c.parser_name_lower <> 'manual_periodificada'
      )
      AND NOT (
        c.parser_name_lower <> 'manual_periodificada'
        AND EXISTS (
            SELECT 1
            FROM periodified_roots pr
            WHERE pr.company_code = c.company_code
              AND pr.invoice_root = c.invoice_number_trimmed
        )
      )
      AND NOT (
        c.company_code = 'SL'
        AND c.supplier_code_upper = 'ARTLINK'
        AND EXISTS (
            SELECT 1
            FROM artlink_stock_refs ar
            WHERE ar.ref_year = left(c.period_yyyymm, 4)
              AND ar.invoice_number_trimmed = c.invoice_number_trimmed
              AND ar.net_amount = c.net_amount
        )
      )
)
SELECT
    f.id,
    f.market_code,
    f.company_code,
    f.period_yyyymm,
    f.source_period_yyyymm,
    f.invoice_date,
    f.invoice_number,
    f.document_type,
    CASE WHEN f.document_type = 'credit_note' THEN -1 ELSE 1 END AS sign_multiplier,
    f.supplier_code,
    f.supplier_name,
    f.issuer_company_name,
    f.issuer_tax_id,
    f.billed_company_name,
    f.billed_tax_id,
    f.pyg_category,
    f.pyg_subcategory,
    f.division_invoice,
    CASE
        WHEN f.division_invoice_lower <> '' THEN f.division_invoice_lower
        ELSE f.pyg_subcategory
    END AS pyg_detail,
    f.path_bucket,
    f.path_subcategory,
    f.path_detail,
    f.currency_code,
    f.net_amount,
    f.vat_percent,
    f.vat_amount,
    f.gross_amount,
    (CASE WHEN f.document_type = 'credit_note' THEN -1 ELSE 1 END) * f.net_amount AS signed_net_amount,
    (CASE WHEN f.document_type = 'credit_note' THEN -1 ELSE 1 END) * f.vat_amount AS signed_vat_amount,
    (CASE WHEN f.document_type = 'credit_note' THEN -1 ELSE 1 END) * f.gross_amount AS signed_gross_amount,
    (CASE WHEN f.document_type = 'credit_note' THEN -1 ELSE 1 END) *
    CASE
        WHEN f.company_code = 'INC' AND f.supplier_code_upper = 'JONDO' THEN f.gross_amount
        ELSE f.net_amount
    END AS pyg_amount,
    coalesce(bs.breakdown_count, 0) AS breakdown_count,
    coalesce(bs.breakdown_codes, '') AS breakdown_codes,
    f.payment_status,
    f.payment_due_date,
    f.payment_date,
    f.payment_method,
    f.drive_url,
    f.drive_file_id,
    f.windows_path,
    f.original_filename,
    f.parser_name
FROM filtered f
LEFT JOIN breakdown_summary bs ON bs.document_id = f.id;

REVOKE ALL ON ALL TABLES IN SCHEMA velneo FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA velneo FROM velneo_read_access;

GRANT USAGE ON SCHEMA velneo TO velneo_read_access;
GRANT SELECT ON ALL TABLES IN SCHEMA velneo TO velneo_read_access;

-- If the role already existed, rotate/set the real password after applying:
--   ALTER ROLE velneo_read_access PASSWORD '<NUEVO_PASSWORD_SEGURO>';
