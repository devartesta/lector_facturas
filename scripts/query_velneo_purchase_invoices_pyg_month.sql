-- Example monthly export from the Velneo read-only schema.
-- Replace the value in params.period_yyyymm as needed.

WITH params AS (
    SELECT '202605'::text AS period_yyyymm
)
SELECT
    market_code,
    company_code,
    period_yyyymm,
    invoice_date,
    invoice_number,
    document_type,
    supplier_code,
    supplier_name,
    pyg_category,
    pyg_subcategory,
    pyg_detail,
    currency_code,
    net_amount,
    vat_percent,
    vat_amount,
    gross_amount,
    pyg_amount,
    payment_status,
    payment_due_date,
    payment_date,
    payment_method,
    drive_url,
    windows_path
FROM velneo.purchase_invoices_pyg
WHERE period_yyyymm = (SELECT period_yyyymm FROM params)
ORDER BY market_code, invoice_date, supplier_code, invoice_number, document_type;
