# Acceso JDBC read-only para Velneo

Este acceso expone a Velneo una superficie limpia de solo lectura en PostgreSQL. El usuario JDBC es `velneo_read_access` y su `search_path` queda fijado al schema `velneo`, donde solo existen vistas sobre las tablas autorizadas.

La password real no debe guardarse en el repositorio. Se genera fuera del repo, se aplica en PostgreSQL y se comparte por un canal seguro.

## Conexion JDBC

```text
jdbc:postgresql://<host>:<port>/<database>?currentSchema=velneo
```

Credenciales:

```text
user=velneo_read_access
password=<password compartida por canal seguro>
```

## Vistas expuestas

| Vista para Velneo | Origen | Uso |
| --- | --- | --- |
| `velneo.documents` | `invoices.documents` | Facturas de compra procesadas y validadas |
| `velneo.document_breakdowns` | `invoices.document_breakdowns` | Desglose de importes de facturas |
| `velneo.payroll_documents` | `invoices.payroll_documents` | Nominas y resumen de coste empresa |
| `velneo.artist_royalties_documents` | `invoices.artist_royalties_documents` | Documentos individuales de royalties |
| `velneo.artist_royalties_monthly_summary` | `invoices.artist_royalties_monthly_summary` | Resumen mensual de royalties |
| `velneo.payment_fee_monthly_summary` | `invoices.payment_fee_monthly_summary` | Resumen mensual de payment fees |
| `velneo.payment_order_transactions` | `invoices.payment_order_transactions` | Transacciones normalizadas de payment fees |
| `velneo.frame_stock_monthly` | `supply.frame_stock_monthly` | Resumen mensual de stock |
| `velneo.frame_purchases` | `supply.frame_purchases` | Cabeceras de compras de marcos |
| `velneo.frame_purchase_lines` | `supply.frame_purchase_lines` | Lineas de compras de marcos |
| `velneo.purchase_invoices_pyg` | `velneo.documents` + `velneo.document_breakdowns` | Facturas de compra que realmente entran en PYG, listas para export mensual |

No se concede acceso directo a los schemas `invoices` ni `supply`. Tampoco se exponen tablas raw o de operativa interna como `invoices.paypal_transactions_raw`, `invoices.shopify_payout_transactions`, `invoices.ingestion_queue` o `invoices.review_items`.

## Aplicacion

El script idempotente esta en:

```text
scripts/create_velneo_read_access.sql
```

Antes de ejecutarlo, sustituir `'<GENERAR_PASSWORD_SEGURO>'` por una password real o aplicar la password despues con `ALTER ROLE`.

```bash
psql "$DATABASE_URL" -f scripts/create_velneo_read_access.sql
```

Si el rol ya existia, el bloque `CREATE ROLE` no cambia la password. En ese caso, rotarla explicitamente:

```sql
ALTER ROLE velneo_read_access PASSWORD '<NUEVO_PASSWORD_SEGURO>';
```

## SQL principal

```sql
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

REVOKE ALL ON ALL TABLES IN SCHEMA velneo FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA velneo FROM velneo_read_access;

GRANT USAGE ON SCHEMA velneo TO velneo_read_access;
GRANT SELECT ON ALL TABLES IN SCHEMA velneo TO velneo_read_access;
```

## Verificacion

Conectar como `velneo_read_access` usando `currentSchema=velneo` y comprobar lecturas permitidas:

```sql
SELECT count(*) FROM documents;
SELECT count(*) FROM payroll_documents;
SELECT count(*) FROM artist_royalties_monthly_summary;
SELECT count(*) FROM payment_fee_monthly_summary;
SELECT count(*) FROM frame_stock_monthly;
```

Comprobar que el acceso directo a tablas base falla:

```sql
SELECT count(*) FROM invoices.documents;
SELECT count(*) FROM supply.frame_stock_monthly;
```

Comprobar que no hay permisos de escritura:

```sql
INSERT INTO documents DEFAULT VALUES;
UPDATE documents SET status = status;
DELETE FROM documents;
```

Revisar grants desde una cuenta administradora:

```sql
SELECT table_schema, table_name, privilege_type
FROM information_schema.role_table_grants
WHERE grantee = 'velneo_read_access'
ORDER BY table_schema, table_name, privilege_type;
```

El resultado esperado debe contener solo `SELECT` sobre objetos del schema `velneo`.

## Baja del acceso

```sql
REVOKE SELECT ON ALL TABLES IN SCHEMA velneo FROM velneo_read_access;
REVOKE USAGE ON SCHEMA velneo FROM velneo_read_access;
DROP ROLE velneo_read_access;
```
