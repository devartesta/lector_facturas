# lector_facturas

Sistema automatizado de gestión financiera para el grupo **Artesta**. Descarga facturas del correo, las clasifica, las mueve a Google Drive y genera los libros P&G mensuales en Excel.

---

## Aliases de sociedades

| Alias | Razón social |
|-------|-------------|
| `SL`  | Artesta Store, S.L. |
| `Ltd` | Artesta Stores (UK) Ltd |
| `Inc` | Artesta Inc |

---

## Arquitectura

```
Gmail  ──►  worker-01 (email-download)
                │
                ▼
        invoices.ingestion_queue  (parse_status='pending')
                │
                ▼
        worker-03 (invoice-processing)
                │  descarga de Drive, parsea, valida, mueve
                ▼
        invoices.ingestion_queue  (parse_status='ok' / 'failed')
        Google Drive /validated/<sociedad>/<año>/<mes>/
                │
                ▼
        worker-05/06/07/08  (pyg sync, diariamente a las 20:00)
                │
                ▼
        Google Drive /  pyg_sl_YYYY.xlsx
                         pyg_ltd_YYYY.xlsx
                         pyg_inc_YYYY.xlsx
                         pyg_consolidado_YYYY.xlsx
```

Todos los workers son procesos de larga duración desplegados en **Railway** (proyecto `artestahub-web`, entorno `dev`). Llaman a la API REST también desplegada en Railway.

- API: `https://lector-facturas-api-dev.up.railway.app`
- Los workers solo se arrancan/paran desde Railway; la lógica de negocio vive en la API.

---

## Base de datos (PostgreSQL)

Todas las tablas están en el schema `invoices`.

### `invoices.ingestion_queue`
Cola principal de facturas descargadas del correo.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `id` | UUID | PK |
| `company_code` | TEXT | `SL`, `LTD`, `INC` |
| `drive_file_id` | TEXT | ID del archivo en Google Drive |
| `drive_file_name` | TEXT | Nombre del archivo |
| `parse_status` | TEXT | `pending` / `ok` / `failed` |
| `validation_bucket` | TEXT | `to_check` / `to_process` / `validated` |
| `parse_error` | TEXT | Mensaje de error si `parse_status='failed'` |
| `parsed_at` | TIMESTAMPTZ | Fecha de parseo |
| `created_at` | TIMESTAMPTZ | Fecha de creación |

### `invoices.documents`
Facturas ya procesadas y validadas.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `id` | UUID | PK |
| `company_code` | TEXT | Sociedad |
| `supplier_code` | TEXT | Código del proveedor (ej. `IPOSTAL`, `METAADS`) |
| `period_yyyymm` | VARCHAR(6) | Periodo (ej. `202601`) |
| `category` | TEXT | `cogs` / `opex` / `income` |
| `subcategory` | TEXT | `manufacturing`, `logistics`, `marketing`, `staff`, `administration`, `technology`, `otros_gastos`, etc. |
| `amount` | NUMERIC | Importe bruto |
| `currency` | VARCHAR(3) | `EUR`, `GBP`, `USD` |
| `amount_eur` | NUMERIC | Importe convertido a EUR |
| `drive_file_id` | TEXT | ID en Google Drive |
| `source` | TEXT | Parser que lo procesó |

### `invoices.expense_rows`
Filas de gasto para el P&G (desnormalizadas para la hoja de cálculo).

### `invoices.otros_gastos`
Gastos manuales no cubiertos por facturas automáticas.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `company_code` | TEXT | Sociedad |
| `period_yyyymm` | VARCHAR(6) | Periodo |
| `amount_eur` | NUMERIC | Importe en EUR (positivo = gasto) |
| `notes` | TEXT | Descripción |

API: `PUT /otros-gastos/{company_code}/{period_yyyymm}`

### `invoices.otros_ingresos`
Ingresos manuales no cubiertos por ventas automáticas.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `company_code` | TEXT | Sociedad |
| `period_yyyymm` | VARCHAR(6) | Periodo |
| `amount_eur` | NUMERIC | Importe en EUR (positivo = ingreso) |

API: `PUT /otros-ingresos/{company_code}/{period_yyyymm}`

### `invoices.diferencias_divisas`
Ajuste de conversión de divisas por período. Los valores son **negativos** cuando hay pérdida de cambio. Se resta del beneficio (gasto en negativo: `profit = turnover - cogs - opex + diferencias_divisas`).

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `id` | SERIAL | PK |
| `company_code` | VARCHAR(10) | Sociedad |
| `period_yyyymm` | VARCHAR(6) | Periodo (UNIQUE por sociedad) |
| `amount_eur` | NUMERIC(12,2) | Importe EUR (negativo = pérdida de cambio) |
| `notes` | TEXT | Descripción |

Migración inicial: `python scripts/migrate_diferencias_divisas.py`

Datos iniciales SL:
- `202601`: -132.90 €
- `202602`: -277.52 €

### `invoices.payment_order_transactions`
Transacciones individuales de Shopify / PayPal.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `order_name` | TEXT | Nº de pedido (ej. `AS-12345`) |
| `platform` | TEXT | `shopify` / `paypal` |
| `company_code` | TEXT | Sociedad |
| `transaction_type` | TEXT | `charge`, `refund`, `dispute_withdrawal`, `dispute_reversal`, … |
| `gross_amount` | NUMERIC | Importe bruto en moneda original |
| `fee_amount` | NUMERIC | Comisión del TPV |
| `net_amount` | NUMERIC | Neto recibido |
| `affects_balance` | BOOL | Si la transacción mueve el saldo del payout |
| `is_cancelled` | BOOL | Transacción anulada |
| `is_chargeback` | BOOL | True en dispute_withdrawal / T1111 |

### `invoices.shopify_payout_transactions`
Transacciones de payout de Shopify con fee por pedido.

| Columna clave | Descripción |
|---------------|-------------|
| `order_name` | Nº de pedido |
| `company_code` | Sociedad |
| `type` | `charge`, `refund`, `payout`, … |
| `amount` | Importe bruto |
| `fee` | Comisión Shopify Payments para este pedido |
| `net` | Neto (amount - fee) |

Fuente primaria para la columna **"Shopify fee"** en `shopify_sales_inc_*.xlsx`. Para el total mensual de fees que va al P&G se usa `payment_fee_monthly_summary.total_cost_amount`.

### `invoices.payment_fee_monthly_summary`
Resumen mensual de comisiones de pago, agregado por `company_code`, `platform`, `period_yyyymm`.

| Columna | Descripción |
|---------|-------------|
| `gross_amount` | Ventas brutas procesadas por el TPV |
| `fee_amount` | Comisiones del TPV |
| `chargeback_amount` | Importe de chargebacks |
| `chargeback_fee_amount` | Comisión de tramitación del chargeback |
| `total_cost_amount` | `fee_amount + chargeback_fee_amount` — **este es el valor que va al P&G** |
| `net_amount` | Neto liquidado |

### `invoices.worker_coordination`
Coordinación entre workers para evitar que el P&G se genere mientras se están procesando facturas.

| Columna | Descripción |
|---------|-------------|
| `job_name` | `invoice_processing` |
| `is_running` | `TRUE` mientras corre el worker-03 |
| `heartbeat_at` | Último ping (stale si > 4h) |

### `finance.informe_vat_gestorias_resumen_{yyyymm}`
Tablas particionadas (una por mes). Resumen de ventas por sociedad × país × tasa IVA × método de pago, sin pedidos Hannun.

| Columna | Descripción |
|---------|-------------|
| `payment_currency` | Moneda (`EUR`, `GBP`, `USD`) |
| `country` | País de envío |
| `shipping_state_code` | Estado (solo US) |
| `tax_rate_teorical` | Tipo IVA teórico según país |
| `tax_rate_shopify` | Tipo IVA registrado por Shopify |
| `tax_rate_calculated` | Tipo calculado (tax/net) |
| `num_orders` | Número de pedidos |
| `imp_sales_gross` | Ventas brutas |
| `imp_sales_tax` | IVA / Sales tax |
| `imp_sales_net` | Ventas netas |
| `is_hannun_tag` | 1 = pedido Hannun (excluido en el resumen) |

### `finance.informe_vat_gestorias_detalle`
Tabla única con detalle a nivel de pedido. Filtrar siempre por `order_month_yyyymm` y `payment_currency`.

| Columna | Descripción |
|---------|-------------|
| `order_name` | Nº de pedido |
| `order_date` | Fecha del pedido |
| `shipping_country_code` | País de envío |
| `shipping_state_code` | Estado (US) |
| `payment_gateway_names` | JSON array de métodos de pago (`shopify_payments`, `paypal`, …) |
| `shown_gross_presentment` | Bruto en moneda de presentación |
| `shown_tax_presentment` | Impuesto en moneda de presentación |
| `shown_net_presentment` | Neto en moneda de presentación |
| `descuadre` | Diferencia entre neto declarado y calculado |
| `is_hannun_tag` | 1 = pedido Hannun |
| `is_rever_tag` | 1 = pedido Rever (devolución) |
| `standard_rate` | Tipo IVA teórico del país |
| `tax_rate` | Tipo IVA aplicado por Shopify |

---

## Libros P&G (pyg_*.xlsx)

Se generan cuatro libros, uno por sociedad y uno consolidado. El consolidado NO incluye la pestaña de Nº Facturas.

### Estructura de pestañas

| Pestaña | Descripción |
|---------|-------------|
| `P&G-SL` / `P&G-LTD` / `P&G-INC` | Hoja principal de PyG |
| `Nº Facturas-SL` / `-LTD` / `-INC` | Conteo de facturas (misma estructura, ver abajo) |
| `g-expenses-sl` / `-ltd` / `-inc` | Detalle de gastos (fuente de SUMIFS) |
| `g-payment-fees-sl` / etc. | Detalle de comisiones de pago |
| `g-services-sl` | Detalle de servicios intercompany |
| `g-shopify-sl` / `i-*` | Detalle de ventas Shopify |
| `fx-rates` | Tipos de cambio auditados |

### Estructura de filas (P&G)

```
Turnover
  Product sales
    Shopify  (por mercado: ES, EU, INT, US, UK)
    Marketplaces  (HANNUN, TOASTY, CHOOSE)       ← solo SL
    Rappels  (LIVITUM)                            ← solo SL
    Supplies  (REVER)                             ← solo SL
  Services  (HANNUN, QHANDS, Ltd, Inc)           ← solo SL
  Otros ingresos

Expenses
  COGS
    Manufacturing  (por proveedor: TORRAS, PRESSING, etc.)
    Logistics      (por proveedor: CORREOS, GLS, UPS, etc.)
    Royalties
    Payment fees   (SHOPIFY, PAYPAL)
  [KPIs: Gross margin, Contributive margin]
  Opex
    Marketing      (METAADS, GOOGLEADS por mercado)
    Staff          (PAYROLL, DOSCONSULTING)        ← solo SL
    Shared services (SL, LTD, Inc)                ← LTD / INC
    Administration (por proveedor)
    Technology     (por proveedor)
    Otros gastos
  Diferencias divisas   ← después de Otros gastos, fuera del Opex

PROFIT
% Profit / turnover
```

### Fórmulas clave

```
Turnover     = product_sales + services + otros_ingresos
Expenses     = cogs + opex
Profit       = turnover - cogs - opex - diferencias_divisas
```

> **Diferencias divisas** se posiciona después del bloque Opex (debajo de Otros gastos) y se resta del beneficio. Los valores almacenados son negativos (ej. -132.90 €) por lo que la resta de un número negativo actúa como suma, aumentando el beneficio. No forma parte de Turnover ni de Expenses.

### Pestaña "Nº Facturas"

Misma estructura de filas que el P&G principal pero muestra el **número de facturas** de cada línea en lugar del importe. Creada automáticamente en SL, LTD e INC (no en el consolidado).

**Marcado en rojo (último mes con datos):** una celda se pinta de rojo (`#FFAAAA`) si:
- El mes anterior tenía > 0 facturas **Y**
- El último mes tiene 0 facturas **O** tiene menos de la mitad que el mes anterior

---

## Pipeline de ingestión de facturas

```
1. worker-01  POST /jobs/email-download/run
   → Escanea correo (Gmail API), detecta adjuntos con extensiones de factura
   → Sube el PDF/XML a Drive en /validation/to-check/<sociedad>/
   → Inserta en ingestion_queue con parse_status='pending', validation_bucket='to_check'

2. worker-03  POST /jobs/invoice-processing/run
   → Para cada fila en to_check:
     a. Descarga el archivo de Drive
     b. Detecta el parser por nombre de archivo / remitente
     c. Parsea → extrae (sociedad, período, importe, divisa, proveedor)
     d. Calcula ruta destino en Drive (ej. /validated/SL/2026/01/IPOSTAL/)
     e. Renombra el archivo al formato estándar (ej. IPOSTAL_202601_001.pdf)
     f. Mueve a la carpeta destino
     g. Actualiza ingestion_queue: parse_status='ok', validation_bucket='validated'
   → Si falla: parse_status='failed', validation_bucket='to_check', graba parse_error

3. worker-05/06/07/08  POST /integrations/pyg/<sociedad>/sync
   → Espera a que worker-03 esté inactivo (worker_coordination)
   → Consulta BD, construye bundle de datos
   → Genera .xlsx con openpyxl
   → Sube (sobreescribe) en Drive
```

### Parsers disponibles

`adobe`, `apphoto`, `artesta_income`, `artist_royalties`, `artlink`, `canva`, `claris`, `continuum`, `correos`, `dct`, `gls`, `godaddy`, `googleworkspace`, `gorgias`, `hannun`, `hetzner`, `ipostal`, `jondo`, `konvoai`, `marketing_ads`, `masmovil`, `microsoft`, `noda`, `openai`, `partner_income_fr`, `payroll`, `portclearance`, `pressing`, `proco`, `producthero`, `quickbooks`, `railway`, `regus`, `rever`, `shared_services`, `shopify`, `spring`, `tgi`, `torras`, `ups`, `vitaly`, `youraccountstaxes`, `yumaai`

### Catálogo de proveedores

`src/lector_facturas/config/providers_master.csv` — mapea nombre de proveedor → `supplier_code`, `category`, `subcategory`, `destination_path`.

---

## API REST

Base URL: `https://lector-facturas-api-dev.up.railway.app`

Autenticación: `Authorization: Bearer <API_SECRET_KEY>` (opcional; si `API_SECRET_KEY` no está en Railway, el endpoint es público). Solo `/health` está siempre libre.

### Health

| Método | Ruta | Descripción |
|--------|------|-------------|
| GET | `/health` | Healthcheck |

### Jobs (workers llaman a estos endpoints)

| Método | Ruta | Descripción |
|--------|------|-------------|
| POST | `/jobs/email-download/run` | Descarga adjuntos del correo (worker-01) |
| POST | `/jobs/email-review/run` | Revisión de correo (worker-02) |
| POST | `/jobs/invoice-processing/run` | Procesa facturas en to-check (worker-03) |
| POST | `/jobs/validation-to-process/run` | Mueve manualmente de to-check a to-process |
| POST | `/jobs/payment-fees/run` | Sync de comisiones (worker-04) |
| POST | `/jobs/daily-run` | Run diario combinado |
| POST | `/jobs/daily-review-email/run` | Envía digest de revisión (worker-09) |
| GET  | `/jobs/mail-sync/state` | Estado del último sync de correo |

### Revisión

| Método | Ruta | Descripción |
|--------|------|-------------|
| GET | `/review-items` | Lista ítems pendientes de revisión |
| GET | `/review-items/{id}` | Detalle de un ítem |
| POST | `/review-items/{id}/resolve` | Resuelve un ítem de revisión |
| GET | `/validation/queue` | Cola de ingestión |
| GET | `/validation/queue/{id}` | Detalle de un ítem de la cola |
| POST | `/validation/to-process/upload` | Sube un archivo manualmente |

### Datos manuales

| Método | Ruta | Descripción |
|--------|------|-------------|
| PUT | `/otros-gastos/{company_code}/{period_yyyymm}` | Upsert de gasto manual |
| PUT | `/otros-ingresos/{company_code}/{period_yyyymm}` | Upsert de ingreso manual |
| DELETE | `/documents/{document_id}` | Elimina un documento procesado |

### P&G sync

| Método | Ruta | Descripción |
|--------|------|-------------|
| POST | `/integrations/pyg/sl/sync` | Genera y sube pyg_sl_YYYY.xlsx |
| POST | `/integrations/pyg/ltd/sync` | Genera y sube pyg_ltd_YYYY.xlsx |
| POST | `/integrations/pyg/inc/sync` | Genera y sube pyg_inc_YYYY.xlsx |
| POST | `/integrations/pyg/consolidated/sync` | Genera y sube pyg_consolidado_YYYY.xlsx |

Body (JSON): `{ "year": 2026, "file_name": "pyg_sl_2026.xlsx", "drive_folder_id": "..." }`

### Informes de ventas (supply)

| Método | Ruta | Descripción |
|--------|------|-------------|
| POST | `/supply/gestoria/sync` | Genera `shopify_sales_{company}_{yyyymm}.xlsx` y lo sube a Drive |
| POST | `/supply/payment-reconciliation/sync` | Genera `payment_reconciliation_{company}_{yyyymm}.xlsx` y lo sube a Drive |

**Body común (JSON):** `{ "company_code": "SL", "period_yyyymm": "202603" }`

**`/supply/gestoria/sync`** genera el informe de ventas para la gestoría con dos pestañas:
- **Summary** — ventas agregadas por país × tipo IVA, con subtotales y grand total. Para INC incluye columna *Shopify fee* por estado y total que cuadra con `payment_fee_monthly_summary` (misma fuente que el P&G).
- **Detail** — un pedido por fila con importes, tasas IVA, discrepancias y flags. Para INC incluye columna *Shopify fee* por pedido.

Ruta Drive: `{entidad}/{año}/{yyyymm}/income/sales/shopify/shopify_sales_{company}_{yyyymm}.xlsx`

**`/supply/payment-reconciliation/sync`** coteja pedido a pedido contabilidad vs canal de pago (Shopify Payments + PayPal) con pestañas: *Resumen*, *Chargebacks*, *Shopify*, *PayPal*, *Bank Transfer*, *Gift Cards*.

Ruta Drive: `{entidad}/{año}/{yyyymm}/income/sales/shopify/payment_reconciliation_{company}_{yyyymm}.xlsx`

### Integraciones

| Método | Ruta | Descripción |
|--------|------|-------------|
| GET | `/integrations/google-drive/status` | Estado de la conexión Drive |
| POST | `/integrations/google-drive/bootstrap` | Crea estructura de carpetas en Drive |
| POST | `/integrations/payment-fees/sync` | Sync de comisiones Shopify/PayPal |
| GET | `/payment-fees/transactions` | Lista transacciones de comisiones |
| GET | `/payment-fees/summary` | Resumen mensual de comisiones |

### Catálogo

| Método | Ruta | Descripción |
|--------|------|-------------|
| GET | `/companies` | Lista de sociedades |
| GET | `/suppliers` | Lista de proveedores (filtrable por `?company=SL`) |

---

## Workers (Railway)

Cada worker es un proceso independiente en Railway que llama a la API en loop.

| Worker | Script | Horario | Variables de entorno clave |
|--------|--------|---------|---------------------------|
| `lf-01-email-download` | `worker_01_email_download.py` | Cada 30 min | `EMAIL_DOWNLOAD_RUN_URL`, `EMAIL_REVIEW_MAILBOX`, `EMAIL_DOWNLOAD_INTERVAL_MINUTES` |
| `lf-02-email-review` | `worker_02_email_review.py` | Cada 30 min | `EMAIL_REVIEW_RUN_URL` |
| `lf-03-invoice-processing` | `worker_03_invoice_processing.py` | Cada 15 min | `INVOICE_PROCESSING_RUN_URL` |
| `lf-04-payment-fees` | `worker_04_payment_fees.py` | Diario 02:30 | `PAYMENT_FEES_RUN_URL`, `PAYMENT_FEES_LOOKBACK_DAYS` |
| `lf-05-pyg-sl` | `worker_05_pyg_sl.py` | Diario 20:00 | `PYG_SL_RUN_URL`, `PYG_SL_YEAR`, `PYG_SL_DRIVE_FOLDER_ID` |
| `lf-06-pyg-ltd` | `worker_06_pyg_ltd.py` | Diario 20:10 | `PYG_LTD_RUN_URL`, `PYG_LTD_YEAR` |
| `lf-07-pyg-inc` | `worker_07_pyg_inc.py` | Diario 20:20 | `PYG_INC_RUN_URL`, `PYG_INC_YEAR` |
| `lf-08-pyg-consolidated` | `worker_08_pyg_consolidated.py` | Diario 20:30 | `PYG_CONSOLIDATED_RUN_URL`, `PYG_CONSOLIDATED_YEAR` |
| `lf-09-daily-summary` | `worker_09_daily_summary.py` | Diario 20:00 | `DAILY_SUMMARY_RUN_URL` |
| `lf-10-sales-report` | `worker_11_daily_reports.py` | Diario 08:00 | `API_BASE_URL`, `REPORTS_HOUR`, `REPORTS_MINUTE`, `REPORTS_CLOSE_DAY`, `REPORTS_COMPANIES`, `REPORTS_TIMEZONE` |

Los workers 05-08 esperan a que el worker-03 termine antes de generar el P&G (coordinación via `invoices.worker_coordination`). Si el job de facturas lleva más de 4h sin heartbeat se considera stale y no se bloquea.

### `lf-10-sales-report` — Informes de ventas diarios

Genera y actualiza diariamente los ficheros de ventas en Google Drive para todas las sociedades.

**Script:** `scripts/worker_11_daily_reports.py`
**Horario:** 08:00 Madrid

Llama vía HTTP a dos endpoints de la API para cada sociedad (`SL`, `LTD`, `INC`) y el mes en curso:

| Endpoint | Fichero generado | Ruta en Drive |
|----------|-----------------|---------------|
| `POST /supply/gestoria/sync` | `shopify_sales_{company}_{yyyymm}.xlsx` | `{entidad}/{año}/{yyyymm}/income/sales/shopify/` |
| `POST /supply/payment-reconciliation/sync` | `payment_reconciliation_{company}_{yyyymm}.xlsx` | `{entidad}/{año}/{yyyymm}/income/sales/shopify/` |

**Lógica de cierre de mes:** los primeros `REPORTS_CLOSE_DAY` días del mes nuevo (por defecto 2) también regenera el mes anterior, capturando pedidos tardíos. A partir del día 3 ya solo actualiza el mes en curso.

**Variables de entorno:**

| Variable | Por defecto | Descripción |
|----------|------------|-------------|
| `API_BASE_URL` | — (obligatorio) | URL de la API |
| `API_KEY` | — | Bearer token (si la API requiere auth) |
| `REPORTS_HOUR` | `8` | Hora de ejecución (local) |
| `REPORTS_MINUTE` | `0` | Minuto de ejecución |
| `REPORTS_CLOSE_DAY` | `2` | Días del mes nuevo que regenera también el mes anterior |
| `REPORTS_TIMEZONE` | `Europe/Madrid` | Zona horaria |
| `REPORTS_COMPANIES` | `SL,LTD,INC` | Sociedades a procesar |

**Alertas:** tras 3 fallos consecutivos llama a `send_worker_failure_alert`.

---

## Variables de entorno (Railway)

### Google / Gmail / Drive

| Variable | Descripción |
|----------|-------------|
| `GOOGLE_CLIENT_ID` | OAuth2 client ID |
| `GOOGLE_CLIENT_SECRET` | OAuth2 client secret |
| `GOOGLE_REFRESH_TOKEN` | Refresh token (scope: `drive`, `gmail.readonly`, `gmail.send`, `spreadsheets`) |
| `GOOGLE_DRIVE_ROOT_FOLDER_ID` | ID de la carpeta raíz en Drive donde se guardan los PyGs |
| `GOOGLE_DRIVE_SHARED_DRIVE_ID` | ID del shared drive (opcional) |
| `GMAIL_SENDER` | Correo remitente / buzón a escanear (ej. `andrea@artestastore.com`) |
| `GMAIL_RECIPIENTS` | Destinatarios de alertas separados por coma |

### Base de datos

| Variable | Descripción |
|----------|-------------|
| `DATABASE_URL` | PostgreSQL connection string |

### Shopify

| Variable | Descripción |
|----------|-------------|
| `SHOPIFY_SHOP` | Nombre de la tienda (ej. `artesta`) |
| `SHOPIFY_CLIENT_ID` | API key de Shopify |
| `SHOPIFY_CLIENT_SECRET` | API secret de Shopify |
| `SHOPIFY_API_VERSION` | Versión API (ej. `2026-01`) |

### PayPal

| Variable | Descripción |
|----------|-------------|
| `PAYPAL_CLIENT_ID` | Client ID de PayPal |
| `PAYPAL_CLIENT_SECRET` | Client secret de PayPal |
| `PAYPAL_BASE_URL` | `https://api-m.paypal.com` (producción) |

### Seguridad

| Variable | Descripción |
|----------|-------------|
| `API_SECRET_KEY` | Bearer token para proteger la API (opcional; si no está definido la API es pública) |
| `COMPANY_NAME` | Nombre visible en notificaciones (ej. `Artesta Store`) |

---

## Scripts de utilidad

### Estructura de carpetas en Drive

```bash
python scripts/bootstrap_google_drive_structure.py --year 2026 --start-month 1 --end-month 12
```

### Crear carpeta del mes siguiente (local / OneDrive)

```bash
python scripts/create_next_month_structure.py --root "C:\...\ARTESTA - 6. Finances"
```

Wrapper PowerShell para el programador de tareas:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\run_monthly_structure_job.ps1
```

### Migración: tabla diferencias_divisas

Crea la tabla y carga los datos iniciales (SL enero/febrero 2026):

```bash
python scripts/migrate_diferencias_divisas.py
# con DATABASE_URL explícita:
python scripts/migrate_diferencias_divisas.py --database-url postgresql://...
```

### Sync de comisiones de pago

```bash
# Rango explícito
python scripts/sync_payment_fees.py --date-from 2026-01-01 --date-to 2026-01-31 --platform shopify

# Ventana móvil (para ejecución nocturna)
python scripts/sync_payment_fees.py --lookback-days 45
```

### Generar P&G localmente (sin Railway)

```bash
# Requiere DATABASE_URL en .env.local y credenciales Google
python scripts/build_pyg_sl_workbook.py --year 2026
python scripts/build_pyg_ltd_workbook.py --year 2026
python scripts/build_pyg_inc_workbook.py --year 2026
```

### Importar facturas manualmente a Drive

Hay un script `scripts/import_<proveedor>_to_drive.py` para cada proveedor principal (ipostal, correos, gls, hannun, payroll_sl, etc.). Todos aceptan `--year` y `--month`.

### Notificar proveedor no reconocido

```bash
python scripts/notify_unmatched_supplier.py \
  --company SL --year 2026 --period 202603 \
  --sender "billing@example.com" \
  --subject "Invoice attached" \
  --file "C:\ruta\factura.pdf"
```

---

## Informes de ventas mensuales

Generados diariamente por el worker `lf-10-sales-report` para las tres sociedades y subidos a Google Drive. Hay dos tipos de informe por sociedad y mes:

```
shopify_sales_{company}_{yyyymm}.xlsx          — Informe para la gestoría (VAT / Sales tax)
payment_reconciliation_{company}_{yyyymm}.xlsx — Cotejo ventas contabilidad vs pago
```

### Estructura en Drive

```
ARTESTA - 6. Finances
├── Artesta Store, S.L
│   └── 2026
│       └── 202603
│           └── income / sales / shopify
│               ├── shopify_sales_sl_202603.xlsx
│               └── payment_reconciliation_sl_202603.xlsx
├── Artesta Stores (UK) Ltd
│   └── (misma estructura)
└── Artesta Inc
    └── (misma estructura)
```

### `shopify_sales_{company}_{yyyymm}.xlsx`

Generado por `gestoria_workbook.py` + `pyg_sync.sync_gestoria_to_drive()`.

**Pestaña Summary**

| Columna | Descripción |
|---------|-------------|
| Country / State | País o estado (US agrupa por estado) |
| VAT rate theoretical | Tipo IVA teórico del país |
| VAT rate Shopify | Tipo registrado por Shopify |
| VAT rate calculated | Calculado como tax / net |
| # Orders | Número de pedidos |
| Gross | Ventas brutas en moneda de presentación |
| VAT / Tax | Impuesto |
| Net | Neto |
| Shopify fee | *(solo INC)* Fees de Shopify Payments por estado. Grand total = `payment_fee_monthly_summary.total_cost_amount` (= línea "SHOPIFY" del P&G INC) |
| Status | ✓ si las tasas coinciden, ⚠ diff si difieren > 0.001 |

Filas amarillas = discrepancia de tasa IVA entre Shopify y teórico.

**Pestaña Detail**

Un pedido por fila. Para INC incluye la columna *Shopify fee* entre *Net* y *Discrepancy*, con el fee por pedido de `invoices.shopify_payout_transactions`.

**Fuentes de datos:**
- Resumen: `finance.informe_vat_gestorias_resumen_{yyyymm}` (particionada por mes)
- Detalle: `finance.informe_vat_gestorias_detalle` filtrada por `order_month_yyyymm` y `payment_currency`
- Fees INC: `invoices.shopify_payout_transactions` (por pedido) + `invoices.payment_fee_monthly_summary` (total mensual)

**Mapeo sociedad → moneda → región:**

| Sociedad | Moneda | Región | Filtro |
|----------|--------|--------|--------|
| SL | EUR | EU | `payment_currency = 'EUR'` |
| LTD | GBP | UK | `payment_currency = 'GBP'` |
| INC | USD | US | `payment_currency = 'USD'` |

### `payment_reconciliation_{company}_{yyyymm}.xlsx`

Generado por `payment_reconciliation.py` + `payment_reconciliation_workbook.py` + `pyg_sync.sync_payment_reconciliation_to_drive()`.

**Pestañas:**

| Pestaña | Contenido |
|---------|-----------|
| Resumen | Tabla de totales por canal + leyenda de estados |
| Chargebacks | Inventario de chargebacks últimos 12 meses (abiertos, ganados, perdidos) |
| Shopify | Cotejo Shopify Payments: solo contab. / solo pago / diferencias |
| PayPal | Cotejo PayPal: solo contab. / solo pago / diferencias |
| Bank Transfer | Pedidos pagados por transferencia (no cotejan con TPV) |
| Gift Cards | Pedidos pagados con tarjeta regalo (no cotejan con TPV) |

**Fuentes de datos:**
- Contabilidad: `finance.informe_vat_gestorias_detalle`
- Shopify: `invoices.shopify_payout_transactions` (charges)
- PayPal: `invoices.paypal_transactions_raw`
- Chargebacks: `invoices.shopify_payout_transactions` (dispute_withdrawal, dispute_reversal)

**Tolerancia:** diferencia considerada si `|accounting - payment| > 0.01` en moneda de presentación.

**Nota sobre cobertura histórica:** `shopify_payout_transactions` cubre desde julio 2025 para INC y agosto 2025 para SL/LTD. Períodos anteriores mostrarán muchos "solo contabilidad" que son falsos positivos.

### Módulos Python

| Módulo | Descripción |
|--------|-------------|
| `gestoria_workbook.py` | `collect_gestoria_data()` + `build_gestoria_workbook()` — lógica pura sin Drive |
| `payment_reconciliation.py` | `build_reconciliation()` — lógica pura sin Excel ni Drive |
| `payment_reconciliation_workbook.py` | `build_payment_reconciliation_workbook()` — genera el `.xlsx` |
| `pyg_sync.sync_gestoria_to_drive()` | Orquesta: llama a workbook + sube a Drive |
| `pyg_sync.sync_payment_reconciliation_to_drive()` | Orquesta: llama a reconciliation + workbook + Drive |
| `scripts/worker_11_daily_reports.py` | Worker de larga duración; llama a la API vía HTTP |

---

## Despliegue en Railway

```bash
# Instalar Railway CLI
npm install -g @railway/cli
railway login

# Desde la raíz del repo, con el proyecto vinculado
railway up --service lector-facturas-api --environment dev
```

El build usa **Railpack** (Python 3.12). El comando de arranque está en `Procfile`:

```
web: PYTHONPATH=/app/src uvicorn lector_facturas.api.app:app --host 0.0.0.0 --port ${PORT:-8000}
```

Los workers arrancan con sus propios comandos en Railway (definidos en `railway.toml`).

---

## Desarrollo local

```bash
# Instalar dependencias
pip install -r requirements.txt

# Variables de entorno locales
cp .env.local.example .env.local  # editar con tus credenciales

# Arrancar la API
PYTHONPATH=src uvicorn lector_facturas.api.app:app --reload --port 8000
```

El archivo `.env.local` se carga automáticamente por `settings.load_settings()`. Las claves del `.env.local` nunca sobreescriben variables de entorno ya definidas en el sistema.

---

## Notas sobre Google Drive y OAuth

- El token necesita los siguientes scopes: `drive`, `gmail.readonly`, `gmail.send`, `spreadsheets`.
- Se puede verificar con: `GET https://www.googleapis.com/oauth2/v3/tokeninfo?access_token=<token>`
- Los archivos en Google Drive (Shared Drives) requieren `supportsAllDrives=true` en todas las llamadas a la API.
- El error `403 insufficientFilePermissions` en el servidor Railway (pero no en local) puede deberse a restricciones de la cuenta de servicio en el Shared Drive. Workaround: usar `My Drive` en lugar de Shared Drive para los archivos de facturas, o asignar permisos explícitos al OAuth user en el Shared Drive.

---

## Problemas conocidos y soluciones

### "Excel encontró un problema con el contenido" al abrir pyg_*.xlsx

**Síntoma:** Excel muestra el diálogo de reparación al abrir un archivo P&G. En el log de reparación aparece `Vista de /xl/worksheets/sheetN.xml parte`.

**Causa:** openpyxl genera XML inválido en el elemento `<sheetView>` cuando se combina `row_dimensions[row].hidden = True` + `merge_cells()` + `ws.freeze_panes` y después se llama a `insert_rows()` sobre la misma hoja. Esto afectaba a las hojas `Nº Facturas-*`.

**Solución aplicada:**
- Las hojas `Nº Facturas-*` se excluyen de `_add_back_links` (que hacía el `insert_rows`)
- Se reemplazó `row_dimensions[1].hidden = True` por `row_dimensions[1].height = 1` (misma apariencia, sin el flag hidden que genera el XML conflictivo)

### Facturas vuelven a `to-check` después de procesarse (error 403 en Drive)

**Síntoma:** El worker-03 renombra la factura correctamente pero falla al moverla a la carpeta destino. El error es `Google Drive API request failed: 403 insufficientFilePermissions`. Ocurre en Railway pero no localmente con las mismas credenciales.

**Causa conocida:** Restricción de permisos en el Shared Drive del servidor Railway. El mismo token OAuth funciona localmente porque el cliente local tiene acceso directo; el servidor Railway puede tener restricciones de red o de contexto en las llamadas a la API.

**Estado:** pendiente de resolución. Los archivos afectados quedan en `invoices.ingestion_queue` con `parse_status='failed'` y pueden moverse manualmente a `to-process` para reintentarlo.

### Diferencias divisas — convención de signos

Los valores en `invoices.diferencias_divisas` se almacenan con el **signo contable**:
- Pérdida de cambio → valor negativo (ej. -132.90)
- Ganancia de cambio → valor positivo

La fórmula en el P&G es `profit = turnover - cogs - opex - diferencias_divisas`. Dado que los valores son negativos, el resultado es `profit = ... - (-132.90) = ... + 132.90`, lo que **aumenta** el beneficio cuando hay pérdida de cambio negativa.

> Si la convención cambia (valores almacenados como positivos para pérdidas), cambiar el signo en `_fill_ltd_formulas` / `_fill_sl_formulas` / `_fill_inc_formulas` y en `_compose_row` del consolidado.

### Añadir un nuevo proveedor al P&G

1. Añadir la fila en `providers_master.csv` con `supplier_code`, `category`, `subcategory`, `destination_path`.
2. Si el proveedor cae en `opex/administration` o `opex/technology`, aparecerá automáticamente en el P&G en la siguiente generación.
3. Si requiere un parser nuevo, crear `src/lector_facturas/parsers/<proveedor>.py` y registrarlo en `invoice_ingestion.py`.

### Añadir un nuevo mes/año al P&G

Los P&G generan siempre los 12 meses del año indicado (`year` parameter en el endpoint). Los meses sin datos quedan a cero. No se necesita configuración adicional.

### openpyxl `insert_rows` corrompe fórmulas en shared-services

**Síntoma:** La hoja `shared-services` del P&G-SL mostraba valores erróneos o referencias rotas tras añadir una fila de back-link con `ws.insert_rows(1)`.

**Causa:** `insert_rows` desplaza los datos pero NO actualiza referencias a strings de fórmulas en otras celdas, por lo que cualquier fórmula que apuntara a una fila absoluta (e.g. `$A$1`) seguía apuntando a la fila equivocada.

**Solución aplicada:**
- La hoja `shared-services` se excluye de `_add_back_links` (que hacía el `insert_rows`)
- Se genera **después** de `_add_back_links`, por lo que nunca sufre el desplazamiento
- El back-link se añade manualmente en la fila 1 usando `ws.cell(1, 1).hyperlink`
- El yyyymm de referencia está en la fila 2 (`{col}$2`); todas las fórmulas usan esa referencia

---

## P&G Consolidado — Arquitectura de fórmulas

El consolidado (`pyg_consolidado_YYYY.xlsx`) ya no contiene valores hardcodeados. Toda la lógica numérica vive en fórmulas Excel que leen de tres hojas de datos y una hoja de tipos de cambio.

### Hojas de datos crudos

| Hoja | Contenido | Columnas |
|------|-----------|---------|
| `i-sl` | Datos SL en EUR | `yyyymm` \| `line_key` \| `amount_eur` |
| `i-ltd` | Datos LTD en GBP | `yyyymm` \| `line_key` \| `amount_gbp` |
| `i-inc` | Datos INC en USD | `yyyymm` \| `line_key` \| `amount_usd` |
| `fx-rates` | Tipos de cambio BCE fin de mes | `yyyymm` \| `currency` \| `reference_rate` |

**`fx-rates`:** `reference_rate` = unidades de moneda extranjera por 1 EUR (convención BCE).
- GBP/EUR = 0.83 → para convertir GBP→EUR: `amount_gbp / 0.83`
- USD/EUR = 1.08 → para convertir USD→EUR: `amount_usd / 1.08`

Los tipos provienen de la API XML del BCE (`sdw-wsrest.ecb.europa.eu`), usando el último día hábil del mes (EOM rate).

### Fórmulas P&G Consolidado

```excel
-- Helpers (fila 1 = yyyymm del mes)
GBP_RATE = AVERAGEIFS('fx-rates'!$C:$C,'fx-rates'!$A:$A,D$1,'fx-rates'!$B:$B,"GBP")
USD_RATE = AVERAGEIFS('fx-rates'!$C:$C,'fx-rates'!$A:$A,D$1,'fx-rates'!$B:$B,"USD")

-- SL (ya en EUR)
sl("product_sales") = SUMIFS('i-sl'!$C:$C,'i-sl'!$A:$A,D$1,'i-sl'!$B:$B,"product_sales")

-- LTD (GBP → EUR dividiendo por GBP_RATE)
ltd("product_sales") = IFERROR(SUMIFS('i-ltd'!$C:$C,...)/ GBP_RATE, 0)

-- INC (USD → EUR dividiendo por USD_RATE)
inc("product_sales") = IFERROR(SUMIFS('i-inc'!$C:$C,...)/ USD_RATE, 0)

-- Shared services (eliminación intercompany)
-- SL cobra servicios a LTD/INC (services_interco en EUR)
-- LTD/INC pagan shared_services (en GBP/USD)
-- En el consolidado: LTD_pay/GBP + INC_pay/USD - SL_income ≈ 0
shared_services = ltd("shared_services") + inc("shared_services") - sl("services_interco")
```

### Eliminación intercompany de Shared Services

SL factura servicios compartidos a LTD e INC en EUR. LTD paga en GBP (convirtiendo al tipo del período), INC paga en USD. Al consolidar, LTD y INC convierten de vuelta a EUR con el mismo tipo BCE → la línea `shared_services` del consolidado es exactamente cero.

```
SL recibe:  +X EUR  (services_interco)
LTD paga:   -X EUR  → almacenado como -(X * GBP_rate) GBP → en Excel / GBP_rate = -X EUR
INC paga:   -Y EUR  → almacenado como -(Y * USD_rate) USD → en Excel / USD_rate = -Y EUR
Neto consolidado:  LTD + INC - SL  ≈  0
```

---

## P&G-SL — Hoja Shared Services

La hoja `shared-services` del libro `pyg_sl_YYYY.xlsx` muestra el desglose de los costes de servicios compartidos que SL presta a LTD e INC, con el porcentaje aplicado a cada sociedad.

### Estructura de filas

| Fila | Contenido |
|------|-----------|
| 1 | Back-link a P&G-SL (hipervínculo) |
| 2 | yyyymm de cada mes (oculta) — usada como clave de lookup en SUMIFS |
| 3 | Título + nombres de meses |
| 4 | **LTD** (cabecera) |
| 5 | Marketing LTD |
| 6 | Royalties LTD |
| 7 | Staff LTD |
| 8 | Admin + Tech LTD |
| 9 | **TOTAL LTD EUR** |
| 10 | TOTAL LTD GBP |
| 11 | (vacía) |
| 12 | **INC** (cabecera) |
| 13-18 | (misma estructura) |
| 19 | (vacía) |
| 20 | **TOTAL SHARED SERVICES EUR** (LTD_EUR + INC_EUR) |

### Parámetros de reparto

Los porcentajes de reparto (`pct_marketing_uk`, `pct_royalties_uk`, `pct_admin_uk`, etc.) se definen en `src/lector_facturas/config/params_sl.csv`. Las filas de Admin + Tech de SL se suman antes de aplicar el porcentaje:

```python
LTD_AD = (P&G-SL[administration_row] + P&G-SL[technology_row]) * pct_admin_uk
```

---

## P&G-SL — Royalties por ámbito geográfico

Dentro de la sección "Royalties (% sales)" del P&G-SL, se muestran tres sub-filas colapsables con el desglose por zona:

| Sub-fila | `summary_scope` |
|----------|----------------|
| `eu` | Royalties Europa |
| `uk` | Royalties Reino Unido |
| `us` | Royalties EE.UU. |

**Fuente de datos:** `invoices.artist_royalties_monthly_summary` filtrada por `summary_scope IN ('eu', 'uk', 'us', 'total')`.

**Fórmula en Excel:**
```excel
=SUMIFS('i-royalties-scope-sl'!$C:$C,
        'i-royalties-scope-sl'!$A:$A, D$1,
        'i-royalties-scope-sl'!$B:$B, "eu")
```

---

## Control de pagos (Payment Tracking)

### Tablas de base de datos

#### `invoices.suppliers`

Catálogo de proveedores con términos de pago.

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `code` | TEXT | Código del proveedor (PK) |
| `name` | TEXT | Nombre |
| `company_code` | TEXT | Sociedad (`SL`, `LTD`, `INC`) |
| `payment_terms_days` | INT | Días de pago pactados |
| `is_direct_debit` | BOOL | True si es domiciliación |

#### `invoices.documents` — columnas de pago

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `payment_status` | TEXT | `pending` / `paid` / `partial` / `direct_debit` |
| `payment_date` | DATE | Fecha de pago efectivo |
| `payment_method` | TEXT | `bank_transfer` / `direct_debit` / `card` / `other` |
| `payment_amount` | NUMERIC | Importe pagado (si parcial) |
| `payment_due_date` | DATE | Fecha de vencimiento calculada |
| `is_overdue` | BOOL | True si `payment_due_date < today AND payment_status = 'pending'` |

### Endpoints de Payment Tracking

| Método | Ruta | Descripción |
|--------|------|-------------|
| `GET` | `/suppliers?company_code=SL` | Lista proveedores con términos de pago |
| `PATCH` | `/suppliers/{code}/payment-settings?company_code=SL` | Actualiza días y domiciliación de un proveedor |
| `GET` | `/documents/payment-status?company_code=&period_yyyymm=&payment_status=&overdue_only=` | Lista facturas con estado de pago |
| `POST` | `/documents/{id}/payment` | Registra pago de una factura |

**Body `PATCH /suppliers/{code}/payment-settings`:**
```json
{ "payment_terms_days": 30, "is_direct_debit": true }
```

**Body `POST /documents/{id}/payment`:**
```json
{
  "payment_status": "paid",
  "payment_date": "2026-03-15",
  "payment_method": "bank_transfer",
  "payment_amount": 1234.56,
  "payment_due_date": "2026-03-31"
}
```

**Query params `GET /documents/payment-status`:**

| Param | Tipo | Descripción |
|-------|------|-------------|
| `company_code` | TEXT | Filtro por sociedad (opcional) |
| `period_yyyymm` | TEXT | Filtro por mes, e.g. `202603` (opcional) |
| `payment_status` | TEXT | `pending` / `paid` / `partial` / `direct_debit` (opcional) |
| `overdue_only` | BOOL | Solo facturas vencidas (opcional) |
