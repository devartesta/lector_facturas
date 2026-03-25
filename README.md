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

### `invoices.payment_fee_monthly_summary`
Resumen mensual de comisiones de pago, agregado por `company_code`, `platform`, `period_yyyymm`.

### `invoices.worker_coordination`
Coordinación entre workers para evitar que el P&G se genere mientras se están procesando facturas.

| Columna | Descripción |
|---------|-------------|
| `job_name` | `invoice_processing` |
| `is_running` | `TRUE` mientras corre el worker-03 |
| `heartbeat_at` | Último ping (stale si > 4h) |

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
| `lf-09-daily-summary` | `worker_09_daily_summary.py` | Diario 08:00 | `DAILY_SUMMARY_RUN_URL` |

Los workers 05-08 esperan a que el worker-03 termine antes de generar el P&G (coordinación via `invoices.worker_coordination`). Si el job de facturas lleva más de 4h sin heartbeat se considera stale y no se bloquea.

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
