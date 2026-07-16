# Manual de integracion JDBC para Velneo

Documento para el equipo de desarrollo que va a consumir la base de datos desde Velneo.

## 1. Credenciales y conexion

Acceso creado en PostgreSQL:

```text
JDBC URL: jdbc:postgresql://shinkansen.proxy.rlwy.net:33105/railway?currentSchema=velneo
Usuario: velneo_read_access
Password: entregar por canal seguro
Schema por defecto: velneo
Permisos: solo SELECT sobre vistas del schema velneo
```

La password generada esta guardada localmente en `.env.velneo.local` como `VELNEO_DB_PASSWORD`. Ese fichero esta ignorado por Git y no debe subirse al repositorio. Para entregar la password al equipo, copiarla desde ese fichero y enviarla por un canal seguro.

Ejemplo de propiedades JDBC:

```properties
url=jdbc:postgresql://shinkansen.proxy.rlwy.net:33105/railway?currentSchema=velneo
user=velneo_read_access
password=<password entregada por canal seguro>
```

## 2. Paso a paso para acceder

### Paso 1. Recibir credenciales

El equipo de Velneo debe recibir estos tres datos:

```text
Host: shinkansen.proxy.rlwy.net
Puerto: 33105
Base de datos: railway
Usuario: velneo_read_access
Password: <password entregada por canal seguro>
Schema: velneo
```

Tambien puede usarse directamente esta URL JDBC:

```text
jdbc:postgresql://shinkansen.proxy.rlwy.net:33105/railway?currentSchema=velneo
```

La password no se envia en este documento. Debe compartirse aparte por canal seguro.

### Paso 2. Configurar el driver JDBC

Usar el driver oficial de PostgreSQL para JDBC. En la configuracion de la conexion:

```text
Driver: PostgreSQL JDBC
URL: jdbc:postgresql://shinkansen.proxy.rlwy.net:33105/railway?currentSchema=velneo
User: velneo_read_access
Password: <password entregada por canal seguro>
```

Si la herramienta no acepta `currentSchema` en la URL, configurar el schema por defecto como `velneo` en las propiedades avanzadas. El rol tambien tiene `search_path = velneo`, asi que las consultas sin schema deberian resolver contra `velneo`.

### Paso 3. Probar conexion

Ejecutar:

```sql
SELECT current_user, current_schema();
```

Resultado esperado:

```text
current_user = velneo_read_access
current_schema = velneo
```

### Paso 4. Listar las vistas disponibles

Ejecutar:

```sql
SELECT table_name
FROM information_schema.views
WHERE table_schema = 'velneo'
ORDER BY table_name;
```

Resultado esperado:

```text
artist_royalties_documents
artist_royalties_monthly_summary
document_breakdowns
documents
frame_purchase_lines
frame_purchases
frame_stock_monthly
payment_fee_monthly_summary
payment_order_transactions
payroll_documents
purchase_invoices_pyg
```

### Paso 5. Probar lecturas basicas

Ejecutar:

```sql
SELECT count(*) FROM documents;
SELECT count(*) FROM payroll_documents;
SELECT count(*) FROM payment_fee_monthly_summary;
SELECT count(*) FROM frame_stock_monthly;
```

Si estas consultas funcionan, el acceso de lectura esta operativo.

### Paso 6. Consultar datos con filtros

Ejemplos:

```sql
SELECT *
FROM documents
WHERE company_code = 'SL'
  AND period_yyyymm = '202604'
ORDER BY invoice_date, supplier_code, invoice_number;
```

```sql
SELECT *
FROM payment_fee_monthly_summary
WHERE company_code = 'SL'
  AND period_yyyymm >= '202601'
ORDER BY period_yyyymm, platform, market_code;
```

```sql
SELECT *
FROM frame_stock_monthly
WHERE mes_yyyymm = '202604'
ORDER BY fabricante;
```

```sql
SELECT *
FROM purchase_invoices_pyg
WHERE period_yyyymm = '202605'
ORDER BY market_code, invoice_date, supplier_code, invoice_number;
```

### Paso 7. Validar que el acceso esta limitado

Estas consultas deben fallar. Si funcionan, avisar al equipo interno porque el acceso no esta correctamente limitado:

```sql
SELECT count(*) FROM invoices.documents;
SELECT count(*) FROM supply.frame_stock_monthly;
INSERT INTO documents DEFAULT VALUES;
```

### Errores habituales

| Error | Causa probable | Accion |
| --- | --- | --- |
| `password authentication failed` | Password incorrecta o rotada | Pedir la password vigente por canal seguro. |
| `permission denied for schema invoices` | Se esta consultando `invoices.*` directamente | Usar las vistas sin prefijo o con prefijo `velneo.*`. |
| `relation "documents" does not exist` | No esta activo el schema `velneo` | Usar `?currentSchema=velneo` o consultar `velneo.documents`. |
| Timeout de conexion | Red, firewall o host/puerto incorrecto | Revisar host `shinkansen.proxy.rlwy.net` y puerto `33105`. |
| Error de escritura | Comportamiento esperado | El usuario es exclusivamente read-only. |

## 3. Alcance del acceso

El usuario `velneo_read_access` no tiene acceso directo a los schemas internos `invoices` ni `supply`. Solo puede leer vistas dentro del schema `velneo`.

Vistas disponibles:

| Vista | Contenido |
| --- | --- |
| `documents` | Facturas de compra procesadas |
| `document_breakdowns` | Desgloses de importes por factura |
| `payroll_documents` | Documentos y resumenes de nominas |
| `artist_royalties_documents` | Documentos individuales de royalties |
| `artist_royalties_monthly_summary` | Resumen mensual de royalties |
| `payment_fee_monthly_summary` | Resumen mensual de costes de pago |
| `payment_order_transactions` | Transacciones normalizadas de payment fees |
| `frame_stock_monthly` | Resumen mensual de stock de marcos |
| `frame_purchases` | Cabeceras de compras de marcos |
| `frame_purchase_lines` | Lineas de compras de marcos |
| `purchase_invoices_pyg` | Facturas de compra que realmente entran en PYG, listas para export mensual por `period_yyyymm` |

Quedan fuera tablas internas/raw como `ingestion_queue`, `review_items`, `paypal_transactions_raw`, `shopify_payout_transactions` y el resto de `supply`.

## 4. Consultas de prueba

Estas consultas deben funcionar:

```sql
SELECT count(*) FROM documents;
SELECT count(*) FROM payroll_documents;
SELECT count(*) FROM artist_royalties_monthly_summary;
SELECT count(*) FROM payment_fee_monthly_summary;
SELECT count(*) FROM frame_stock_monthly;
```

Estas consultas deben fallar por permisos:

```sql
SELECT count(*) FROM invoices.documents;
SELECT count(*) FROM supply.frame_stock_monthly;
INSERT INTO documents DEFAULT VALUES;
UPDATE documents SET status = status;
DELETE FROM documents;
```

## 5. Convenciones generales

| Campo | Descripcion |
| --- | --- |
| `id` | Identificador unico de la fila. Puede ser `uuid` o `integer` segun la vista. |
| `company_code` | Sociedad del grupo: normalmente `SL`, `LTD` o `INC`. |
| `period_yyyymm` / `mes_yyyymm` | Periodo contable en formato `YYYYMM`, por ejemplo `202604`. |
| `currency_code` / `currency` | Moneda ISO, por ejemplo `EUR`, `GBP` o `USD`. |
| `created_at` | Fecha/hora de creacion de la fila. |
| `updated_at` | Fecha/hora de ultima actualizacion de la fila. |
| `drive_file_id` / `drive_url` | Referencia al archivo archivado en Google Drive. |
| `extracted_raw` / `raw_payload` | JSON original o intermedio usado para trazabilidad. No deberia ser necesario para integraciones ordinarias. |

## 6. Diccionario de vistas y columnas

### `documents`

Facturas de compra ya procesadas y validadas.

| Columna | Tipo | Descripcion |
| --- | --- | --- |
| `id` | uuid | Identificador de la factura. |
| `invoice_number` | text | Numero de factura del proveedor. |
| `invoice_date` | date | Fecha de emision de la factura. |
| `issuer_company_name` | text | Razon social que emite la factura. |
| `billed_company_name` | text | Razon social facturada. |
| `issuer_tax_id` | text | NIF/CIF/VAT ID del emisor cuando este disponible; puede ser NULL. |
| `billed_tax_id` | text | NIF/CIF/VAT ID del receptor/facturado cuando este disponible; puede ser NULL. |
| `supplier_name` | text | Nombre del proveedor. |
| `company_code` | text | Sociedad del grupo asociada. |
| `windows_path` | text | Ruta local historica o de trabajo del archivo. |
| `drive_url` | text | URL del documento en Google Drive. |
| `vat_percent` | numeric | Porcentaje de IVA aplicado. |
| `gross_amount` | numeric | Importe bruto de la factura. |
| `vat_amount` | numeric | Importe de IVA. |
| `net_amount` | numeric | Importe neto sin IVA. |
| `supplier_id` | uuid | Identificador interno del proveedor. |
| `supplier_code` | text | Codigo normalizado del proveedor. |
| `currency_code` | text | Moneda de la factura. |
| `drive_file_id` | text | ID del archivo en Google Drive. |
| `storage_root` | text | Raiz o ubicacion logica de almacenamiento. |
| `document_type` | text | Tipo de documento, por ejemplo factura o abono. |
| `status` | text | Estado operativo del documento. |
| `source_channel` | text | Canal de origen, por ejemplo email o importacion. |
| `email_message_id` | text | ID del mensaje de email de origen. |
| `email_thread_id` | text | ID del hilo de email de origen. |
| `attachment_original_name` | text | Nombre original del adjunto. |
| `parser_name` | text | Parser que extrajo los datos. |
| `parser_confidence` | numeric | Confianza del parser cuando aplica. |
| `extracted_raw` | jsonb | Datos extraidos en bruto para auditoria. |
| `review_notes` | text | Notas de revision manual. |
| `created_at` | timestamp with time zone | Fecha/hora de creacion. |
| `updated_at` | timestamp with time zone | Fecha/hora de actualizacion. |
| `source_sender` | text | Remitente original. |
| `source_subject` | text | Asunto original. |
| `period_yyyymm` | text | Periodo contable `YYYYMM`. |
| `received_at` | timestamp with time zone | Fecha/hora de recepcion. |
| `sender_email` | text | Email del remitente. |
| `original_filename` | text | Nombre original del archivo. |
| `billing_period_start` | date | Inicio del periodo facturado. |
| `billing_period_end` | date | Fin del periodo facturado. |
| `division_invoice` | text | Division o agrupacion contable de la factura. |
| `payment_status` | text | Estado de pago. |
| `payment_date` | date | Fecha de pago registrada. |
| `payment_method` | text | Metodo de pago registrado. |
| `payment_amount` | numeric | Importe pagado registrado. |
| `payment_due_date` | date | Fecha de vencimiento de pago. |

### `document_breakdowns`

Desglose de importes asociados a una factura.

| Columna | Tipo | Descripcion |
| --- | --- | --- |
| `id` | uuid | Identificador del desglose. |
| `document_id` | uuid | Factura asociada, enlaza con `documents.id`. |
| `breakdown_code` | text | Codigo del desglose o concepto. |
| `net_amount` | numeric | Importe neto del concepto. |
| `vat_amount` | numeric | IVA del concepto. |
| `gross_amount` | numeric | Importe bruto del concepto. |
| `notes` | text | Notas del desglose. |
| `created_at` | timestamp with time zone | Fecha/hora de creacion. |
| `updated_at` | timestamp with time zone | Fecha/hora de actualizacion. |

### `payroll_documents`

Documentos de nomina y resumen de costes laborales.

| Columna | Tipo | Descripcion |
| --- | --- | --- |
| `id` | uuid | Identificador del documento de nomina. |
| `company_code` | text | Sociedad del grupo. |
| `provider_code` | text | Codigo del proveedor o gestor laboral. |
| `provider_name` | text | Nombre del proveedor. |
| `source_channel` | text | Canal de origen. |
| `source_sender` | text | Remitente original. |
| `source_subject` | text | Asunto original. |
| `original_filename` | text | Nombre original del archivo. |
| `document_type` | text | Tipo de documento de nomina. |
| `payroll_period_start` | date | Inicio del periodo de nomina. |
| `payroll_period_end` | date | Fin del periodo de nomina. |
| `period_yyyymm` | text | Periodo contable `YYYYMM`. |
| `employee_count` | integer | Numero de empleados incluidos. |
| `gross_pay_amount` | numeric | Salario bruto agregado. |
| `employee_deductions_amount` | numeric | Deducciones de empleados. |
| `net_pay_amount` | numeric | Neto a pagar agregado. |
| `employer_social_security_amount` | numeric | Coste de seguridad social a cargo de empresa. |
| `total_company_cost_amount` | numeric | Coste total empresa. |
| `social_security_liquidation_amount` | numeric | Liquidacion de seguridad social. |
| `tax_withholdings_amount` | numeric | Retenciones fiscales. |
| `currency_code` | text | Moneda. |
| `windows_path` | text | Ruta local historica o de trabajo. |
| `drive_file_id` | text | ID del archivo en Google Drive. |
| `drive_url` | text | URL del archivo en Google Drive. |
| `extracted_raw` | jsonb | Datos extraidos en bruto. |
| `review_notes` | text | Notas de revision. |
| `created_at` | timestamp with time zone | Fecha/hora de creacion. |
| `updated_at` | timestamp with time zone | Fecha/hora de actualizacion. |
| `stored_filename` | text | Nombre de archivo archivado. |
| `email_message_id` | text | ID del email de origen. |

### `artist_royalties_documents`

Documentos individuales de royalties de artistas.

| Columna | Tipo | Descripcion |
| --- | --- | --- |
| `id` | uuid | Identificador del documento. |
| `company_code` | text | Sociedad del grupo. |
| `supplier_code` | text | Codigo del proveedor de royalties. |
| `supplier_name` | text | Nombre del proveedor. |
| `invoice_number` | text | Numero de factura. |
| `credit_note_number` | text | Numero de abono, si aplica. |
| `invoice_date` | date | Fecha del documento. |
| `billing_period_start` | date | Inicio del periodo liquidado. |
| `billing_period_end` | date | Fin del periodo liquidado. |
| `period_yyyymm` | text | Periodo contable `YYYYMM`. |
| `artist_name` | text | Nombre del artista. |
| `artist_tax_id` | text | Identificador fiscal del artista. |
| `artist_email` | text | Email del artista. |
| `artist_country` | text | Pais del artista. |
| `artist_region_code` | text | Region o ambito del artista. |
| `payment_method` | text | Metodo de pago. |
| `gross_amount` | numeric | Importe bruto de royalties. |
| `withholding_percent` | numeric | Porcentaje de retencion. |
| `withholding_amount` | numeric | Importe retenido. |
| `net_amount` | numeric | Importe neto. |
| `currency_code` | text | Moneda. |
| `windows_path` | text | Ruta local historica o de trabajo. |
| `drive_file_id` | text | ID del archivo en Google Drive. |
| `drive_url` | text | URL del archivo en Google Drive. |
| `original_filename` | text | Nombre original del archivo. |
| `source_channel` | text | Canal de origen. |
| `parser_name` | text | Parser usado. |
| `parser_confidence` | numeric | Confianza del parser. |
| `extracted_raw` | jsonb | Datos extraidos en bruto. |
| `review_notes` | text | Notas de revision. |
| `created_at` | timestamp with time zone | Fecha/hora de creacion. |
| `updated_at` | timestamp with time zone | Fecha/hora de actualizacion. |

### `artist_royalties_monthly_summary`

Resumen mensual de royalties por sociedad y ambito.

| Columna | Tipo | Descripcion |
| --- | --- | --- |
| `id` | uuid | Identificador del resumen. |
| `company_code` | text | Sociedad del grupo. |
| `supplier_code` | text | Codigo del proveedor. |
| `summary_scope` | text | Ambito del resumen, por ejemplo total, eu, uk o us. |
| `period_yyyymm` | text | Periodo contable `YYYYMM`. |
| `posters_amount` | numeric | Importe asociado a posters. |
| `stationery_amount` | numeric | Importe asociado a stationery. |
| `gross_amount` | numeric | Importe bruto. |
| `withholding_amount` | numeric | Importe retenido. |
| `withholding_percent` | numeric | Porcentaje de retencion. |
| `net_amount` | numeric | Importe neto. |
| `paypal_amount` | numeric | Importe pagado via PayPal. |
| `bank_transfer_amount` | numeric | Importe pagado via transferencia bancaria. |
| `one_x_amount` | numeric | Importe pagado por metodo 1X, si aplica. |
| `source_filename` | text | Archivo fuente del resumen. |
| `windows_path` | text | Ruta local historica o de trabajo. |
| `drive_file_id` | text | ID del archivo en Google Drive. |
| `drive_url` | text | URL del archivo en Google Drive. |
| `extracted_raw` | jsonb | Datos extraidos en bruto. |
| `created_at` | timestamp with time zone | Fecha/hora de creacion. |
| `updated_at` | timestamp with time zone | Fecha/hora de actualizacion. |

### `payment_fee_monthly_summary`

Resumen mensual de costes de pasarelas o plataformas de pago.

| Columna | Tipo | Descripcion |
| --- | --- | --- |
| `id` | uuid | Identificador del resumen. |
| `company_code` | text | Sociedad del grupo. |
| `period_yyyymm` | text | Periodo contable `YYYYMM`. |
| `platform` | text | Plataforma de pago, por ejemplo Shopify o PayPal. |
| `market_code` | text | Mercado asociado. |
| `currency_code` | text | Moneda. |
| `orders_count` | integer | Numero de pedidos incluidos. |
| `transactions_count` | integer | Numero de transacciones incluidas. |
| `gross_amount` | numeric | Importe bruto procesado. |
| `fee_amount` | numeric | Comisiones ordinarias. |
| `chargeback_amount` | numeric | Importe de chargebacks. |
| `chargeback_fee_amount` | numeric | Comisiones de chargebacks. |
| `total_cost_amount` | numeric | Coste total de payment fees. |
| `net_amount` | numeric | Importe neto tras costes. |
| `payout_count` | integer | Numero de payouts incluidos. |
| `created_at` | timestamp with time zone | Fecha/hora de creacion. |
| `updated_at` | timestamp with time zone | Fecha/hora de actualizacion. |

### `payment_order_transactions`

Transacciones normalizadas usadas para calcular payment fees.

| Columna | Tipo | Descripcion |
| --- | --- | --- |
| `id` | uuid | Identificador de la transaccion. |
| `platform` | text | Plataforma de origen. |
| `company_code` | text | Sociedad del grupo. |
| `market_code` | text | Mercado asociado. |
| `currency_code` | text | Moneda. |
| `order_id` | text | ID externo del pedido. |
| `order_name` | text | Nombre o numero visible del pedido. |
| `external_transaction_id` | text | ID externo de transaccion. |
| `external_payout_id` | text | ID externo del payout. |
| `transaction_date` | timestamp with time zone | Fecha/hora de la transaccion. |
| `payout_date` | timestamp with time zone | Fecha/hora del payout. |
| `transaction_type` | text | Tipo de transaccion. |
| `status` | text | Estado de la transaccion. |
| `gross_amount` | numeric | Importe bruto. |
| `fee_amount` | numeric | Comision aplicada. |
| `net_amount` | numeric | Importe neto. |
| `chargeback_amount` | numeric | Importe de chargeback. |
| `chargeback_fee_amount` | numeric | Comision asociada a chargeback. |
| `affects_balance` | boolean | Indica si afecta al saldo. |
| `is_cancelled` | boolean | Indica si la transaccion esta cancelada. |
| `is_chargeback` | boolean | Indica si la transaccion es chargeback. |
| `payment_reference` | text | Referencia de pago. |
| `customer_reference` | text | Referencia de cliente. |
| `raw_payload` | jsonb | Payload original normalizado para auditoria. |
| `period_yyyymm` | text | Periodo contable `YYYYMM`. |
| `created_at` | timestamp with time zone | Fecha/hora de creacion. |
| `updated_at` | timestamp with time zone | Fecha/hora de actualizacion. |

### `frame_stock_monthly`

Resumen mensual de stock de marcos.

| Columna | Tipo | Descripcion |
| --- | --- | --- |
| `fabricante` | text | Fabricante o proveedor del marco. |
| `mes_yyyymm` | text | Mes del resumen en formato `YYYYMM`. |
| `currency` | text | Moneda de valoracion. |
| `opening_units` | integer | Unidades iniciales. |
| `opening_value` | numeric | Valor inicial del stock. |
| `purchased_units` | integer | Unidades compradas durante el mes. |
| `purchased_value` | numeric | Valor de las compras del mes. |
| `consumed_units` | integer | Unidades consumidas durante el mes. |
| `consumed_value` | numeric | Valor del consumo del mes. |
| `closing_units` | integer | Unidades finales. |
| `closing_value` | numeric | Valor final del stock. |
| `calculated_at` | timestamp with time zone | Fecha/hora de calculo del resumen. |

### `frame_purchases`

Cabeceras de compras de marcos.

| Columna | Tipo | Descripcion |
| --- | --- | --- |
| `id` | integer | Identificador de la compra. |
| `fabricante` | text | Fabricante o proveedor. |
| `purchase_date` | date | Fecha de compra. |
| `currency` | text | Moneda de la compra. |
| `notes` | text | Notas de la compra. |
| `created_at` | timestamp with time zone | Fecha/hora de creacion. |
| `updated_at` | timestamp with time zone | Fecha/hora de actualizacion. |

### `frame_purchase_lines`

Lineas de detalle de compras de marcos.

| Columna | Tipo | Descripcion |
| --- | --- | --- |
| `id` | integer | Identificador de la linea. |
| `purchase_id` | integer | Compra asociada, enlaza con `frame_purchases.id`. |
| `frame_color` | text | Color del marco. |
| `frame_size` | text | Tamano del marco. |
| `quantity` | integer | Unidades compradas. |
| `unit_price` | numeric | Precio unitario. |
| `currency` | text | Moneda de la linea. |
| `total` | numeric | Importe total de la linea. |

## 7. Recomendaciones de integracion

- Usar siempre el schema `velneo`; no referenciar `invoices` ni `supply`.
- Tratar todas las vistas como read-only.
- Filtrar por `period_yyyymm` o `mes_yyyymm` para cargas incrementales.
- Usar `id` como identificador tecnico cuando exista.
- Tener especial cuidado con datos personales en royalties y nominas.
- No depender de columnas JSON (`extracted_raw`, `raw_payload`) salvo para auditoria puntual.

## 8. Soporte operativo

Para rotar la password:

```sql
ALTER ROLE velneo_read_access PASSWORD '<NUEVO_PASSWORD_SEGURO>';
```

Para validar permisos desde una cuenta administradora:

```sql
SELECT table_schema, table_name, privilege_type
FROM information_schema.role_table_grants
WHERE grantee = 'velneo_read_access'
ORDER BY table_schema, table_name, privilege_type;
```

El resultado esperado es `SELECT` solo sobre objetos del schema `velneo`.
