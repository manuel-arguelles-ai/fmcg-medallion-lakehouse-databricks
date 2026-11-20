-- NO EJECUTADO — requiere confirmacion de Manu (crea tablas y vistas en Unity Catalog).
-- GENERADO desde src/gold/ddl.py; no editar a mano:
--   cd src && python -m gold.ddl fmcg_lakehouse > gold/sql/01_gold_ddl.sql
-- Requiere el esquema gold (infra/02-unity-catalog.sql) y el DDL de Silver ya aplicado
-- (las vistas de calidad leen ops.dq_results y quarantine.*). Todo es idempotente.
-- Orden: tablas gold.*, ops.gold_runs, vistas de KPI.

CREATE TABLE IF NOT EXISTS fmcg_lakehouse.gold.dim_date (
  `date_sk` INT COMMENT 'yyyymmdd; -1 = desconocido',
  `date` DATE COMMENT 'dia calendario',
  `year` INT,
  `quarter` INT COMMENT '1..4',
  `quarter_label` STRING COMMENT 'Q1..Q4',
  `month` INT COMMENT '1..12',
  `month_name` STRING COMMENT 'nombre del mes en espanol',
  `year_month` STRING COMMENT 'YYYY-MM',
  `month_start` DATE COMMENT 'primer dia del mes',
  `month_end` DATE COMMENT 'ultimo dia del mes',
  `day` INT COMMENT 'dia del mes',
  `day_of_week` INT COMMENT 'ISO: 1 = lunes ... 7 = domingo',
  `day_name` STRING COMMENT 'en espanol',
  `is_weekend` BOOLEAN COMMENT 'sabado o domingo',
  `week_of_year` INT COMMENT 'semana ISO',
  `day_of_year` INT COMMENT '1..366',
  `is_month_end` BOOLEAN COMMENT 'ultimo dia del mes',
  `is_unknown_member` BOOLEAN COMMENT 'fila del miembro desconocido',
  `_silver_updated_at` TIMESTAMP COMMENT 'ultima escritura de la fila origen en Silver',
  `_gold_loaded_at` TIMESTAMP COMMENT 'momento de la recarga de Gold'
)
COMMENT 'Calendario diario (anios completos que cubren los pedidos) + miembro -1. Clave unica: date_sk. Recarga completa (INSERT OVERWRITE)';

CREATE TABLE IF NOT EXISTS fmcg_lakehouse.gold.dim_customer (
  `customer_sk` BIGINT COMMENT 'clave sustituta = xxhash64(customer_key), estable entre recargas; -1 = desconocido',
  `company` STRING COMMENT 'madre | hija (''Unknown'' en el miembro -1)',
  `customer_id` STRING COMMENT 'clave natural',
  `customer_key` STRING COMMENT 'clave de negocio company:customer_id',
  `customer_name` STRING COMMENT 'nombre normalizado en Silver',
  `city` STRING COMMENT 'ciudad canonica o ''Unknown''',
  `customer_label` STRING COMMENT 'nombre-ciudad',
  `market` STRING COMMENT 'de silver.ref_company',
  `channel` STRING COMMENT 'de silver.ref_company',
  `name_was_normalized` BOOLEAN COMMENT 'Silver CUS_03/CUS_04',
  `city_was_corrected` BOOLEAN COMMENT 'Silver CUS_05',
  `city_was_null` BOOLEAN COMMENT 'Silver CUS_06',
  `city_is_unmapped` BOOLEAN COMMENT 'Silver CUS_07',
  `is_unknown_member` BOOLEAN COMMENT 'fila del miembro desconocido',
  `_silver_updated_at` TIMESTAMP COMMENT 'ultima escritura de la fila origen en Silver',
  `_gold_loaded_at` TIMESTAMP COMMENT 'momento de la recarga de Gold'
)
COMMENT 'Clientes SCD1 (estado actual de Silver) + miembro -1. Clave unica: company, customer_id. Recarga completa (INSERT OVERWRITE)';

CREATE TABLE IF NOT EXISTS fmcg_lakehouse.gold.dim_product (
  `product_sk` BIGINT COMMENT 'clave sustituta = xxhash64(product_key); -1 = desconocido',
  `company` STRING COMMENT 'madre | hija (''Unknown'' en el miembro -1)',
  `product_id` STRING COMMENT 'clave natural',
  `product_key` STRING COMMENT 'clave de negocio company:product_id',
  `product_code` STRING COMMENT 'sha2(product_name) de Silver',
  `product_name` STRING COMMENT 'nombre completo',
  `product` STRING COMMENT 'nombre sin variante',
  `variant` STRING COMMENT 'variante (texto entre parentesis)',
  `category` STRING COMMENT 'categoria canonica',
  `division` STRING COMMENT 'division por categoria o ''Unknown''',
  `product_id_was_prefixed` BOOLEAN COMMENT 'Silver PRD_02',
  `name_was_corrected` BOOLEAN COMMENT 'Silver PRD_04',
  `category_was_normalized` BOOLEAN COMMENT 'Silver PRD_05',
  `division_was_unknown` BOOLEAN COMMENT 'Silver PRD_06',
  `is_unknown_member` BOOLEAN COMMENT 'fila del miembro desconocido',
  `_silver_updated_at` TIMESTAMP COMMENT 'ultima escritura de la fila origen en Silver',
  `_gold_loaded_at` TIMESTAMP COMMENT 'momento de la recarga de Gold'
)
COMMENT 'Productos SCD1 (estado actual de Silver) + miembro -1. Clave unica: company, product_id. Recarga completa (INSERT OVERWRITE)';

CREATE TABLE IF NOT EXISTS fmcg_lakehouse.gold.dim_price (
  `price_sk` BIGINT COMMENT 'clave sustituta = xxhash64(product_key|valid_from); -1 = sin precio',
  `product_sk` BIGINT COMMENT 'dim_product (-1 si el producto no resuelve)',
  `company` STRING COMMENT 'madre | hija',
  `product_id` STRING COMMENT 'clave natural del producto',
  `product_key` STRING COMMENT 'company:product_id',
  `valid_from` DATE COMMENT 'inicio de vigencia (primer dia del mes del precio)',
  `valid_to` DATE COMMENT 'fin de vigencia (ultimo dia del mismo mes; no se estira a meses sin precio)',
  `unit_price` DECIMAL(12,2) COMMENT 'precio unitario vigente',
  `currency` STRING COMMENT 'moneda de Silver (no se fija en Gold)',
  `prev_unit_price` DECIMAL(12,2) COMMENT 'precio del mes calendario anterior (NULL si no hubo)',
  `price_change_pct` DECIMAL(12,4) COMMENT 'unit_price / prev_unit_price - 1',
  `price_was_negative` BOOLEAN COMMENT 'Silver PRC_04',
  `price_was_invalid` BOOLEAN COMMENT 'Silver PRC_05 (si la accion fuera marcar)',
  `month_was_reformatted` BOOLEAN COMMENT 'Silver PRC_03',
  `product_unresolved` BOOLEAN COMMENT 'Silver PRC_07',
  `is_unknown_member` BOOLEAN COMMENT 'fila del miembro desconocido',
  `_silver_updated_at` TIMESTAMP COMMENT 'ultima escritura de la fila origen en Silver',
  `_gold_loaded_at` TIMESTAMP COMMENT 'momento de la recarga de Gold'
)
COMMENT 'Precio por producto con vigencia mensual (valid_from/valid_to) + miembro -1. Clave unica: company, product_id, valid_from. Recarga completa (INSERT OVERWRITE)';

CREATE TABLE IF NOT EXISTS fmcg_lakehouse.gold.fact_sales (
  `company` STRING COMMENT 'madre | hija (dimension degenerada)',
  `order_id` STRING COMMENT 'dimension degenerada',
  `line_no` INT COMMENT 'dimension degenerada',
  `date_sk` INT COMMENT 'dim_date (yyyymmdd)',
  `customer_sk` BIGINT COMMENT 'dim_customer (-1 = desconocido)',
  `product_sk` BIGINT COMMENT 'dim_product (-1 = desconocido)',
  `price_sk` BIGINT COMMENT 'dim_price vigente en order_date (-1 = sin precio)',
  `order_date` DATE COMMENT 'fecha del pedido',
  `order_month_start` DATE COMMENT 'primer dia del mes del pedido',
  `order_qty` INT COMMENT 'cantidad',
  `unit_price` DECIMAL(12,2) COMMENT 'precio vigente (de Silver)',
  `line_amount` DECIMAL(18,2) COMMENT 'order_qty * unit_price, calculado en Silver (D28); NULL si no hay precio',
  `currency` STRING COMMENT 'moneda de line_amount (de Silver)',
  `price_missing` BOOLEAN COMMENT 'Silver ORD_10',
  `customer_id_was_invalid` BOOLEAN COMMENT 'Silver ORD_07',
  `customer_unresolved` BOOLEAN COMMENT 'Silver ORD_08',
  `product_unresolved` BOOLEAN COMMENT 'Silver ORD_09',
  `date_was_reformatted` BOOLEAN COMMENT 'Silver ORD_06',
  `_silver_updated_at` TIMESTAMP COMMENT 'ultima escritura de la fila origen en Silver',
  `_gold_loaded_at` TIMESTAMP COMMENT 'momento de la recarga de Gold'
)
CLUSTER BY (order_month_start, product_sk)
COMMENT 'Hecho de ventas, grano linea de pedido; monto = line_amount de Silver. Clave unica: company, order_id, line_no. Recarga completa (INSERT OVERWRITE)';

CREATE TABLE IF NOT EXISTS fmcg_lakehouse.gold.agg_sales_monthly (
  `month_start` DATE COMMENT 'mes (primer dia)',
  `year` INT,
  `quarter` INT,
  `month` INT,
  `company` STRING COMMENT 'madre | hija',
  `customer_sk` BIGINT COMMENT 'dim_customer',
  `product_sk` BIGINT COMMENT 'dim_product',
  `currency` STRING COMMENT 'moneda de revenue',
  `order_lines` BIGINT COMMENT 'lineas de pedido',
  `orders` BIGINT COMMENT 'pedidos distintos (NO aditivo entre productos/clientes)',
  `total_qty` BIGINT COMMENT 'suma de order_qty',
  `revenue` DECIMAL(28,2) COMMENT 'suma de line_amount (lineas sin precio no suman)',
  `lines_without_price` BIGINT COMMENT 'lineas con price_missing',
  `_silver_updated_at` TIMESTAMP COMMENT 'ultima escritura de la fila origen en Silver',
  `_gold_loaded_at` TIMESTAMP COMMENT 'momento de la recarga de Gold'
)
CLUSTER BY (month_start, product_sk)
COMMENT 'Agregado mes x producto x cliente derivado de fact_sales (reconstruible). Clave unica: month_start, company, customer_sk, product_sk, currency. Recarga completa (INSERT OVERWRITE)';

CREATE TABLE IF NOT EXISTS fmcg_lakehouse.gold.feature_sales_monthly (
  `month_start` DATE COMMENT 'mes de la observacion',
  `company` STRING COMMENT 'madre | hija',
  `customer_sk` BIGINT COMMENT 'dim_customer',
  `product_sk` BIGINT COMMENT 'dim_product',
  `currency` STRING COMMENT 'moneda de los montos',
  `revenue` DECIMAL(28,2) COMMENT 'ingreso del mes',
  `total_qty` BIGINT COMMENT 'cantidad del mes',
  `order_lines` BIGINT COMMENT 'lineas del mes',
  `revenue_lag_1` DECIMAL(28,2) COMMENT 'ingreso de hace 1 mes calendario (0 si no compro)',
  `revenue_lag_2` DECIMAL(28,2) COMMENT 'hace 2 meses',
  `revenue_lag_3` DECIMAL(28,2) COMMENT 'hace 3 meses',
  `qty_lag_1` BIGINT COMMENT 'cantidad de hace 1 mes (0 si no compro)',
  `qty_lag_2` BIGINT COMMENT 'hace 2 meses',
  `qty_lag_3` BIGINT COMMENT 'hace 3 meses',
  `revenue_avg_3m` DECIMAL(28,2) COMMENT 'promedio de los 3 meses previos (sin el actual)',
  `months_since_prev_purchase` INT COMMENT 'meses desde la compra anterior (NULL si es la primera)',
  `purchase_month_seq` INT COMMENT 'n-esimo mes con compras del par cliente-producto',
  `target_revenue_next_month` DECIMAL(28,2) COMMENT 'ETIQUETA: ingreso del mes siguiente; NULL si aun no se conoce',
  `_silver_updated_at` TIMESTAMP COMMENT 'ultima escritura de la fila origen en Silver',
  `_gold_loaded_at` TIMESTAMP COMMENT 'momento de la recarga de Gold'
)
CLUSTER BY (month_start, customer_sk)
COMMENT 'Features ML mes x cliente x producto con lags de calendario y etiqueta del mes siguiente. Clave unica: month_start, company, customer_sk, product_sk, currency. Recarga completa (INSERT OVERWRITE)';

CREATE TABLE IF NOT EXISTS fmcg_lakehouse.ops.gold_runs (
  `run_id` STRING COMMENT 'id de la corrida de Gold',
  `task` STRING COMMENT 'tabla/tarea',
  `target_table` STRING COMMENT 'tabla escrita',
  `rows_written` BIGINT COMMENT 'filas tras el INSERT OVERWRITE',
  `checks` STRING COMMENT 'reconciliaciones hechas (texto)',
  `started_at` TIMESTAMP,
  `finished_at` TIMESTAMP,
  `status` STRING COMMENT 'ok | failed',
  `error` STRING
)
COMMENT 'Una fila por tarea de Gold: filas escritas y reconciliaciones';

CREATE OR REPLACE VIEW fmcg_lakehouse.gold.v_kpi_sales_monthly
COMMENT 'KPI: ingresos, cantidad, pedidos y ticket por mes y compania'
AS
SELECT d.year, d.quarter, d.month, d.month_start, d.year_month,
  f.company, f.currency,
  SUM(f.line_amount) AS revenue,
  SUM(f.order_qty) AS total_qty,
  COUNT(DISTINCT f.order_id) AS orders,
  COUNT(*) AS order_lines,
  COUNT_IF(f.price_missing) AS lines_without_price,
  SUM(f.line_amount) / NULLIF(COUNT(DISTINCT f.order_id), 0) AS avg_ticket
FROM fmcg_lakehouse.gold.fact_sales f
JOIN fmcg_lakehouse.gold.dim_date d ON d.date_sk = f.date_sk
GROUP BY d.year, d.quarter, d.month, d.month_start, d.year_month, f.company, f.currency;

CREATE OR REPLACE VIEW fmcg_lakehouse.gold.v_kpi_top10_products
COMMENT 'KPI: top 10 productos por ingreso, por anio y Total (RANK, empates comparten puesto)'
AS
WITH by_period AS (
  SELECT CAST(a.year AS STRING) AS period, a.product_sk, a.currency,
         SUM(a.revenue) AS revenue, SUM(a.total_qty) AS total_qty
  FROM fmcg_lakehouse.gold.agg_sales_monthly a GROUP BY a.year, a.product_sk, a.currency
  UNION ALL
  SELECT 'Total' AS period, a.product_sk, a.currency, SUM(a.revenue), SUM(a.total_qty)
  FROM fmcg_lakehouse.gold.agg_sales_monthly a GROUP BY a.product_sk, a.currency
), ranked AS (
  SELECT b.*, RANK() OVER (PARTITION BY b.period, b.currency ORDER BY b.revenue DESC NULLS LAST) AS revenue_rank
  FROM by_period b
)
SELECT r.period, r.revenue_rank, p.company, p.product_name, p.product, p.variant, p.category, p.division,
       r.currency, r.revenue, r.total_qty, r.product_sk
FROM ranked r JOIN fmcg_lakehouse.gold.dim_product p ON p.product_sk = r.product_sk
WHERE r.revenue_rank <= 10;

CREATE OR REPLACE VIEW fmcg_lakehouse.gold.v_kpi_category_mix
COMMENT 'KPI: mix por division/categoria y participacion en el mes por compania'
AS
SELECT a.year, a.month_start, a.company, p.division, p.category, a.currency,
  SUM(a.revenue) AS revenue,
  SUM(a.total_qty) AS total_qty,
  SUM(a.revenue) / NULLIF(SUM(SUM(a.revenue)) OVER (PARTITION BY a.month_start, a.company, a.currency), 0)
    AS revenue_share
FROM fmcg_lakehouse.gold.agg_sales_monthly a
JOIN fmcg_lakehouse.gold.dim_product p ON p.product_sk = a.product_sk
GROUP BY a.year, a.month_start, a.company, p.division, p.category, a.currency;

CREATE OR REPLACE VIEW fmcg_lakehouse.gold.v_kpi_company_comparison
COMMENT 'KPI: comparativo madre vs hija por mes (participacion y variacion mensual)'
AS
WITH m AS (
  SELECT f.order_month_start AS month_start, f.company, f.currency,
         SUM(f.line_amount) AS revenue, SUM(f.order_qty) AS total_qty,
         COUNT(DISTINCT f.order_id) AS orders,
         COUNT(DISTINCT CASE WHEN f.customer_sk <> -1 THEN f.customer_sk END) AS active_customers,
         COUNT_IF(f.customer_sk = -1) AS lines_unknown_customer
  FROM fmcg_lakehouse.gold.fact_sales f GROUP BY f.order_month_start, f.company, f.currency
)
SELECT YEAR(m.month_start) AS year, m.*,
  m.revenue / NULLIF(m.orders, 0) AS avg_ticket,
  m.revenue / NULLIF(SUM(m.revenue) OVER (PARTITION BY m.month_start, m.currency), 0) AS revenue_share_month,
  m.revenue / NULLIF(LAG(m.revenue) OVER (PARTITION BY m.company, m.currency ORDER BY m.month_start), 0) - 1
    AS revenue_mom_pct
FROM m;

CREATE OR REPLACE VIEW fmcg_lakehouse.gold.v_kpi_data_quality
COMMENT 'Calidad: ultima corrida de Silver por entidad (ops.dq_results) con descripcion de la regla'
AS
WITH latest AS (
  SELECT entity, MAX(ts) AS ts FROM fmcg_lakehouse.ops.dq_results GROUP BY entity
), rules AS (
  SELECT * FROM VALUES
    ('CUS_01', 'customers', 'customer_id nulo o no numerico', 'error'),
    ('CUS_02', 'customers', 'customer_name nulo o vacio', 'error'),
    ('CUS_03', 'customers', 'espacios sobrantes en customer_name (trim + colapso)', 'info'),
    ('CUS_04', 'customers', 'customer_name fuera de initcap (MAYUSCULAS/minusculas)', 'info'),
    ('CUS_05', 'customers', 'typo de ciudad corregido con ref_city_mapping', 'info'),
    ('CUS_06', 'customers', 'ciudad nula -> ''Unknown'' (city_was_null)', 'warning'),
    ('CUS_07', 'customers', 'ciudad ausente de ref_city_mapping (city_is_unmapped)', 'warning'),
    ('CUS_08', 'customers', 'duplicado por (company, customer_id): gana mayor _ingest_ts', 'info'),
    ('PRD_01', 'products', 'product_id nulo o no reconocible (ni numerico ni PX-<n>)', 'error'),
    ('PRD_02', 'products', 'product_id con prefijo PX-<n> normalizado a <n>', 'info'),
    ('PRD_03', 'products', 'product_name nulo o vacio', 'error'),
    ('PRD_04', 'products', 'typo en product_name corregido con ref_typo_fixes', 'info'),
    ('PRD_05', 'products', 'category normalizada (mayusculas) con ref_category_division', 'info'),
    ('PRD_06', 'products', 'category ausente de ref_category_division -> division ''Unknown''', 'warning'),
    ('PRD_07', 'products', 'duplicado por (company, product_id): gana mayor _ingest_ts', 'info'),
    ('PRC_01', 'prices', 'product_id nulo o no reconocible', 'error'),
    ('PRC_02', 'prices', 'price_month nulo o con formato no soportado', 'error'),
    ('PRC_03', 'prices', 'price_month con formato distinto de yyyy-MM-dd (normalizado)', 'info'),
    ('PRC_04', 'prices', 'unit_price negativo -> valor absoluto + price_was_negative', 'warning'),
    ('PRC_05', 'prices', 'unit_price nulo o no numerico', 'error'),
    ('PRC_06', 'prices', 'duplicado por (company, product_id, price_month)', 'info'),
    ('PRC_07', 'prices', 'producto del precio ausente de silver.products (product_unresolved)', 'warning'),
    ('ORD_01', 'orders', 'order_id o line_no nulo o no numerico', 'error'),
    ('ORD_02', 'orders', 'order_date nulo o con formato no soportado', 'error'),
    ('ORD_03', 'orders', 'order_qty nulo', 'error'),
    ('ORD_04', 'orders', 'order_qty no es entero positivo', 'error'),
    ('ORD_05', 'orders', 'product_id nulo o no reconocible', 'error'),
    ('ORD_06', 'orders', 'order_date con formato distinto de yyyy-MM-dd (normalizado)', 'info'),
    ('ORD_07', 'orders', 'customer_id no numerico -> miembro -1 (customer_id_was_invalid)', 'warning'),
    ('ORD_08', 'orders', 'customer_id numerico ausente de silver.customers -> -1 (customer_unresolved)', 'warning'),
    ('ORD_09', 'orders', 'product_id ausente de silver.products -> -1, monto NULL (product_unresolved)', 'warning'),
    ('ORD_10', 'orders', 'sin precio vigente del mes -> line_amount NULL (price_missing)', 'warning'),
    ('ORD_11', 'orders', 'duplicado por (company, order_id, line_no): gana mayor _ingest_ts', 'info')
  AS r(rule_id, entity, description, severity)
)
SELECT q.entity, q.company, q.rule_id, r.description, r.severity, q.action,
  q.rows_evaluated, q.rows_failed,
  q.rows_failed / NULLIF(q.rows_evaluated, 0) AS fail_rate,
  q.mode, q.batch_id, q.ts
FROM fmcg_lakehouse.ops.dq_results q
JOIN latest l ON l.entity = q.entity AND l.ts = q.ts
LEFT JOIN rules r ON r.rule_id = q.rule_id;

CREATE OR REPLACE VIEW fmcg_lakehouse.gold.v_kpi_quarantine_summary
COMMENT 'Calidad: filas en cuarentena por entidad, compania y regla'
AS
SELECT entity, company, rule_id, COUNT(*) AS rows_quarantined,
  MAX(quarantined_at) AS last_quarantined_at
FROM (
  SELECT 'customers' AS entity, _company AS company, rule_id, quarantined_at FROM fmcg_lakehouse.quarantine.customers
  UNION ALL
  SELECT 'products' AS entity, _company AS company, rule_id, quarantined_at FROM fmcg_lakehouse.quarantine.products
  UNION ALL
  SELECT 'prices' AS entity, _company AS company, rule_id, quarantined_at FROM fmcg_lakehouse.quarantine.prices
  UNION ALL
  SELECT 'orders' AS entity, _company AS company, rule_id, quarantined_at FROM fmcg_lakehouse.quarantine.orders
) x
GROUP BY entity, company, rule_id;
