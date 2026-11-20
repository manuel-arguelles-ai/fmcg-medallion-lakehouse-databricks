-- GENERADO desde src/gold/ddl.py; no editar a mano:
--   cd src && python -m gold.ddl fmcg_lakehouse --comments > gold/sql/02_gold_comments.sql
-- Aplica a tablas YA creadas los comentarios de tabla (pregunta de negocio + consumidor) y de columna,
-- y recrea las vistas con comentarios por columna. Solo metadatos; idempotente.

COMMENT ON TABLE fmcg_lakehouse.gold.dim_date IS 'Calendario diario (anios completos que cubren los pedidos) + miembro -1. Clave unica: date_sk. Recarga completa (INSERT OVERWRITE). Responde: como se filtran y agrupan las ventas por anio, trimestre, mes, dia de semana o fin de semana. Consumidor: BI, AI';

ALTER TABLE fmcg_lakehouse.gold.dim_date ALTER COLUMN `date_sk` COMMENT 'yyyymmdd; -1 = desconocido';

ALTER TABLE fmcg_lakehouse.gold.dim_date ALTER COLUMN `date` COMMENT 'dia calendario';

ALTER TABLE fmcg_lakehouse.gold.dim_date ALTER COLUMN `year` COMMENT 'anio calendario (yyyy)';

ALTER TABLE fmcg_lakehouse.gold.dim_date ALTER COLUMN `quarter` COMMENT '1..4';

ALTER TABLE fmcg_lakehouse.gold.dim_date ALTER COLUMN `quarter_label` COMMENT 'Q1..Q4';

ALTER TABLE fmcg_lakehouse.gold.dim_date ALTER COLUMN `month` COMMENT '1..12';

ALTER TABLE fmcg_lakehouse.gold.dim_date ALTER COLUMN `month_name` COMMENT 'nombre del mes en espanol';

ALTER TABLE fmcg_lakehouse.gold.dim_date ALTER COLUMN `year_month` COMMENT 'YYYY-MM';

ALTER TABLE fmcg_lakehouse.gold.dim_date ALTER COLUMN `month_start` COMMENT 'primer dia del mes';

ALTER TABLE fmcg_lakehouse.gold.dim_date ALTER COLUMN `month_end` COMMENT 'ultimo dia del mes';

ALTER TABLE fmcg_lakehouse.gold.dim_date ALTER COLUMN `day` COMMENT 'dia del mes';

ALTER TABLE fmcg_lakehouse.gold.dim_date ALTER COLUMN `day_of_week` COMMENT 'ISO: 1 = lunes ... 7 = domingo';

ALTER TABLE fmcg_lakehouse.gold.dim_date ALTER COLUMN `day_name` COMMENT 'en espanol';

ALTER TABLE fmcg_lakehouse.gold.dim_date ALTER COLUMN `is_weekend` COMMENT 'sabado o domingo';

ALTER TABLE fmcg_lakehouse.gold.dim_date ALTER COLUMN `week_of_year` COMMENT 'semana ISO';

ALTER TABLE fmcg_lakehouse.gold.dim_date ALTER COLUMN `day_of_year` COMMENT '1..366';

ALTER TABLE fmcg_lakehouse.gold.dim_date ALTER COLUMN `is_month_end` COMMENT 'ultimo dia del mes';

ALTER TABLE fmcg_lakehouse.gold.dim_date ALTER COLUMN `is_unknown_member` COMMENT 'fila del miembro desconocido';

ALTER TABLE fmcg_lakehouse.gold.dim_date ALTER COLUMN `_silver_updated_at` COMMENT 'ultima escritura de la fila origen en Silver';

ALTER TABLE fmcg_lakehouse.gold.dim_date ALTER COLUMN `_gold_loaded_at` COMMENT 'momento de la recarga de Gold';

COMMENT ON TABLE fmcg_lakehouse.gold.dim_customer IS 'Clientes SCD1 (estado actual de Silver) + miembro -1. Clave unica: company, customer_id. Recarga completa (INSERT OVERWRITE). Responde: quien compra: cliente, ciudad, mercado y canal de cada venta. Consumidor: BI, AI';

ALTER TABLE fmcg_lakehouse.gold.dim_customer ALTER COLUMN `customer_sk` COMMENT 'clave sustituta = xxhash64(customer_key), estable entre recargas; -1 = desconocido';

ALTER TABLE fmcg_lakehouse.gold.dim_customer ALTER COLUMN `company` COMMENT 'madre | hija (''Unknown'' en el miembro -1)';

ALTER TABLE fmcg_lakehouse.gold.dim_customer ALTER COLUMN `customer_id` COMMENT 'clave natural';

ALTER TABLE fmcg_lakehouse.gold.dim_customer ALTER COLUMN `customer_key` COMMENT 'clave de negocio company:customer_id';

ALTER TABLE fmcg_lakehouse.gold.dim_customer ALTER COLUMN `customer_name` COMMENT 'nombre normalizado en Silver';

ALTER TABLE fmcg_lakehouse.gold.dim_customer ALTER COLUMN `city` COMMENT 'ciudad canonica o ''Unknown''';

ALTER TABLE fmcg_lakehouse.gold.dim_customer ALTER COLUMN `customer_label` COMMENT 'nombre-ciudad';

ALTER TABLE fmcg_lakehouse.gold.dim_customer ALTER COLUMN `market` COMMENT 'de silver.ref_company';

ALTER TABLE fmcg_lakehouse.gold.dim_customer ALTER COLUMN `channel` COMMENT 'de silver.ref_company';

ALTER TABLE fmcg_lakehouse.gold.dim_customer ALTER COLUMN `name_was_normalized` COMMENT 'Silver CUS_03/CUS_04';

ALTER TABLE fmcg_lakehouse.gold.dim_customer ALTER COLUMN `city_was_corrected` COMMENT 'Silver CUS_05';

ALTER TABLE fmcg_lakehouse.gold.dim_customer ALTER COLUMN `city_was_null` COMMENT 'Silver CUS_06';

ALTER TABLE fmcg_lakehouse.gold.dim_customer ALTER COLUMN `city_is_unmapped` COMMENT 'Silver CUS_07';

ALTER TABLE fmcg_lakehouse.gold.dim_customer ALTER COLUMN `is_unknown_member` COMMENT 'fila del miembro desconocido';

ALTER TABLE fmcg_lakehouse.gold.dim_customer ALTER COLUMN `_silver_updated_at` COMMENT 'ultima escritura de la fila origen en Silver';

ALTER TABLE fmcg_lakehouse.gold.dim_customer ALTER COLUMN `_gold_loaded_at` COMMENT 'momento de la recarga de Gold';

COMMENT ON TABLE fmcg_lakehouse.gold.dim_product IS 'Productos SCD1 (estado actual de Silver) + miembro -1. Clave unica: company, product_id. Recarga completa (INSERT OVERWRITE). Responde: que se vende: producto, variante, categoria y division. Consumidor: BI, AI';

ALTER TABLE fmcg_lakehouse.gold.dim_product ALTER COLUMN `product_sk` COMMENT 'clave sustituta = xxhash64(product_key); -1 = desconocido';

ALTER TABLE fmcg_lakehouse.gold.dim_product ALTER COLUMN `company` COMMENT 'madre | hija (''Unknown'' en el miembro -1)';

ALTER TABLE fmcg_lakehouse.gold.dim_product ALTER COLUMN `product_id` COMMENT 'clave natural';

ALTER TABLE fmcg_lakehouse.gold.dim_product ALTER COLUMN `product_key` COMMENT 'clave de negocio company:product_id';

ALTER TABLE fmcg_lakehouse.gold.dim_product ALTER COLUMN `product_code` COMMENT 'sha2(product_name) de Silver';

ALTER TABLE fmcg_lakehouse.gold.dim_product ALTER COLUMN `product_name` COMMENT 'nombre completo';

ALTER TABLE fmcg_lakehouse.gold.dim_product ALTER COLUMN `product` COMMENT 'nombre sin variante';

ALTER TABLE fmcg_lakehouse.gold.dim_product ALTER COLUMN `variant` COMMENT 'variante (texto entre parentesis)';

ALTER TABLE fmcg_lakehouse.gold.dim_product ALTER COLUMN `category` COMMENT 'categoria canonica';

ALTER TABLE fmcg_lakehouse.gold.dim_product ALTER COLUMN `division` COMMENT 'division por categoria o ''Unknown''';

ALTER TABLE fmcg_lakehouse.gold.dim_product ALTER COLUMN `product_id_was_prefixed` COMMENT 'Silver PRD_02';

ALTER TABLE fmcg_lakehouse.gold.dim_product ALTER COLUMN `name_was_corrected` COMMENT 'Silver PRD_04';

ALTER TABLE fmcg_lakehouse.gold.dim_product ALTER COLUMN `category_was_normalized` COMMENT 'Silver PRD_05';

ALTER TABLE fmcg_lakehouse.gold.dim_product ALTER COLUMN `division_was_unknown` COMMENT 'Silver PRD_06';

ALTER TABLE fmcg_lakehouse.gold.dim_product ALTER COLUMN `is_unknown_member` COMMENT 'fila del miembro desconocido';

ALTER TABLE fmcg_lakehouse.gold.dim_product ALTER COLUMN `_silver_updated_at` COMMENT 'ultima escritura de la fila origen en Silver';

ALTER TABLE fmcg_lakehouse.gold.dim_product ALTER COLUMN `_gold_loaded_at` COMMENT 'momento de la recarga de Gold';

COMMENT ON TABLE fmcg_lakehouse.gold.dim_price IS 'Precio por producto con vigencia mensual (valid_from/valid_to) + miembro -1. Clave unica: company, product_id, valid_from. Recarga completa (INSERT OVERWRITE). Responde: cual era el precio vigente de un producto en un mes y cuanto cambio respecto del mes anterior. Consumidor: BI, ML';

ALTER TABLE fmcg_lakehouse.gold.dim_price ALTER COLUMN `price_sk` COMMENT 'clave sustituta = xxhash64(product_key|valid_from); -1 = sin precio';

ALTER TABLE fmcg_lakehouse.gold.dim_price ALTER COLUMN `product_sk` COMMENT 'dim_product (-1 si el producto no resuelve)';

ALTER TABLE fmcg_lakehouse.gold.dim_price ALTER COLUMN `company` COMMENT 'madre | hija';

ALTER TABLE fmcg_lakehouse.gold.dim_price ALTER COLUMN `product_id` COMMENT 'clave natural del producto';

ALTER TABLE fmcg_lakehouse.gold.dim_price ALTER COLUMN `product_key` COMMENT 'company:product_id';

ALTER TABLE fmcg_lakehouse.gold.dim_price ALTER COLUMN `valid_from` COMMENT 'inicio de vigencia (primer dia del mes del precio)';

ALTER TABLE fmcg_lakehouse.gold.dim_price ALTER COLUMN `valid_to` COMMENT 'fin de vigencia (ultimo dia del mismo mes; no se estira a meses sin precio)';

ALTER TABLE fmcg_lakehouse.gold.dim_price ALTER COLUMN `unit_price` COMMENT 'precio unitario vigente';

ALTER TABLE fmcg_lakehouse.gold.dim_price ALTER COLUMN `currency` COMMENT 'moneda de Silver (no se fija en Gold)';

ALTER TABLE fmcg_lakehouse.gold.dim_price ALTER COLUMN `prev_unit_price` COMMENT 'precio del mes calendario anterior (NULL si no hubo)';

ALTER TABLE fmcg_lakehouse.gold.dim_price ALTER COLUMN `price_change_pct` COMMENT 'unit_price / prev_unit_price - 1';

ALTER TABLE fmcg_lakehouse.gold.dim_price ALTER COLUMN `price_was_negative` COMMENT 'Silver PRC_04';

ALTER TABLE fmcg_lakehouse.gold.dim_price ALTER COLUMN `price_was_invalid` COMMENT 'Silver PRC_05 (si la accion fuera marcar)';

ALTER TABLE fmcg_lakehouse.gold.dim_price ALTER COLUMN `month_was_reformatted` COMMENT 'Silver PRC_03';

ALTER TABLE fmcg_lakehouse.gold.dim_price ALTER COLUMN `product_unresolved` COMMENT 'Silver PRC_07';

ALTER TABLE fmcg_lakehouse.gold.dim_price ALTER COLUMN `is_unknown_member` COMMENT 'fila del miembro desconocido';

ALTER TABLE fmcg_lakehouse.gold.dim_price ALTER COLUMN `_silver_updated_at` COMMENT 'ultima escritura de la fila origen en Silver';

ALTER TABLE fmcg_lakehouse.gold.dim_price ALTER COLUMN `_gold_loaded_at` COMMENT 'momento de la recarga de Gold';

COMMENT ON TABLE fmcg_lakehouse.gold.fact_sales IS 'Hecho de ventas, grano linea de pedido; monto = line_amount de Silver. Clave unica: company, order_id, line_no. Recarga completa (INSERT OVERWRITE). Responde: cuanto se vendio (ingreso, cantidad, pedidos) por dia, cliente, producto y compania. Consumidor: BI, AI, ML';

ALTER TABLE fmcg_lakehouse.gold.fact_sales ALTER COLUMN `company` COMMENT 'madre | hija (dimension degenerada)';

ALTER TABLE fmcg_lakehouse.gold.fact_sales ALTER COLUMN `order_id` COMMENT 'dimension degenerada';

ALTER TABLE fmcg_lakehouse.gold.fact_sales ALTER COLUMN `line_no` COMMENT 'dimension degenerada';

ALTER TABLE fmcg_lakehouse.gold.fact_sales ALTER COLUMN `date_sk` COMMENT 'dim_date (yyyymmdd)';

ALTER TABLE fmcg_lakehouse.gold.fact_sales ALTER COLUMN `customer_sk` COMMENT 'dim_customer (-1 = desconocido)';

ALTER TABLE fmcg_lakehouse.gold.fact_sales ALTER COLUMN `product_sk` COMMENT 'dim_product (-1 = desconocido)';

ALTER TABLE fmcg_lakehouse.gold.fact_sales ALTER COLUMN `price_sk` COMMENT 'dim_price vigente en order_date (-1 = sin precio)';

ALTER TABLE fmcg_lakehouse.gold.fact_sales ALTER COLUMN `order_date` COMMENT 'fecha del pedido';

ALTER TABLE fmcg_lakehouse.gold.fact_sales ALTER COLUMN `order_month_start` COMMENT 'primer dia del mes del pedido';

ALTER TABLE fmcg_lakehouse.gold.fact_sales ALTER COLUMN `order_qty` COMMENT 'cantidad';

ALTER TABLE fmcg_lakehouse.gold.fact_sales ALTER COLUMN `unit_price` COMMENT 'precio vigente (de Silver)';

ALTER TABLE fmcg_lakehouse.gold.fact_sales ALTER COLUMN `line_amount` COMMENT 'order_qty * unit_price, calculado en Silver (D28); NULL si no hay precio';

ALTER TABLE fmcg_lakehouse.gold.fact_sales ALTER COLUMN `currency` COMMENT 'moneda de line_amount (de Silver)';

ALTER TABLE fmcg_lakehouse.gold.fact_sales ALTER COLUMN `price_missing` COMMENT 'Silver ORD_10';

ALTER TABLE fmcg_lakehouse.gold.fact_sales ALTER COLUMN `customer_id_was_invalid` COMMENT 'Silver ORD_07';

ALTER TABLE fmcg_lakehouse.gold.fact_sales ALTER COLUMN `customer_unresolved` COMMENT 'Silver ORD_08';

ALTER TABLE fmcg_lakehouse.gold.fact_sales ALTER COLUMN `product_unresolved` COMMENT 'Silver ORD_09';

ALTER TABLE fmcg_lakehouse.gold.fact_sales ALTER COLUMN `date_was_reformatted` COMMENT 'Silver ORD_06';

ALTER TABLE fmcg_lakehouse.gold.fact_sales ALTER COLUMN `_silver_updated_at` COMMENT 'ultima escritura de la fila origen en Silver';

ALTER TABLE fmcg_lakehouse.gold.fact_sales ALTER COLUMN `_gold_loaded_at` COMMENT 'momento de la recarga de Gold';

COMMENT ON TABLE fmcg_lakehouse.gold.agg_sales_monthly IS 'Agregado mes x producto x cliente derivado de fact_sales (reconstruible). Clave unica: month_start, company, customer_sk, product_sk, currency. Recarga completa (INSERT OVERWRITE). Responde: cuanto vendio cada par cliente-producto en cada mes (base del top 10 y del mix). Consumidor: BI, AI, ML';

ALTER TABLE fmcg_lakehouse.gold.agg_sales_monthly ALTER COLUMN `month_start` COMMENT 'mes (primer dia)';

ALTER TABLE fmcg_lakehouse.gold.agg_sales_monthly ALTER COLUMN `year` COMMENT 'anio de month_start';

ALTER TABLE fmcg_lakehouse.gold.agg_sales_monthly ALTER COLUMN `quarter` COMMENT 'trimestre de month_start (1..4)';

ALTER TABLE fmcg_lakehouse.gold.agg_sales_monthly ALTER COLUMN `month` COMMENT 'mes de month_start (1..12)';

ALTER TABLE fmcg_lakehouse.gold.agg_sales_monthly ALTER COLUMN `company` COMMENT 'madre | hija';

ALTER TABLE fmcg_lakehouse.gold.agg_sales_monthly ALTER COLUMN `customer_sk` COMMENT 'dim_customer';

ALTER TABLE fmcg_lakehouse.gold.agg_sales_monthly ALTER COLUMN `product_sk` COMMENT 'dim_product';

ALTER TABLE fmcg_lakehouse.gold.agg_sales_monthly ALTER COLUMN `currency` COMMENT 'moneda de revenue';

ALTER TABLE fmcg_lakehouse.gold.agg_sales_monthly ALTER COLUMN `order_lines` COMMENT 'lineas de pedido';

ALTER TABLE fmcg_lakehouse.gold.agg_sales_monthly ALTER COLUMN `orders` COMMENT 'pedidos distintos (NO aditivo entre productos/clientes)';

ALTER TABLE fmcg_lakehouse.gold.agg_sales_monthly ALTER COLUMN `total_qty` COMMENT 'suma de order_qty';

ALTER TABLE fmcg_lakehouse.gold.agg_sales_monthly ALTER COLUMN `revenue` COMMENT 'suma de line_amount (lineas sin precio no suman)';

ALTER TABLE fmcg_lakehouse.gold.agg_sales_monthly ALTER COLUMN `lines_without_price` COMMENT 'lineas con price_missing';

ALTER TABLE fmcg_lakehouse.gold.agg_sales_monthly ALTER COLUMN `_silver_updated_at` COMMENT 'ultima escritura de la fila origen en Silver';

ALTER TABLE fmcg_lakehouse.gold.agg_sales_monthly ALTER COLUMN `_gold_loaded_at` COMMENT 'momento de la recarga de Gold';

COMMENT ON TABLE fmcg_lakehouse.gold.feature_sales_monthly IS 'Features ML mes x cliente x producto con lags de calendario y etiqueta del mes siguiente. Clave unica: month_start, company, customer_sk, product_sk, currency. Recarga completa (INSERT OVERWRITE). Responde: cuanto comprara un cliente de un producto el mes siguiente (etiqueta de entrenamiento). Consumidor: ML';

ALTER TABLE fmcg_lakehouse.gold.feature_sales_monthly ALTER COLUMN `month_start` COMMENT 'mes de la observacion';

ALTER TABLE fmcg_lakehouse.gold.feature_sales_monthly ALTER COLUMN `company` COMMENT 'madre | hija';

ALTER TABLE fmcg_lakehouse.gold.feature_sales_monthly ALTER COLUMN `customer_sk` COMMENT 'dim_customer';

ALTER TABLE fmcg_lakehouse.gold.feature_sales_monthly ALTER COLUMN `product_sk` COMMENT 'dim_product';

ALTER TABLE fmcg_lakehouse.gold.feature_sales_monthly ALTER COLUMN `currency` COMMENT 'moneda de los montos';

ALTER TABLE fmcg_lakehouse.gold.feature_sales_monthly ALTER COLUMN `revenue` COMMENT 'ingreso del mes';

ALTER TABLE fmcg_lakehouse.gold.feature_sales_monthly ALTER COLUMN `total_qty` COMMENT 'cantidad del mes';

ALTER TABLE fmcg_lakehouse.gold.feature_sales_monthly ALTER COLUMN `order_lines` COMMENT 'lineas del mes';

ALTER TABLE fmcg_lakehouse.gold.feature_sales_monthly ALTER COLUMN `revenue_lag_1` COMMENT 'ingreso de hace 1 mes calendario (0 si no compro)';

ALTER TABLE fmcg_lakehouse.gold.feature_sales_monthly ALTER COLUMN `revenue_lag_2` COMMENT 'hace 2 meses';

ALTER TABLE fmcg_lakehouse.gold.feature_sales_monthly ALTER COLUMN `revenue_lag_3` COMMENT 'hace 3 meses';

ALTER TABLE fmcg_lakehouse.gold.feature_sales_monthly ALTER COLUMN `qty_lag_1` COMMENT 'cantidad de hace 1 mes (0 si no compro)';

ALTER TABLE fmcg_lakehouse.gold.feature_sales_monthly ALTER COLUMN `qty_lag_2` COMMENT 'hace 2 meses';

ALTER TABLE fmcg_lakehouse.gold.feature_sales_monthly ALTER COLUMN `qty_lag_3` COMMENT 'hace 3 meses';

ALTER TABLE fmcg_lakehouse.gold.feature_sales_monthly ALTER COLUMN `revenue_avg_3m` COMMENT 'promedio de los 3 meses previos (sin el actual)';

ALTER TABLE fmcg_lakehouse.gold.feature_sales_monthly ALTER COLUMN `months_since_prev_purchase` COMMENT 'meses desde la compra anterior (NULL si es la primera)';

ALTER TABLE fmcg_lakehouse.gold.feature_sales_monthly ALTER COLUMN `purchase_month_seq` COMMENT 'n-esimo mes con compras del par cliente-producto';

ALTER TABLE fmcg_lakehouse.gold.feature_sales_monthly ALTER COLUMN `target_revenue_next_month` COMMENT 'ETIQUETA: ingreso del mes siguiente; NULL si aun no se conoce';

ALTER TABLE fmcg_lakehouse.gold.feature_sales_monthly ALTER COLUMN `_silver_updated_at` COMMENT 'ultima escritura de la fila origen en Silver';

ALTER TABLE fmcg_lakehouse.gold.feature_sales_monthly ALTER COLUMN `_gold_loaded_at` COMMENT 'momento de la recarga de Gold';

COMMENT ON TABLE fmcg_lakehouse.ops.gold_runs IS 'Una fila por tarea de Gold: filas escritas y reconciliaciones. Responde: que escribio cada tarea de Gold y si pasaron sus reconciliaciones. Consumidor: operacion';

ALTER TABLE fmcg_lakehouse.ops.gold_runs ALTER COLUMN `run_id` COMMENT 'id de la corrida de Gold';

ALTER TABLE fmcg_lakehouse.ops.gold_runs ALTER COLUMN `task` COMMENT 'tabla/tarea';

ALTER TABLE fmcg_lakehouse.ops.gold_runs ALTER COLUMN `target_table` COMMENT 'tabla escrita';

ALTER TABLE fmcg_lakehouse.ops.gold_runs ALTER COLUMN `rows_written` COMMENT 'filas tras el INSERT OVERWRITE';

ALTER TABLE fmcg_lakehouse.ops.gold_runs ALTER COLUMN `checks` COMMENT 'reconciliaciones hechas (texto)';

ALTER TABLE fmcg_lakehouse.ops.gold_runs ALTER COLUMN `started_at` COMMENT 'inicio de la tarea (UTC)';

ALTER TABLE fmcg_lakehouse.ops.gold_runs ALTER COLUMN `finished_at` COMMENT 'fin de la tarea (UTC)';

ALTER TABLE fmcg_lakehouse.ops.gold_runs ALTER COLUMN `status` COMMENT 'ok | failed';

ALTER TABLE fmcg_lakehouse.ops.gold_runs ALTER COLUMN `error` COMMENT 'mensaje de error si status = failed (NULL si ok)';

CREATE OR REPLACE VIEW fmcg_lakehouse.gold.v_kpi_sales_monthly (
  `year` COMMENT 'anio calendario',
  `quarter` COMMENT 'trimestre 1..4',
  `month` COMMENT 'mes 1..12',
  `month_start` COMMENT 'primer dia del mes',
  `year_month` COMMENT 'YYYY-MM',
  `company` COMMENT 'madre | hija',
  `currency` COMMENT 'moneda de los montos (de Silver)',
  `revenue` COMMENT 'ingresos = SUM(line_amount); lineas sin precio no suman',
  `total_qty` COMMENT 'cantidad vendida = SUM(order_qty)',
  `orders` COMMENT 'pedidos distintos del mes y compania',
  `order_lines` COMMENT 'lineas de pedido',
  `lines_without_price` COMMENT 'lineas con price_missing (no suman ingresos)',
  `avg_ticket` COMMENT 'ticket promedio = revenue / orders'
)
COMMENT 'KPI: ingresos, cantidad, pedidos y ticket por mes y compania. Responde: como evolucionan ingresos, cantidad, pedidos y ticket promedio mes a mes por compania. Consumidor: BI, AI'
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

CREATE OR REPLACE VIEW fmcg_lakehouse.gold.v_kpi_top10_products (
  `period` COMMENT 'anio (yyyy) o ''Total'' para todo el periodo',
  `revenue_rank` COMMENT 'puesto por ingresos dentro de period y currency (RANK: empates comparten puesto)',
  `company` COMMENT 'madre | hija',
  `product_name` COMMENT 'nombre completo del producto',
  `product` COMMENT 'nombre sin variante',
  `variant` COMMENT 'variante (texto entre parentesis)',
  `category` COMMENT 'categoria canonica',
  `division` COMMENT 'division por categoria o ''Unknown''',
  `currency` COMMENT 'moneda de revenue',
  `revenue` COMMENT 'ingresos del producto en el periodo',
  `total_qty` COMMENT 'cantidad vendida del producto en el periodo',
  `product_sk` COMMENT 'clave de dim_product'
)
COMMENT 'KPI: top 10 productos por ingreso, por anio y Total (RANK, empates comparten puesto). Responde: que 10 productos generan mas ingresos por anio y en todo el periodo. Consumidor: BI, AI'
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

CREATE OR REPLACE VIEW fmcg_lakehouse.gold.v_kpi_category_mix (
  `year` COMMENT 'anio de month_start',
  `month_start` COMMENT 'mes (primer dia)',
  `company` COMMENT 'madre | hija',
  `division` COMMENT 'division por categoria o ''Unknown''',
  `category` COMMENT 'categoria canonica',
  `currency` COMMENT 'moneda de revenue',
  `revenue` COMMENT 'ingresos de la categoria en el mes y compania',
  `total_qty` COMMENT 'cantidad vendida de la categoria en el mes y compania',
  `revenue_share` COMMENT 'participacion de la categoria en los ingresos del mes de la compania (0..1)'
)
COMMENT 'KPI: mix por division/categoria y participacion en el mes por compania. Responde: que peso tiene cada division y categoria en los ingresos del mes de cada compania. Consumidor: BI, AI'
AS
SELECT a.year, a.month_start, a.company, p.division, p.category, a.currency,
  SUM(a.revenue) AS revenue,
  SUM(a.total_qty) AS total_qty,
  SUM(a.revenue) / NULLIF(SUM(SUM(a.revenue)) OVER (PARTITION BY a.month_start, a.company, a.currency), 0)
    AS revenue_share
FROM fmcg_lakehouse.gold.agg_sales_monthly a
JOIN fmcg_lakehouse.gold.dim_product p ON p.product_sk = a.product_sk
GROUP BY a.year, a.month_start, a.company, p.division, p.category, a.currency;

CREATE OR REPLACE VIEW fmcg_lakehouse.gold.v_kpi_company_comparison (
  `year` COMMENT 'anio de month_start',
  `month_start` COMMENT 'mes (primer dia)',
  `company` COMMENT 'madre | hija',
  `currency` COMMENT 'moneda de los montos',
  `revenue` COMMENT 'ingresos del mes de la compania',
  `total_qty` COMMENT 'cantidad vendida del mes',
  `orders` COMMENT 'pedidos distintos del mes',
  `active_customers` COMMENT 'clientes distintos con compra en el mes (sin el miembro -1)',
  `lines_unknown_customer` COMMENT 'lineas con cliente desconocido (-1); cuentan en ingresos',
  `avg_ticket` COMMENT 'ticket promedio = revenue / orders',
  `revenue_share_month` COMMENT 'participacion de la compania en los ingresos del mes (0..1)',
  `revenue_mom_pct` COMMENT 'variacion mensual de ingresos vs el mes anterior de la misma compania (0,05 = +5%; NULL en el primer mes)'
)
COMMENT 'KPI: comparativo madre vs hija por mes (participacion y variacion mensual). Responde: como se compara la hija con la madre: participacion, variacion mensual y clientes activos. Consumidor: BI, AI'
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

CREATE OR REPLACE VIEW fmcg_lakehouse.gold.v_kpi_data_quality (
  `entity` COMMENT 'customers | products | prices | orders',
  `company` COMMENT 'madre | hija',
  `rule_id` COMMENT 'id de la regla (silver/rules.py)',
  `description` COMMENT 'que valida la regla',
  `severity` COMMENT 'severidad de la regla',
  `action` COMMENT 'corregir | marcar | cuarentena',
  `rows_evaluated` COMMENT 'poblacion de la etapa donde se evalua la regla',
  `rows_failed` COMMENT 'filas que dispararon la regla',
  `fail_rate` COMMENT 'rows_failed / rows_evaluated (0..1)',
  `mode` COMMENT 'incremental | full',
  `batch_id` COMMENT 'id de la corrida de Silver',
  `ts` COMMENT 'momento de la corrida'
)
COMMENT 'Calidad: ultima corrida de Silver por entidad (ops.dq_results) con descripcion de la regla. Responde: que reglas de calidad fallaron en la ultima corrida de Silver y con que tasa. Consumidor: BI, AI'
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

CREATE OR REPLACE VIEW fmcg_lakehouse.gold.v_kpi_quarantine_summary (
  `entity` COMMENT 'customers | products | prices | orders',
  `company` COMMENT 'madre | hija',
  `rule_id` COMMENT 'regla que mando la fila a cuarentena',
  `rows_quarantined` COMMENT 'filas en cuarentena',
  `last_quarantined_at` COMMENT 'ultima vez que se puso una fila en cuarentena por esa regla'
)
COMMENT 'Calidad: filas en cuarentena por entidad, compania y regla. Responde: cuantas filas quedaron en cuarentena por entidad, compania y regla. Consumidor: BI, AI'
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
