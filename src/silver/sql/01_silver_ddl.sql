-- NO EJECUTADO — requiere confirmacion de Manu (crea tablas en Unity Catalog y siembra datos).
-- GENERADO desde src/silver/ddl.py; no editar a mano:
--   cd src && python -m silver.ddl fmcg_lakehouse > silver/sql/01_silver_ddl.sql
-- Requiere los esquemas silver, quarantine y ops (infra/02-unity-catalog.sql). Todo es idempotente.
-- Orden: tablas Silver, cuarentena, ops.dq_results, ref_*, siembra de ref_* y miembros -1.

CREATE TABLE IF NOT EXISTS fmcg_lakehouse.silver.customers (
  `company` STRING COMMENT 'madre | hija',
  `customer_id` STRING COMMENT 'clave natural; ''-1'' = miembro desconocido',
  `customer_key` STRING COMMENT 'clave de negocio conformada company:customer_id',
  `customer_name` STRING COMMENT 'trim + colapso de espacios + initcap',
  `city` STRING COMMENT 'ciudad canonica (ref_city_mapping) o ''Unknown''',
  `customer_label` STRING COMMENT 'nombre-ciudad (como la referencia)',
  `market` STRING COMMENT 'de ref_company',
  `channel` STRING COMMENT 'de ref_company',
  `name_was_normalized` BOOLEAN COMMENT 'CUS_03/CUS_04',
  `city_was_corrected` BOOLEAN COMMENT 'CUS_05',
  `city_was_null` BOOLEAN COMMENT 'CUS_06',
  `city_is_unmapped` BOOLEAN COMMENT 'CUS_07',
  `is_unknown_member` BOOLEAN COMMENT 'fila del miembro desconocido',
  `_source_file` STRING COMMENT 'archivo de landing del que viene la fila ganadora',
  `_batch_id` STRING COMMENT 'lote de Bronze',
  `_ingest_ts` TIMESTAMP COMMENT 'momento de ingesta en Bronze; decide el dedup',
  `_silver_updated_at` TIMESTAMP COMMENT 'ultima escritura en Silver'
)
COMMENT 'Silver: clave de negocio = company, customer_id (sin PK fisica; la garantiza el MERGE)'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS fmcg_lakehouse.silver.products (
  `company` STRING COMMENT 'madre | hija',
  `product_id` STRING COMMENT 'clave natural numerica sin prefijo; ''-1'' = desconocido',
  `product_key` STRING COMMENT 'clave de negocio conformada company:product_id',
  `product_id_raw` STRING COMMENT 'id tal cual llego de Bronze',
  `product_id_was_prefixed` BOOLEAN COMMENT 'PRD_02',
  `product_code` STRING COMMENT 'sha2(product_name, 256)',
  `product_name` STRING COMMENT 'nombre completo corregido (typos)',
  `product` STRING COMMENT 'nombre sin la variante entre parentesis',
  `variant` STRING COMMENT 'texto entre parentesis',
  `category` STRING COMMENT 'categoria canonica (ref_category_division)',
  `division` STRING COMMENT 'division por categoria o ''Unknown''',
  `name_was_corrected` BOOLEAN COMMENT 'PRD_04',
  `category_was_normalized` BOOLEAN COMMENT 'PRD_05',
  `division_was_unknown` BOOLEAN COMMENT 'PRD_06',
  `is_unknown_member` BOOLEAN COMMENT 'fila del miembro desconocido',
  `_source_file` STRING COMMENT 'archivo de landing del que viene la fila ganadora',
  `_batch_id` STRING COMMENT 'lote de Bronze',
  `_ingest_ts` TIMESTAMP COMMENT 'momento de ingesta en Bronze; decide el dedup',
  `_silver_updated_at` TIMESTAMP COMMENT 'ultima escritura en Silver'
)
COMMENT 'Silver: clave de negocio = company, product_id (sin PK fisica; la garantiza el MERGE)'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

CREATE TABLE IF NOT EXISTS fmcg_lakehouse.silver.prices (
  `company` STRING COMMENT 'madre | hija',
  `product_id` STRING COMMENT 'clave natural del producto',
  `product_key` STRING COMMENT 'company:product_id',
  `product_code` STRING COMMENT 'de silver.products (NULL si no resuelve)',
  `price_month` DATE COMMENT 'primer dia del mes de vigencia',
  `unit_price` DECIMAL(12,2) COMMENT 'precio unitario > 0 (negativos en valor absoluto)',
  `currency` STRING COMMENT 'moneda estandarizada',
  `price_was_negative` BOOLEAN COMMENT 'PRC_04',
  `price_was_invalid` BOOLEAN COMMENT 'PRC_05 si la accion fuera ''marcar'' (precio 0)',
  `month_was_reformatted` BOOLEAN COMMENT 'PRC_03',
  `product_unresolved` BOOLEAN COMMENT 'PRC_07',
  `_source_file` STRING COMMENT 'archivo de landing del que viene la fila ganadora',
  `_batch_id` STRING COMMENT 'lote de Bronze',
  `_ingest_ts` TIMESTAMP COMMENT 'momento de ingesta en Bronze; decide el dedup',
  `_silver_updated_at` TIMESTAMP COMMENT 'ultima escritura en Silver'
)
COMMENT 'Silver: clave de negocio = company, product_id, price_month (sin PK fisica; la garantiza el MERGE)'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

CREATE TABLE IF NOT EXISTS fmcg_lakehouse.silver.orders (
  `company` STRING COMMENT 'madre | hija',
  `order_id` STRING COMMENT 'clave natural del pedido',
  `line_no` INT COMMENT 'numero de linea dentro del pedido',
  `order_date` DATE COMMENT 'fecha parseada de 5 formatos',
  `order_year` INT COMMENT 'parte de fecha',
  `order_quarter` INT COMMENT 'parte de fecha',
  `order_month` INT COMMENT 'parte de fecha',
  `order_month_start` DATE COMMENT 'primer dia del mes (une con prices.price_month)',
  `is_weekend` BOOLEAN COMMENT 'sabado o domingo',
  `customer_id` STRING COMMENT '''-1'' si invalido o no resuelto',
  `product_id` STRING COMMENT '''-1'' si no resuelto',
  `order_qty` INT COMMENT 'entero > 0',
  `unit_price` DECIMAL(12,2) COMMENT 'precio vigente del mes (NULL si no hay)',
  `line_amount` DECIMAL(18,2) COMMENT 'order_qty * unit_price; NULL + price_missing si no hay precio',
  `currency` STRING COMMENT 'moneda estandarizada',
  `date_was_reformatted` BOOLEAN COMMENT 'ORD_06',
  `customer_id_was_invalid` BOOLEAN COMMENT 'ORD_07',
  `customer_unresolved` BOOLEAN COMMENT 'ORD_08',
  `product_unresolved` BOOLEAN COMMENT 'ORD_09',
  `price_missing` BOOLEAN COMMENT 'ORD_10',
  `_source_file` STRING COMMENT 'archivo de landing del que viene la fila ganadora',
  `_batch_id` STRING COMMENT 'lote de Bronze',
  `_ingest_ts` TIMESTAMP COMMENT 'momento de ingesta en Bronze; decide el dedup',
  `_silver_updated_at` TIMESTAMP COMMENT 'ultima escritura en Silver'
)
COMMENT 'Silver: clave de negocio = company, order_id, line_no (sin PK fisica; la garantiza el MERGE)'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

CREATE TABLE IF NOT EXISTS fmcg_lakehouse.quarantine.customers (
  `customer_id` STRING,
  `customer_name` STRING,
  `city` STRING,
  `_company` STRING,
  `_source_file` STRING,
  `_ingest_ts` TIMESTAMP,
  `_batch_id` STRING,
  `rule_id` STRING,
  `reason` STRING,
  `quarantined_at` TIMESTAMP,
  `silver_run_id` STRING
)
COMMENT 'Filas rechazadas de Silver: fila cruda de Bronze + rule_id + reason + _batch_id (1 fila por fila rechazada)';

CREATE TABLE IF NOT EXISTS fmcg_lakehouse.quarantine.products (
  `product_id` STRING,
  `product_name` STRING,
  `category` STRING,
  `_company` STRING,
  `_source_file` STRING,
  `_ingest_ts` TIMESTAMP,
  `_batch_id` STRING,
  `rule_id` STRING,
  `reason` STRING,
  `quarantined_at` TIMESTAMP,
  `silver_run_id` STRING
)
COMMENT 'Filas rechazadas de Silver: fila cruda de Bronze + rule_id + reason + _batch_id (1 fila por fila rechazada)';

CREATE TABLE IF NOT EXISTS fmcg_lakehouse.quarantine.prices (
  `product_id` STRING,
  `price_month` STRING,
  `unit_price` STRING,
  `_company` STRING,
  `_source_file` STRING,
  `_ingest_ts` TIMESTAMP,
  `_batch_id` STRING,
  `rule_id` STRING,
  `reason` STRING,
  `quarantined_at` TIMESTAMP,
  `silver_run_id` STRING
)
COMMENT 'Filas rechazadas de Silver: fila cruda de Bronze + rule_id + reason + _batch_id (1 fila por fila rechazada)';

CREATE TABLE IF NOT EXISTS fmcg_lakehouse.quarantine.orders (
  `order_id` STRING,
  `line_no` STRING,
  `order_date` STRING,
  `customer_id` STRING,
  `product_id` STRING,
  `order_qty` STRING,
  `_company` STRING,
  `_source_file` STRING,
  `_ingest_ts` TIMESTAMP,
  `_batch_id` STRING,
  `rule_id` STRING,
  `reason` STRING,
  `quarantined_at` TIMESTAMP,
  `silver_run_id` STRING
)
COMMENT 'Filas rechazadas de Silver: fila cruda de Bronze + rule_id + reason + _batch_id (1 fila por fila rechazada)';

CREATE TABLE IF NOT EXISTS fmcg_lakehouse.ops.dq_results (
  `batch_id` STRING COMMENT 'id de la corrida de Silver (no el de Bronze)',
  `entity` STRING COMMENT 'customers | products | prices | orders',
  `company` STRING COMMENT 'madre | hija',
  `rule_id` STRING COMMENT 'ver silver/rules.py',
  `rows_evaluated` BIGINT COMMENT 'poblacion de la etapa donde se evalua la regla',
  `rows_failed` BIGINT COMMENT 'filas que dispararon la regla',
  `action` STRING COMMENT 'corregir | marcar | cuarentena',
  `mode` STRING COMMENT 'incremental | full',
  `ts` TIMESTAMP COMMENT 'momento de la corrida'
)
COMMENT 'Resultados de calidad por corrida, entidad, compania y regla';

CREATE TABLE IF NOT EXISTS fmcg_lakehouse.silver.ref_city_mapping (
  raw_city_key STRING COMMENT 'lower(trim(city)) como llega',
  city STRING COMMENT 'ciudad canonica',
  updated_at TIMESTAMP);

CREATE TABLE IF NOT EXISTS fmcg_lakehouse.silver.ref_category_division (
  category_key STRING COMMENT 'lower(trim(category))',
  category STRING COMMENT 'categoria canonica',
  division STRING, updated_at TIMESTAMP);

CREATE TABLE IF NOT EXISTS fmcg_lakehouse.silver.ref_typo_fixes (
  entity STRING, column_name STRING, wrong_term STRING, correct_term STRING,
  updated_at TIMESTAMP);

CREATE TABLE IF NOT EXISTS fmcg_lakehouse.silver.ref_company (
  company STRING, source_system STRING, market STRING, channel STRING,
  updated_at TIMESTAMP);

MERGE INTO fmcg_lakehouse.silver.ref_city_mapping AS t
USING (SELECT * FROM VALUES
  ('arequipa', 'Arequipa'),
  ('arequipaa', 'Arequipa'),
  ('arequpa', 'Arequipa'),
  ('chiclallo', 'Chiclayo'),
  ('chiclayo', 'Chiclayo'),
  ('chiclayoo', 'Chiclayo'),
  ('cuscco', 'Cusco'),
  ('cusco', 'Cusco'),
  ('cuzco', 'Cusco'),
  ('lima', 'Lima'),
  ('limaa', 'Lima'),
  ('lmia', 'Lima'),
  ('pirua', 'Piura'),
  ('piura', 'Piura'),
  ('trujillio', 'Trujillo'),
  ('trujillo', 'Trujillo'),
  ('trujilo', 'Trujillo')
  AS v(raw_city_key, city)) AS s
ON t.raw_city_key = s.raw_city_key
WHEN NOT MATCHED THEN INSERT (raw_city_key, city, updated_at) VALUES (s.raw_city_key, s.city, current_timestamp());

MERGE INTO fmcg_lakehouse.silver.ref_category_division AS t
USING (SELECT * FROM VALUES
  ('barras proteicas', 'Barras Proteicas', 'Nutricion Deportiva'),
  ('bebidas', 'Bebidas', 'Bebidas'),
  ('bebidas energizantes', 'Bebidas Energizantes', 'Bebidas'),
  ('bebidas isotonicas', 'Bebidas Isotonicas', 'Bebidas'),
  ('cereales y granola', 'Cereales y Granola', 'Alimentos'),
  ('cuidado del hogar', 'Cuidado del Hogar', 'Hogar y Cuidado Personal'),
  ('cuidado personal', 'Cuidado Personal', 'Hogar y Cuidado Personal'),
  ('galletas', 'Galletas', 'Alimentos'),
  ('lacteos', 'Lacteos', 'Alimentos'),
  ('snacks salados', 'Snacks Salados', 'Alimentos'),
  ('snacks saludables', 'Snacks Saludables', 'Alimentos')
  AS v(category_key, category, division)) AS s
ON t.category_key = s.category_key
WHEN NOT MATCHED THEN INSERT (category_key, category, division, updated_at) VALUES (s.category_key, s.category, s.division, current_timestamp());

MERGE INTO fmcg_lakehouse.silver.ref_typo_fixes AS t
USING (SELECT * FROM VALUES
  ('products', 'product_name', 'Energisante', 'Energizante')
  AS v(entity, column_name, wrong_term, correct_term)) AS s
ON t.entity = s.entity AND t.column_name = s.column_name AND t.wrong_term = s.wrong_term
WHEN NOT MATCHED THEN INSERT (entity, column_name, wrong_term, correct_term, updated_at) VALUES (s.entity, s.column_name, s.wrong_term, s.correct_term, current_timestamp());

MERGE INTO fmcg_lakehouse.silver.ref_company AS t
USING (SELECT * FROM VALUES
  ('madre', 'madre_erp', 'Peru', 'Retail tradicional'),
  ('hija', 'hija_export', 'Peru', 'Bares deportivos')
  AS v(company, source_system, market, channel)) AS s
ON t.company = s.company
WHEN NOT MATCHED THEN INSERT (company, source_system, market, channel, updated_at) VALUES (s.company, s.source_system, s.market, s.channel, current_timestamp());

MERGE INTO fmcg_lakehouse.silver.customers AS t
USING (
  SELECT
    c.company AS company,
    '-1' AS customer_id,
    concat(c.company, ':-1') AS customer_key,
    'Unknown' AS customer_name,
    'Unknown' AS city,
    'Unknown' AS customer_label,
    'Unknown' AS market,
    'Unknown' AS channel,
    false AS name_was_normalized,
    false AS city_was_corrected,
    false AS city_was_null,
    false AS city_is_unmapped,
    true AS is_unknown_member,
    'seed' AS _source_file,
    'seed' AS _batch_id,
    current_timestamp() AS _ingest_ts,
    current_timestamp() AS _silver_updated_at
  FROM (VALUES ('madre'), ('hija')) AS c(company)
) AS s
ON t.company = s.company AND t.customer_id = s.customer_id
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO fmcg_lakehouse.silver.products AS t
USING (
  SELECT
    c.company AS company,
    '-1' AS product_id,
    concat(c.company, ':-1') AS product_key,
    '-1' AS product_id_raw,
    false AS product_id_was_prefixed,
    CAST(NULL AS STRING) AS product_code,
    'Unknown' AS product_name,
    'Unknown' AS product,
    CAST(NULL AS STRING) AS variant,
    'Unknown' AS category,
    'Unknown' AS division,
    false AS name_was_corrected,
    false AS category_was_normalized,
    false AS division_was_unknown,
    true AS is_unknown_member,
    'seed' AS _source_file,
    'seed' AS _batch_id,
    current_timestamp() AS _ingest_ts,
    current_timestamp() AS _silver_updated_at
  FROM (VALUES ('madre'), ('hija')) AS c(company)
) AS s
ON t.company = s.company AND t.product_id = s.product_id
WHEN NOT MATCHED THEN INSERT *;
