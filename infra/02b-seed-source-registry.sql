-- NO EJECUTADO — requiere confirmacion de Manu.
-- Siembra de ops.source_registry. GENERADO desde src/bronze/registry.yaml; no editar a mano:
--   cd src && python -m bronze.registry --seed fmcg_lakehouse > ../infra/02b-seed-source-registry.sql
-- (un test verifica que este archivo coincide con el registro). Ejecutar DESPUES de 02-unity-catalog.sql.
MERGE INTO fmcg_lakehouse.ops.source_registry AS t
USING (
  SELECT * FROM VALUES
  ('madre_erp', 'madre', 'customers', 'csv', 'daily', '/Volumes/fmcg_lakehouse/bronze/landing/madre/customers', array('customer_id', 'customer_name', 'city'), 'full', 90, 'full_history'),
  ('madre_erp', 'madre', 'products', 'csv', 'daily', '/Volumes/fmcg_lakehouse/bronze/landing/madre/products', array('product_id', 'product_name', 'category'), 'full', 90, 'full_history'),
  ('madre_erp', 'madre', 'prices', 'csv', 'monthly', '/Volumes/fmcg_lakehouse/bronze/landing/madre/prices', array('product_id', 'price_month', 'unit_price'), 'full', 90, 'full_history'),
  ('madre_erp', 'madre', 'orders', 'csv', 'daily', '/Volumes/fmcg_lakehouse/bronze/landing/madre/orders', array('order_id', 'line_no', 'order_date', 'customer_id', 'product_id', 'order_qty'), 'incremental', 90, 'full_history'),
  ('hija_export', 'hija', 'customers', 'csv', 'daily', '/Volumes/fmcg_lakehouse/bronze/landing/hija/customers', array('customer_id', 'customer_name', 'city'), 'full', 90, 'full_history'),
  ('hija_export', 'hija', 'products', 'csv', 'daily', '/Volumes/fmcg_lakehouse/bronze/landing/hija/products', array('product_id', 'product_name', 'category'), 'full', 90, 'full_history'),
  ('hija_export', 'hija', 'prices', 'csv', 'monthly', '/Volumes/fmcg_lakehouse/bronze/landing/hija/prices', array('product_id', 'price_month', 'unit_price'), 'full', 90, 'full_history'),
  ('hija_export', 'hija', 'orders', 'csv', 'daily', '/Volumes/fmcg_lakehouse/bronze/landing/hija/orders', array('order_id', 'line_no', 'order_date', 'customer_id', 'product_id', 'order_qty'), 'incremental', 90, 'full_history')
  AS v(source_system, company, source, format, frequency, landing_path, expected_columns, load_type, landing_retention_days, bronze_retention)
) AS s
ON t.company = s.company AND t.source = s.source
WHEN MATCHED THEN UPDATE SET t.source_system = s.source_system, t.format = s.format, t.frequency = s.frequency, t.landing_path = s.landing_path, t.expected_columns = s.expected_columns, t.load_type = s.load_type, t.landing_retention_days = s.landing_retention_days, t.bronze_retention = s.bronze_retention, t.updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (source_system, company, source, format, frequency, landing_path, expected_columns, load_type, landing_retention_days, bronze_retention, updated_at) VALUES (s.source_system, s.company, s.source, s.format, s.frequency, s.landing_path, s.expected_columns, s.load_type, s.landing_retention_days, s.bronze_retention, current_timestamp());
