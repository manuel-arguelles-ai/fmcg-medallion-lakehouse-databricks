-- =====================================================================================
-- NO EJECUTADO — requiere confirmacion de Manu.
-- Unity Catalog de la solucion FMCG: external locations, catalogo, esquemas, volumes y tablas ops.
-- Ejecutar paso a paso en un SQL warehouse / notebook como metastore admin (o con los privilegios
-- indicados). Cada bloque es idempotente (IF NOT EXISTS).
-- Metastore REGIONAL COMPARTIDO: solo se crean objetos con prefijo propio (loc_fmcg_*, cred_fmcg_*,
-- fmcg_lakehouse). No tocar los catalogos fmcg, fmcg_dev, pede_dev ni sus locations.
-- Placeholders: <ACCESS_CONNECTOR_RESOURCE_ID> (id completo del Access Connector, ver paso 0).
-- =====================================================================================

-- ---- 0) Storage credential (NO se puede crear por SQL: CLI o UI) -----------------------
-- Resource id del Access Connector (solo lectura):
--   az databricks access-connector show -g rg-fmcg-lakehouse-dev-cus-001 \
--      -n ac-fmcg-lakehouse-dev-cus-001 --query id -o tsv
-- Crear la credencial (requiere CREATE_STORAGE_CREDENTIAL en el metastore):
--   export DATABRICKS_HOST=https://<workspace-host> DATABRICKS_AUTH_TYPE=azure-cli
--   databricks storage-credentials create cred_fmcg_lakehouse --json '{
--     "azure_managed_identity": {"access_connector_id": "<ACCESS_CONNECTOR_RESOURCE_ID>"},
--     "comment": "Access Connector ac-fmcg-lakehouse-dev-cus-001 (solucion FMCG)"}'
-- Verificar: databricks storage-credentials get cred_fmcg_lakehouse
-- El rol "Storage Blob Data Contributor" del connector sobre la cuenta ya esta asignado (01-provision).

-- ---- 1) External locations (una por contenedor) -----------------------------------------
-- Requiere CREATE EXTERNAL LOCATION en el metastore y CREATE EXTERNAL LOCATION sobre la credencial.
CREATE EXTERNAL LOCATION IF NOT EXISTS loc_fmcg_landing
  URL 'abfss://landing@stfmcglakehousedevcus001.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL cred_fmcg_lakehouse) COMMENT 'Landing de CSV crudos (madre/hija)';
CREATE EXTERNAL LOCATION IF NOT EXISTS loc_fmcg_bronze
  URL 'abfss://bronze@stfmcglakehousedevcus001.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL cred_fmcg_lakehouse) COMMENT 'Reservado Bronze (hoy las tablas son managed en uc-managed)';
CREATE EXTERNAL LOCATION IF NOT EXISTS loc_fmcg_silver
  URL 'abfss://silver@stfmcglakehousedevcus001.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL cred_fmcg_lakehouse) COMMENT 'Reservado Silver';
CREATE EXTERNAL LOCATION IF NOT EXISTS loc_fmcg_gold
  URL 'abfss://gold@stfmcglakehousedevcus001.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL cred_fmcg_lakehouse) COMMENT 'Reservado Gold';
CREATE EXTERNAL LOCATION IF NOT EXISTS loc_fmcg_uc_managed
  URL 'abfss://uc-managed@stfmcglakehousedevcus001.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL cred_fmcg_lakehouse) COMMENT 'Almacenamiento managed del catalogo fmcg_lakehouse';
-- Equivalente por CLI: databricks external-locations create loc_fmcg_landing \
--   'abfss://landing@stfmcglakehousedevcus001.dfs.core.windows.net/' cred_fmcg_lakehouse

-- ---- 2) Catalogo y esquemas ---------------------------------------------------------------
CREATE CATALOG IF NOT EXISTS fmcg_lakehouse
  MANAGED LOCATION 'abfss://uc-managed@stfmcglakehousedevcus001.dfs.core.windows.net/'
  COMMENT 'Lakehouse medallion FMCG (madre + hija)';
CREATE SCHEMA IF NOT EXISTS fmcg_lakehouse.bronze     COMMENT 'Crudo, append-only, todo STRING + metadatos';
CREATE SCHEMA IF NOT EXISTS fmcg_lakehouse.silver     COMMENT 'Limpio, tipado, deduplicado, conformado';
CREATE SCHEMA IF NOT EXISTS fmcg_lakehouse.gold       COMMENT 'Modelo dimensional y KPIs';
CREATE SCHEMA IF NOT EXISTS fmcg_lakehouse.quarantine COMMENT 'Filas rechazadas por calidad + motivo';
CREATE SCHEMA IF NOT EXISTS fmcg_lakehouse.ops        COMMENT 'Registro de fuentes, corridas, rechazos, calidad';

-- ---- 3) Volumes ---------------------------------------------------------------------------
-- Externo sobre el contenedor landing: de aqui lee Auto Loader (/Volumes/fmcg_lakehouse/bronze/landing/...)
-- Requiere CREATE EXTERNAL VOLUME sobre loc_fmcg_landing y que NO haya tablas/volumes bajo esa ruta.
CREATE EXTERNAL VOLUME IF NOT EXISTS fmcg_lakehouse.bronze.landing
  LOCATION 'abfss://landing@stfmcglakehousedevcus001.dfs.core.windows.net/'
  COMMENT 'CSV crudos: <compania>/<fuente>/ingest_date=YYYY-MM-DD/<archivo>.csv';
-- Managed: checkpoints y schema location de Auto Loader (uno por compania x fuente, bajo bronze/).
CREATE VOLUME IF NOT EXISTS fmcg_lakehouse.ops.checkpoints
  COMMENT 'Checkpoints de Auto Loader: bronze/<compania>_<fuente>/{stream,schema}';

-- ---- 4) Tablas ops (mismo DDL que src/bronze/registry.py:OPS_DDL; ingest.py tambien las crea) ----
CREATE TABLE IF NOT EXISTS fmcg_lakehouse.ops.source_registry (
  source_system STRING, company STRING, source STRING, format STRING, frequency STRING,
  landing_path STRING, expected_columns ARRAY<STRING>, load_type STRING,
  landing_retention_days INT, bronze_retention STRING, updated_at TIMESTAMP);
CREATE TABLE IF NOT EXISTS fmcg_lakehouse.ops.landing_rejects (
  rejected_at TIMESTAMP, batch_id STRING, company STRING, source STRING,
  file_path STRING, file_size BIGINT, rule_id STRING, reason STRING);
CREATE TABLE IF NOT EXISTS fmcg_lakehouse.ops.bronze_runs (
  batch_id STRING, company STRING, source STRING, target_table STRING, load_type STRING,
  started_at TIMESTAMP, finished_at TIMESTAMP, status STRING, files_new INT, files_rejected INT,
  rows_read BIGINT, rows_written BIGINT, rows_rescued BIGINT, rows_rejected BIGINT, error STRING);

-- Siembra de ops.source_registry: ver infra/02b-seed-source-registry.sql (generado desde registry.yaml).

-- ---- 5) Permisos (comentado: los grupos aun no existen) ---------------------------------------
-- GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG fmcg_lakehouse TO `analysts`;
-- GRANT ALL PRIVILEGES ON CATALOG fmcg_lakehouse TO `de_engineers`;
-- GRANT READ VOLUME ON VOLUME fmcg_lakehouse.bronze.landing TO `de_engineers`;

-- ---- 6) Verificacion (solo lectura) -----------------------------------------------------------
-- SHOW EXTERNAL LOCATIONS; SHOW SCHEMAS IN fmcg_lakehouse; SHOW VOLUMES IN fmcg_lakehouse.bronze;
-- LIST '/Volumes/fmcg_lakehouse/bronze/landing/';
