# Criterios de aceptación por capa

Propósito: lista cerrada de criterios **verificables por un juez de solo lectura** para decidir si la solución está terminada. Cada criterio se comprueba leyendo archivos del repo (o `git ls-files` / `git log` / `grep`) o el resultado ya documentado en `docs/ESTADO.md` y `docs/05`, `docs/08`, `docs/09`. Ningún criterio exige ejecutar nada contra Azure ni Databricks: si algo solo se puede probar en el workspace, la evidencia aceptada es el resultado **documentado** (conteo, corrida, captura).

Fuentes usadas:
- **referencia**: lo que pide o hace `takadlag/databricks-medallion-lakehouse-fmcg` @ `5e99b2b` (README, notebooks y `docs/Dashboard.png`), resumido en `docs/00-analisis-referencia.md`; incluye las brechas y los errores de la referencia que la solución debe corregir.
- **experto**: aportes de la sala de data engineers (`docs/04` para Bronze, `docs/07` para Silver y, para Gold, las 5 respuestas de la sala: Gold es la capa final de consumo con los datos más limpios tras procesamientos, joins y configuraciones incrementales; contiene las estructuras que consumen BI, ML y AI para reducir transformaciones pesadas a futuro; define casos de negocio; son KPIs y tablas agregadas de negocio (hechos y dimensiones) que consumen los dashboards; cada tabla resuelve una pregunta del negocio. Lo nuevo frente al proyecto: preparar Gold también para ML/AI, no solo para dashboards).
- **decisión Dxx**: registro de `docs/02-decisiones.md`. D1–D8 y D26–D29 son decisiones tomadas. **D9–D25 (y G1–G9 de `docs/09` §2) son propuestas del agente aún no revisadas por los expertos**: los criterios que dependen de ellas llevan la marca **(propuesta)**.
- **buena práctica**: criterio de ingeniería del diseño (`docs/03-diseno-solucion.md`) o estándar del oficio.

Moneda: la solución queda en **PEN** (D26: se revirtió el cambio a USD). Ningún criterio pide USD ni INR.

Columnas: **ID** · **Criterio** (atómico y medible) · **P** = peso (1 deseable, 2 importante, 3 imprescindible) · **Fuente** · **Evidencia** (ruta + qué buscar).

---

## 1. Infraestructura y Unity Catalog (INF)

| ID | Criterio | P | Fuente | Evidencia |
|---|---|---|---|---|
| INF-01 | La infraestructura Azure propia (resource group, ADLS Gen2, Access Connector, workspace Databricks **premium**) está registrada como script en el repo, sin reutilizar recursos previos | 3 | decisión D5 | `infra/01-provision-azure.sh`: `az group create`, `az storage account create`, `az databricks access-connector create`, `az databricks workspace create ... --sku premium` con nombres `*-fmcg-lakehouse-dev-cus-001` |
| INF-02 | La cuenta de storage tiene HNS, TLS 1.2 mínimo y acceso público a blobs deshabilitado | 2 | experto (seguridad, `docs/04`) | `infra/01-provision-azure.sh`: `--hns true --min-tls-version TLS1_2 --allow-blob-public-access false` |
| INF-03 | El workspace se crea sin NAT gateway (nodos con IP pública) y se verificó que no hay NAT | 2 | decisión D6 | `infra/01-provision-azure.sh`: `--enable-no-public-ip false`; `docs/ESTADO.md` fila H0: "0 NAT gateways" |
| INF-04 | Todos los recursos se crean en `centralus` | 1 | decisión D7 | `infra/01-provision-azure.sh`: variable de región = `centralus` usada en cada `-l` |
| INF-05 | El Access Connector tiene el rol *Storage Blob Data Contributor* con alcance limitado a la cuenta de storage (no a la suscripción ni al RG) | 2 | buena práctica | `infra/01-provision-azure.sh`: `az role assignment create ... --role "Storage Blob Data Contributor" --scope "$(az storage account show ...)"` |
| INF-06 | Existe una storage credential UC basada en el Access Connector y 5 external locations (landing, bronze, silver, gold, uc-managed) | 2 | buena práctica (`docs/03` §3) | `infra/02-unity-catalog.sql`: comando de la credencial `cred_fmcg_lakehouse` y 5 `CREATE EXTERNAL LOCATION IF NOT EXISTS loc_fmcg_*` |
| INF-07 | El catálogo de la solución es `fmcg_lakehouse` con managed location en el contenedor `uc-managed` | 3 | decisión D8 | `infra/02-unity-catalog.sql`: `CREATE CATALOG IF NOT EXISTS fmcg_lakehouse MANAGED LOCATION 'abfss://uc-managed@...'` |
| INF-08 | Ningún script, job ni código referencia los catálogos ajenos del metastore compartido (`fmcg`, `fmcg_dev`, `pede_dev`) | 3 | decisión D8 / buena práctica | `grep -rnE "\b(fmcg|fmcg_dev|pede_dev)\.[a-z]" src infra resources databricks.yml` sin resultados |
| INF-09 | Existen los 5 esquemas `bronze`, `silver`, `gold`, `quarantine`, `ops`, cada uno con `COMMENT` de su propósito | 2 | buena práctica (`docs/03` §3) | `infra/02-unity-catalog.sql`: 5 `CREATE SCHEMA IF NOT EXISTS fmcg_lakehouse.<esquema> COMMENT '...'` |
| INF-10 | Landing se expone como volume externo UC `fmcg_lakehouse.bronze.landing` (sin claves de storage en el código) | 2 | experto (seguridad: acceso solo por UC) | `infra/02-unity-catalog.sql`: `CREATE EXTERNAL VOLUME IF NOT EXISTS fmcg_lakehouse.bronze.landing`; `src/bronze/registry.yaml`: `landing_root: /Volumes/{catalog}/bronze/landing` |
| INF-11 | Checkpoints y schema location de Auto Loader viven en el volume managed `fmcg_lakehouse.ops.checkpoints` | 1 | decisión D11 **(propuesta)** | `infra/02-unity-catalog.sql`: `CREATE VOLUME IF NOT EXISTS fmcg_lakehouse.ops.checkpoints`; `src/bronze/ingest.py`: `checkpointLocation` bajo ese volume |
| INF-12 | Todo DDL del repo es re-ejecutable: cada `CREATE` usa `IF NOT EXISTS` u `OR REPLACE` | 2 | buena práctica | `grep -nE "CREATE " infra/*.sql src/*/sql/*.sql` → ninguna línea sin `IF NOT EXISTS`/`OR REPLACE`; `tests/test_gold_ddl.py::test_statements_are_idempotent_and_only_touch_gold_or_ops_gold_runs` |
| INF-13 | Los permisos por grupo están activos: `analysts` solo lectura sobre `gold`, `de_engineers` escritura | 2 | experto (seguridad, `docs/04`) / `docs/03` §3 | `infra/02-unity-catalog.sql`: sentencias `GRANT` **sin comentar** (`--`); `docs/ESTADO.md` sin el pendiente "Permisos/grupos comentados" |
| INF-14 | La ejecución de la infraestructura y de UC está verificada con evidencia | 3 | buena práctica (H0–H1) | `docs/ESTADO.md` filas H0 y H1 en ✅ con objetos enumerados (credencial, 5 locations, catálogo, 5 esquemas, 2 volumes, tablas `ops.*`) |
| INF-15 | El repo no contiene secretos: sin tokens, claves de cuenta ni SAS, y `.gitignore` excluye `.env` y `.databricks/` | 3 | buena práctica | `.gitignore` contiene `.env`, `*.env`, `.databricks/`; `git grep -nE "dapi[0-9a-f]{32}|AccountKey=|sig=[A-Za-z0-9%]+"` sin resultados |
| INF-16 | Los scripts que escriben en la nube ofrecen modo simulación | 1 | buena práctica | `infra/run_sql.py`: argumento `--dry-run`; `infra/03-upload-landing.sh`: opción `--dry-run` |

## 2. Datos sintéticos y landing (DAT)

| ID | Criterio | P | Fuente | Evidencia |
|---|---|---|---|---|
| DAT-01 | El generador es reproducible: semilla fija por defecto (42) y misma semilla → mismos archivos | 3 | referencia (brecha 1: sin datos) / buena práctica | `src/datagen/__main__.py`: `--seed ... default=42`; `src/datagen/generator.py`: `random.Random(f"{seed}:{company}:{source}")`; `tests/test_datagen.py::test_deterministic_same_seed` |
| DAT-02 | Se generan 2 compañías (madre, hija) × 4 fuentes (customers, products, prices, orders) | 3 | referencia (tablas hija + madre) | `src/bronze/registry.yaml`: `companies: madre, hija` y 4 `sources`; `tests/test_registry_schemas.py::test_registry_expands_8_entries` |
| DAT-03 | El volumen por defecto es ~1 M de líneas de pedido en 24 meses (2024-01 a 2025-12) | 2 | referencia (rango de `dim_date`) / `docs/03` §4 | `docs/05-bronce-implementacion.md` §1: "1.005.642 líneas de pedido", "2024-01..2025-12"; `docs/ESTADO.md` H2 |
| DAT-04 | La hija trae sembradas las anomalías que limpia la referencia: duplicados (incl. entre archivos), espacios, mayúsculas mixtas, typo de ciudad, ciudad nula, categoría en minúsculas, typo de término, `product_id` no numérico, 4 formatos de fecha (incl. día de semana en texto), precio negativo, precio no numérico, `customer_id` inválido, `order_qty` nulo | 3 | referencia | `docs/05-bronce-implementacion.md` §1 (lista de `anomaly_catalog`); `src/datagen/generator.py` (una función o rama por anomalía) |
| DAT-05 | La madre sale limpia: 0 anomalías en todas las categorías | 2 | buena práctica (`docs/03` §4) | `tests/test_datagen.py::test_madre_clean_hija_dirty`; `docs/05` §1: "la madre da 0 en todas" |
| DAT-06 | El generador escribe `MANIFEST.json` con conteo de filas por archivo y conteo por anomalía, y los totales cuadran con los archivos | 3 | buena práctica (reconciliación) | `src/README.md` (mención a `MANIFEST.json`); `tests/test_datagen.py::test_manifest_matches_files` y `::test_totals_are_sum_of_files` |
| DAT-07 | La carpeta destino sigue `landing/<compañía>/<fuente>/ingest_date=YYYY-MM-DD/<fuente>_<compañía>_<YYYYMMDD>.csv` | 2 | experto (carpeta de destino) | `docs/05` §1 (layout); `src/bronze/registry.yaml` (patrón de nombre); `tests/test_registry_schemas.py::test_file_pattern_matches_datagen_names` |
| DAT-08 | El formato de archivo está declarado: CSV, UTF-8, separador `,`, con encabezado | 2 | experto (formato) | `src/bronze/registry.yaml` `defaults`: `format: csv`, `encoding: UTF-8`, `delimiter: ","`, `header: true` |
| DAT-09 | Los datos generados no se versionan en git | 1 | buena práctica | `.gitignore` contiene `data/generated/`; `git ls-files data/` vacío |
| DAT-10 | La carga a landing es un script reproducible que no borra ni mueve archivos | 2 | buena práctica | `infra/03-upload-landing.sh`: `az storage blob upload-batch`; sin `delete`/`delete-batch`/`mv` |
| DAT-11 | Está documentado que los archivos subidos a landing igualan al manifest | 3 | buena práctica (criterio de salida H2) | `docs/ESTADO.md` H2 en ✅: "1.606 CSV subidos = 1.606 del MANIFEST" |
| DAT-12 | La retención de landing a 90 días está **aplicada** como política de ciclo de vida del storage | 1 | experto (reglas de retención) | `docs/ESTADO.md`: pendiente "Ciclo de vida de landing a 90 días" marcado ✅ (hoy ⛔), o script en `infra/` con `management-policy create` y la regla `landing-90d` documentado como ejecutado |
| DAT-13 | `customers`/`products` se emiten como snapshot mensual por defecto, con opción diaria, y la diferencia con la frecuencia declarada está documentada | 1 | decisión D12 **(propuesta)** | `src/datagen/__main__.py`: opción `--dim-snapshots`; `src/bronze/registry.yaml`: comentario junto a `frequency: daily`; `docs/05` §1 |

## 3. Bronze (BRZ)

| ID | Criterio | P | Fuente | Evidencia |
|---|---|---|---|---|
| BRZ-01 | Todas las columnas de negocio de Bronze son `STRING` y el esquema se declara (no se infiere) | 3 | experto (todo STRING) / referencia (corrige `inferSchema`) | `src/bronze/schemas.py` (`ddl_schema_string`); `grep -rn inferSchema src/bronze` vacío; `tests/test_registry_schemas.py::test_schema_all_string_and_declared` |
| BRZ-02 | La ingesta usa Auto Loader (`cloudFiles`) con `trigger(availableNow=True)` y checkpoint por compañía × fuente | 3 | referencia | `src/bronze/ingest.py`: `format("cloudFiles")`, `.trigger(availableNow=True)`, `.option("checkpointLocation", ...)` |
| BRZ-03 | Lo que no encaja en el esquema se conserva en `_rescued_data` | 2 | referencia | `src/bronze/ingest.py`: `.option("rescuedDataColumn", schemas.RESCUED)`; `src/bronze/schemas.py`: `RESCUED = "_rescued_data"` |
| BRZ-04 | Cada fila lleva los metadatos `_company`, `_source_system`, `_source`, `_source_file`, `_file_size`, `_file_modification_time`, `_ingest_ts`, `_ingest_date`, `_batch_id`, `_load_type` | 3 | experto (origen, fecha de ingesta, tipo de carga) / referencia | `src/bronze/schemas.py`: `METADATA_COLUMNS`; `tests/test_registry_schemas.py::test_table_columns_order_and_metadata` |
| BRZ-05 | Bronze no transforma las columnas de negocio (sin limpieza, tipado ni dedup) | 2 | experto ("capturar todo, no transformar nada") | `src/bronze/ingest.py`: `grep -nE "dropDuplicates|initcap|trim\(|to_date|regexp_replace"` vacío; solo `withColumn` de metadatos |
| BRZ-06 | Bronze es append-only también en dimensiones: las tablas `bronze.<c>_<f>` solo se escriben con `mode("append")`; `overwrite` solo en staging | 3 | referencia (corrige su `overwrite`) / `docs/04` | `src/bronze/ingest.py`: escritura a Bronze con `.mode("append")`; la única `mode("overwrite")` escribe en `p["staging"]` |
| BRZ-07 | Los archivos de landing no se mueven ni se borran: el checkpoint es la verdad de "ya procesado" | 2 | referencia (corrige `dbutils.fs.mv`) | `grep -rnE "fs\.mv|fs\.rm|\.delete\(" src/bronze` vacío |
| BRZ-08 | Cada corrida deja en `bronze.staging_<c>_<f>` solo el lote nuevo | 2 | referencia (staging incremental) | `src/bronze/ingest.py` paso 5 (filtro por `_batch_id` de la corrida); `src/bronze/schemas.py` (DDL de staging); `tests/test_registry_schemas.py::test_table_ddl_properties_and_staging` |
| BRZ-09 | Si una corrida no trae filas nuevas, staging conserva el último lote con datos | 1 | decisión D9 **(propuesta)** | `src/bronze/ingest.py`: el `overwrite` de staging solo ocurre cuando el lote tiene filas; `docs/05` §4 D9 |
| BRZ-10 | La configuración de fuentes es un dato: `registry.yaml` declara sistema origen, frecuencia, formato, ruta, columnas, tipo de carga y retención, y se siembra en `ops.source_registry` | 3 | experto (sistemas a migrar, frecuencia, formato, carpeta) | `src/bronze/registry.yaml` (`madre_erp`, `hija_export`, `frequency`, `load_type`, `columns`, `landing_retention_days`); `infra/02b-seed-source-registry.sql`; `tests/test_registry_schemas.py::test_seed_file_in_sync_with_registry`; `docs/ESTADO.md` H1: "8 filas en `ops.source_registry`" |
| BRZ-11 | Antes de cargar se aplican 6 validaciones mínimas por archivo (vacío, extensión, patrón de nombre, encabezado = columnas esperadas, carpeta `ingest_date` válida, sin filas de datos) | 3 | experto (validaciones mínimas pre-carga) | `src/bronze/validation.py`: `R01_EMPTY_FILE` … `R06_NO_DATA_ROWS`; `tests/test_validation.py` |
| BRZ-12 | Los archivos rechazados se registran en `ops.landing_rejects` y sus filas no entran a Bronze | 3 | experto | `src/bronze/ingest.py`: `saveAsTable(f"{catalog}.ops.landing_rejects")` y filtro por `_source_file` rechazado en `foreachBatch` |
| BRZ-13 | Un reintento no duplica: la escritura usa `txnAppId`/`txnVersion` además del checkpoint | 2 | buena práctica | `src/bronze/ingest.py`: `.option("txnAppId", ...)` y `.option("txnVersion", micro_batch_id)` |
| BRZ-14 | La generación del checkpoint es un parámetro (`--generation`) que forma parte del `txnAppId` | 1 | decisión D14 **(propuesta)** | `src/bronze/ingest.py`: argumento `--generation` y `txnAppId` con sufijo `_g{generation}` |
| BRZ-15 | Las tablas Bronze fijan retención Delta: `delta.logRetentionDuration = 30 days`, `delta.deletedFileRetentionDuration = 7 days` | 2 | experto (retención) | `src/bronze/schemas.py`: `TABLE_PROPERTIES`; `tests/test_registry_schemas.py::test_table_ddl_properties_and_staging` |
| BRZ-16 | Las tablas Bronze son managed (sin `LOCATION`) dentro del catálogo con managed location `uc-managed` | 1 | decisión D15 **(propuesta)** | `src/bronze/schemas.py` (`table_ddl` sin `LOCATION`) |
| BRZ-17 | Está documentado que Bronze reconcilia con landing (8/8 tablas en filas y archivos, 0 `_rescued_data`) y que una 2.ª corrida escribe 0 filas | 3 | buena práctica (criterio de salida H3) | `docs/ESTADO.md` H3 en ✅ con esos resultados |

## 4. Silver (SLV)

| ID | Criterio | P | Fuente | Evidencia |
|---|---|---|---|---|
| SLV-01 | Las columnas de Silver tienen tipos correctos: fechas `DATE`, cantidades `INT`, montos y precios `DECIMAL`, banderas `BOOLEAN` | 3 | experto (tipos de datos correctos) | `src/silver/sql/01_silver_ddl.sql`: `order_date DATE`, `order_qty INT`, `unit_price`/`line_amount DECIMAL(...)`, `*_was_* BOOLEAN` |
| SLV-02 | El dedup es determinista: `row_number()` por clave de negocio, gana el mayor `_ingest_ts` con desempate explícito; no hay `dropDuplicates` | 3 | referencia (corrige `dropDuplicates` arbitrario) | `src/silver/transform.py` (`dedup()` con `row_number` y orden por `_ingest_ts`, `_source_file`, `_batch_id` desc); `grep -n dropDuplicates src/silver` vacío; `tests/test_silver_rules.py::test_dedup_latest_ingest_wins_and_is_deterministic` |
| SLV-03 | La clave de negocio de cada entidad es la clave natural + `company` | 3 | buena práctica (`docs/03` §5.2) | `docs/08` §2 (claves); `src/silver/sql/01_silver_ddl.sql`: columnas `customer_key`/`product_key` = `company:id` |
| SLV-04 | La clave de orders es `(company, order_id, line_no)` y el dedup se aplica antes de evaluar reglas | 2 | decisión D21 **(propuesta)** | `src/silver/transform.py` (función de orders: dedup por esas 3 columnas antes de las reglas); `docs/08` §4 D21 |
| SLV-05 | Las dimensiones tienen miembro desconocido `-1` y las órdenes con `customer_id` inválido o no resuelto apuntan a `-1` con bandera; no existe el valor de reserva `999999` | 3 | referencia (corrige `999999`) / decisión D19 **(propuesta)** | `src/silver/sql/01_silver_ddl.sql` (siembra de filas `-1`, `is_unknown_member`); `tests/test_silver_rules.py::test_order_invalid_customer_goes_to_unknown_member`; `grep -rn 999999 src` vacío |
| SLV-06 | customers: nombre con trim + initcap y ciudad corregida desde `silver.ref_city_mapping` | 3 | referencia | `src/silver/rules.py` (`CUS_03`, `CUS_04`, `CUS_05`); `src/silver/sql/01_silver_ddl.sql`: `ref_city_mapping`; `tests/test_silver_rules.py::test_hija_customers_seeded_anomalies_detected` |
| SLV-07 | customers: ciudad nula → `Unknown` con bandera `city_was_null` (no se descarta la fila) | 2 | experto (tratamiento de nulos) / decisión D23 **(propuesta)** | `src/silver/rules.py` (`CUS_06`); DDL `city_was_null BOOLEAN` |
| SLV-08 | products: typo de término corregido desde `ref_typo_fixes`, categoría canónica y `division` desde `ref_category_division`, `variant` extraída del paréntesis y `product_code = sha2(product_name)` | 2 | referencia | `src/silver/sql/01_silver_ddl.sql` (columnas `variant`, `product_code`, `division`, comentarios); `src/silver/transform.py`: `F.sha2`; `tests/test_silver_rules.py::test_hija_products_seeded_anomalies_detected` |
| SLV-09 | products: `product_id` con prefijo `PX-<n>` se recupera a `<n>` con bandera; lo irrecuperable va a cuarentena | 2 | decisión D16 **(propuesta)** | `src/silver/rules.py` (`PRD_01`, `PRD_02`); DDL `product_id_was_prefixed`, `product_id_raw` |
| SLV-10 | prices: el mes se parsea de todos los formatos sembrados y el precio negativo se corrige a positivo con bandera `price_was_negative` | 3 | referencia / experto ("no admite negativos") | `src/silver/rules.py` (`PRC_03`, `PRC_04`, `DATE_FORMATS`); `tests/test_silver_rules.py::test_price_cases` |
| SLV-11 | prices: precio no numérico o nulo va a cuarentena | 3 | decisión D27 | `src/silver/rules.py`: `PRICE_INVALID_ACTION = CUARENTENA` y regla `PRC_05` |
| SLV-12 | orders: la fecha se parsea en los 4 formatos de la hija, incluido `"Tuesday, July 01, 2025"` | 3 | referencia | `src/silver/rules.py` (`ORD_06`, formato `weekday_long`); `tests/test_silver_rules.py::test_parse_date_four_formats_plus_iso` y `::test_every_datagen_date_format_parses` |
| SLV-13 | orders: `order_qty` nula, cero, negativa o no entera va a cuarentena (no se descarta en silencio) | 3 | referencia / experto (nulos) | `src/silver/rules.py` (`ORD_03`, `ORD_04`); `tests/test_silver_rules.py::test_order_quarantine_rules` |
| SLV-14 | Los joins con customers, products y prices son `LEFT` (nunca `INNER`) y los no resueltos se marcan y cuentan | 3 | referencia (corrige `INNER JOIN` que perdía pedidos) | `src/silver/transform.py`: cada `.join(...)` con `"left"`, ninguno con `"inner"`; reglas `ORD_08`, `ORD_09`, `PRC_07` en `src/silver/rules.py` |
| SLV-15 | `line_amount` se calcula en Silver como cantidad × precio vigente del mes; sin precio queda `NULL` con bandera `price_missing` | 3 | decisión D28 | `src/silver/sql/01_silver_ddl.sql`: `line_amount`, `price_missing`; `tests/test_silver_rules.py::test_order_unresolved_customer_product_and_price` |
| SLV-16 | Precios y montos llevan columna `currency` = `'PEN'` desde una sola constante; no aparece `INR` ni `USD` en el código de Silver | 3 | experto (estandarizar monedas) / decisión D26 | `src/silver/rules.py:20`: `CURRENCY = "PEN"`; `grep -rnE "INR|USD" src/silver` vacío |
| SLV-17 | orders incluye derivados de fecha `order_year`, `order_quarter`, `order_month`, `order_month_start` e `is_weekend` | 2 | experto (cálculos derivados) | `src/silver/sql/01_silver_ddl.sql` (tabla `silver.orders`); `tests/test_silver_rules.py::test_order_happy_path_and_derivations` |
| SLV-18 | Los mapeos de negocio viven en 4 tablas `silver.ref_*` sembradas solo-insert (sin pisar ediciones), no en diccionarios dentro de `transform.py` | 2 | buena práctica (`docs/03` §5.2) / decisión D24 **(propuesta)** | `src/silver/sql/01_silver_ddl.sql`: `ref_city_mapping`, `ref_category_division`, `ref_typo_fixes`, `ref_company` y siembra que inserta solo lo faltante; `src/silver/transform.py` lee `ref_*` |
| SLV-19 | La escritura es `MERGE` idempotente por clave de negocio que solo actualiza con datos más recientes, y una 2.ª corrida `full` dio los mismos conteos | 3 | referencia (MERGE incremental) / buena práctica | `src/silver/transform.py`: `MERGE` con `WHEN MATCHED AND s._ingest_ts >= t._ingest_ts`; `docs/ESTADO.md` H4: "2.ª corrida `full` con los mismos conteos" |
| SLV-20 | Está documentado que Silver en el workspace coincide con los conteos esperados (`src/silver/expected.py`): orders 699.699 + 296.754, cuarentena 4.633 (orders) y 24 (prices) | 3 | buena práctica (criterio de salida H4) | `docs/08-silver-implementacion.md` §5; `docs/ESTADO.md` H4 en ✅ |

## 5. Gold (GLD)

| ID | Criterio | P | Fuente | Evidencia |
|---|---|---|---|---|
| GLD-01 | Gold es un esquema en estrella con `dim_date`, `dim_customer`, `dim_product`, `dim_price` y el hecho `fact_sales`, conformado para madre e hija | 3 | experto (hechos y dimensiones) / referencia | `src/gold/sql/01_gold_ddl.sql`: 5 `CREATE TABLE IF NOT EXISTS fmcg_lakehouse.gold.<tabla>` |
| GLD-02 | `dim_date` tiene grano diario con año, trimestre, mes, nombre de mes y fin de semana, y cubre al menos 2024-01-01 a 2025-12-31 | 2 | referencia (grano mensual 2024–2025, ampliado) | `src/gold/sql/01_gold_ddl.sql` (`dim_date`); `src/gold/model.py` (`date_rows`); `tests/test_gold_model.py` |
| GLD-03 | Las claves sustitutas son `xxhash64` de la clave de negocio (estables entre recargas) y cada recarga verifica su unicidad | 2 | decisión G2 **(propuesta)** | DDL `customer_sk ... xxhash64(customer_key)`; `tests/test_gold_static.py::test_dimensions_get_unknown_member_and_uniqueness_check` |
| GLD-04 | Cada dimensión tiene un único miembro `-1` | 2 | decisión G3 **(propuesta)** | `tests/test_gold_ddl.py::test_unknown_member_select_covers_all_columns`; DDL `is_unknown_member` |
| GLD-05 | `dim_price` guarda el historial de precios con vigencia (`valid_from`, `valid_to`) por `(company, product_id, valid_from)` | 3 | referencia (corrige que el año nuevo pisaba al anterior) | `src/gold/sql/01_gold_ddl.sql` (`dim_price` con `valid_from`, `valid_to`); `tests/test_gold_model.py` (vigencia) |
| GLD-06 | `fact_sales` tiene grano línea de pedido con clave única `(company, order_id, line_no)` y claves a las 4 dimensiones; el precio se resuelve por rango de fechas | 3 | buena práctica (`docs/03` §5.3) / decisión G6 **(propuesta)** | DDL `fact_sales` (`date_sk`, `customer_sk`, `product_sk`, `price_sk`); `src/gold/transform.py`: `order_date` `between` `valid_from` y `valid_to` |
| GLD-07 | `fact_sales` reutiliza `line_amount` de Silver y no recalcula montos | 2 | decisión D28 | `src/gold/transform.py`: `line_amount` tomado de `silver.orders`, sin `order_qty * unit_price`; `docs/09` §1 |
| GLD-08 | Todo monto en Gold viaja con `currency` y toda agregación agrupa también por `currency`; el SQL de Gold no tiene monedas fijas (`INR`, `PEN`, `USD`) ni la columna `total_amount_inr` | 3 | decisión D26 / experto (monedas) | `tests/test_gold_ddl.py::test_no_hardcoded_currency_and_amounts_carry_currency`; `grep -nE "INR|total_amount_inr" src/gold` vacío |
| GLD-09 | Existe el agregado `agg_sales_monthly` (mes × compañía × cliente × producto × moneda) con `revenue`, `total_qty`, `orders`, `order_lines` y `lines_without_price`, derivado de `fact_sales` | 3 | experto (tablas agregadas de negocio) / referencia (agregado mensual) | `src/gold/sql/01_gold_ddl.sql` (`agg_sales_monthly`); `resources/gold_job.yml`: `agg_sales_monthly` depende de `fact_sales` |
| GLD-10 | Hay vistas de KPI que responden las preguntas de negocio: ventas mensuales (ingresos, cantidad, pedidos, ticket), top 10 productos, mix por categoría y comparativo madre vs hija | 3 | referencia (KPIs del dashboard) / experto (tablas que resuelven una pregunta) | `src/gold/sql/01_gold_ddl.sql`: `v_kpi_sales_monthly`, `v_kpi_top10_products`, `v_kpi_category_mix`, `v_kpi_company_comparison` |
| GLD-11 | Cada tabla y vista de Gold declara el caso de negocio o la pregunta que resuelve y su consumidor (BI, ML o AI) | 2 | experto (define casos de negocio) | `COMMENT` de cada tabla/vista en `src/gold/sql/01_gold_ddl.sql` o tabla "pregunta → objeto Gold" en `docs/09`; parcial si solo describen grano y clave |
| GLD-12 | Existe una tabla de features ML `feature_sales_monthly` con lags 1–3 de ingreso y cantidad por mes calendario, promedio móvil que excluye el mes actual y etiqueta `target_revenue_next_month` (NULL si aún no se conoce) | 3 | experto (Gold también para ML/AI) / decisión G9 **(propuesta)** | DDL `feature_sales_monthly` (`revenue_lag_1..3`, `revenue_avg_3m`, `target_revenue_next_month`); `tests/test_gold_model.py` (lags con huecos, etiqueta NULL) |
| GLD-13 | Todas las columnas de las tablas Gold tienen `COMMENT` (metadatos semánticos para Genie/AI) | 2 | experto (consumo por AI) / buena práctica | `src/gold/sql/01_gold_ddl.sql`: cada línea de columna dentro de `CREATE TABLE` contiene `COMMENT '...'` |
| GLD-14 | Hecho y agregados usan clustering líquido (`CLUSTER BY`) en lugar de particionado o Z-order manual | 2 | referencia (optimización con Z-order) / buena práctica | `src/gold/sql/01_gold_ddl.sql`: 3 `CLUSTER BY`; `grep -nE "ZORDER|PARTITIONED BY" src/gold` vacío |
| GLD-15 | Cada tabla Gold se reconstruye completa con `INSERT OVERWRITE` (idempotente) y está documentado que una 2.ª corrida dio resultados idénticos | 3 | decisión G4 **(propuesta)** / buena práctica | `src/gold/transform.py`: `INSERT OVERWRITE`; `docs/ESTADO.md` H5: "2.ª corrida idéntica" |
| GLD-16 | Está documentado que Gold reconcilia con Silver: `fact_sales` = 996.453 filas = `silver.orders` e ingresos 37.450.346,40 PEN iguales en Silver, `fact_sales` y `agg_sales_monthly` | 3 | buena práctica | `docs/ESTADO.md` H5 |
| GLD-17 | Los KPIs de Gold coinciden con un cálculo independiente en Python desde los CSV de datagen (ingresos y cantidad por compañía × año) | 3 | buena práctica (criterio de salida H5) | script o prueba en el repo (p. ej. `src/gold/expected.py` o `tests/`) **y** resultado documentado en `docs/ESTADO.md` H5 sin la nota "falta KPI independiente" |
| GLD-18 | Las ventas con cliente desconocido cuentan en ingresos y su volumen queda visible | 1 | decisión G7 **(propuesta)** | `src/gold/sql/01_gold_ddl.sql`: `v_kpi_company_comparison` con conteo de líneas de cliente `-1` |
| GLD-19 | Gold solo lee de Silver/ops y nunca escribe en Silver ni Bronze | 2 | buena práctica | `tests/test_gold_static.py` (escrituras solo en `gold.*` y `ops.gold_runs`); `tests/test_gold_ddl.py::test_statements_are_idempotent_and_only_touch_gold_or_ops_gold_runs` |
| GLD-20 | El tipo de historia de cada dimensión (SCD1 hoy; SCD2 abierta) está declarado en el `COMMENT` de la tabla y en `docs/09` | 1 | decisión G1 **(propuesta)** | DDL: `COMMENT 'Clientes SCD1 ...'` y equivalente en `dim_product`; `docs/09` §2 G1 |

## 6. Orquestación e incremental (ORQ)

| ID | Criterio | P | Fuente | Evidencia |
|---|---|---|---|---|
| ORQ-01 | Todos los jobs están definidos como código en el Asset Bundle y enlazados desde `databricks.yml` | 3 | referencia (promete Workflows) / buena práctica | `databricks.yml`: job `bronze_ingest` e `include` con `resources/silver_job.yml` y `resources/gold_job.yml` (y el job maestro de ORQ-07) |
| ORQ-02 | `databricks bundle validate -t dev` pasa sobre el bundle completo | 2 | buena práctica | `docs/ESTADO.md` ("Verificado: ... `bundle validate`") o `docs/09` §3, referido al bundle con los tres jobs enlazados |
| ORQ-03 | El bundle sincroniza solo el código necesario (incluye `src/bronze`, `src/silver`, `src/gold`; excluye `src/datagen`) | 1 | buena práctica | `databricks.yml`: `sync.include` y `sync.exclude` |
| ORQ-04 | Todas las tareas corren en serverless: usan `environment_key` y ningún job declara `new_cluster`, `job_cluster_key` ni `existing_cluster_id` | 2 | decisión D6 | `grep -nE "new_cluster|job_cluster_key|existing_cluster_id" databricks.yml resources/*.yml` vacío |
| ORQ-05 | El catálogo es un parámetro (`var.catalog` → `--catalog`) y no hay rutas fijas de workspace ni de S3 en el código | 2 | referencia (brecha 5: rutas fijas) | `databricks.yml` `variables.catalog`; `grep -rnE "/Workspace/|s3://" src resources` vacío |
| ORQ-06 | Silver respeta dependencias: `prices` ← `products`; `orders` ← `prices` y `customers` | 2 | experto (orquestación con dependencias) / decisión D24 **(propuesta)** | `resources/silver_job.yml` (`depends_on`); `tests/test_silver_static.py::test_dependency_order` |
| ORQ-07 | Un job maestro encadena Bronze → Silver → Gold (Silver espera a Bronze y Gold a Silver) | 3 | experto (Silver depende de Bronze) / buena práctica (H6) | `resources/*.yml`: job con `run_job_task` (o tareas) y `depends_on` entre `bronze_ingest`, `silver_transform` y `gold_build` |
| ORQ-08 | Gold respeta dependencias: `setup` → dimensiones → `dim_price` → `fact_sales` → `agg_sales_monthly` → `feature_sales_monthly` | 2 | decisión G8 **(propuesta)** | `resources/gold_job.yml`; `tests/test_gold_ddl.py::test_job_tasks_and_dependencies_match_transform` |
| ORQ-09 | Ningún job admite corridas concurrentes | 1 | buena práctica | `max_concurrent_runs: 1` en cada job de `databricks.yml` y `resources/*.yml` |
| ORQ-10 | Silver tiene modo `incremental` (lee `bronze.staging_*`) y modo `full` (lee Bronze completo) seleccionable por parámetro | 2 | referencia (carga full + incremental) | `resources/silver_job.yml`: parámetro `mode`; `src/silver/run.py`: `--mode (incremental\|full)`; `tests/test_silver_static.py::test_run_args_defaults_and_choices` |
| ORQ-11 | Está documentada una corrida incremental real: un segundo lote de landing solo agrega o modifica lo afectado en Bronze, Silver y Gold | 3 | buena práctica (criterio de salida H6) / referencia (carga incremental) | `docs/ESTADO.md` H6 en ✅ con conteos antes/después del lote incremental |
| ORQ-12 | Está documentada una corrida end-to-end verde del job maestro | 3 | buena práctica (criterio de salida H6) | `docs/ESTADO.md` H6 en ✅ con id o fecha de la corrida y estado de cada tarea |
| ORQ-13 | El job maestro declara su disparador (`schedule` o `trigger.file_arrival` sobre landing), pausado en `dev` | 1 | experto (frecuencia de actualización) | `resources/*.yml` del job maestro: bloque `schedule` o `trigger` con `pause_status` |
| ORQ-14 | Los jobs avisan o reintentan ante fallas (`max_retries` en tareas o `email_notifications.on_failure`) | 1 | buena práctica | `resources/*.yml` / `databricks.yml`: `max_retries` o `on_failure` |
| ORQ-15 | El procedimiento de despliegue documenta el motor `direct` del bundle | 1 | decisión D29 | `docs/ESTADO.md` (trampas: `DATABRICKS_BUNDLE_ENGINE=direct`) y el README de despliegue |

## 7. Serving: dashboard y Genie (SRV)

| ID | Criterio | P | Fuente | Evidencia |
|---|---|---|---|---|
| SRV-01 | El dashboard AI/BI está versionado en el repo y declarado en el bundle | 3 | referencia (dashboard "Sales Insights") / buena práctica | archivo `*.lvdash.json` en el repo; `databricks.yml` o `resources/*.yml` con `resources.dashboards` que lo referencia |
| SRV-02 | El dashboard filtra por año, trimestre y mes | 3 | referencia | `*.lvdash.json`: widgets de filtro sobre `year`, `quarter`, `month` (o equivalentes de `dim_date`) |
| SRV-03 | El dashboard muestra ingresos totales y cantidad total | 3 | referencia | `*.lvdash.json`: contadores con `SUM(revenue)` y `SUM(total_qty)` (o campos de `v_kpi_sales_monthly`) |
| SRV-04 | El dashboard muestra el top 10 de productos por ingresos | 3 | referencia | `*.lvdash.json`: gráfico de barras sobre `v_kpi_top10_products` |
| SRV-05 | El dashboard muestra la distribución de ventas por categoría | 3 | referencia | `*.lvdash.json`: visual sobre `v_kpi_category_mix` |
| SRV-06 | El dashboard compara madre vs hija (participación y variación mensual) | 2 | buena práctica (`docs/03` §5.4) | `*.lvdash.json`: visual sobre `v_kpi_company_comparison` |
| SRV-07 | El dashboard tiene un panel de calidad con filas rechazadas por regla | 2 | buena práctica (`docs/03` §5.4) | `*.lvdash.json`: visual sobre `v_kpi_quarantine_summary` o `v_kpi_data_quality` |
| SRV-08 | Todos los datasets del dashboard consultan solo objetos de `gold` | 3 | experto (Gold es la capa de consumo de BI) | `*.lvdash.json`: cada `query` referencia `fmcg_lakehouse.gold.*`; `grep -nE "\.(silver|bronze)\." *.lvdash.json` vacío |
| SRV-09 | Los montos del dashboard muestran la moneda PEN (o la columna `currency`) y no INR | 2 | decisión D26 | `*.lvdash.json`: títulos o formato con `PEN`/`S/` o campo `currency`; `grep -n INR` vacío |
| SRV-10 | Hay capturas del dashboard publicado en el repo, enlazadas desde el README | 2 | referencia (`docs/Dashboard.png`) / H7 | imagen(es) en `docs/` (p. ej. `docs/img/dashboard*.png`) y enlace en `README.md` |
| SRV-11 | Existe un espacio Genie sobre `gold` con tablas/vistas elegidas, instrucciones y al menos 5 preguntas de ejemplo, documentado en el repo | 3 | referencia ("Ask Genie") / experto (Gold para AI) | archivo de configuración o sección en `docs/` con lista de objetos `gold.*`, texto de instrucciones y preguntas de ejemplo |
| SRV-12 | Cada pregunta de ejemplo de Genie tiene su SQL de referencia y el valor esperado, comprobado contra Gold | 2 | buena práctica | tabla pregunta → SQL → resultado esperado en `docs/` (resultado tomado de las vistas KPI o de GLD-16) |
| SRV-13 | Hay al menos una captura de Genie respondiendo una pregunta de negocio | 1 | referencia / H7 | imagen en `docs/` enlazada desde `README.md` o desde el documento de Genie |
| SRV-14 | El SQL Warehouse usado es serverless con auto-stop corto (≤ 10 min) | 1 | decisión D6 / `docs/03` §5.4 | documento de serving o `docs/ESTADO.md` con tipo y `auto_stop_mins` del warehouse, o recurso del bundle con esos campos |

## 8. Calidad y observabilidad (DQO)

| ID | Criterio | P | Fuente | Evidencia |
|---|---|---|---|---|
| DQO-01 | Las reglas de calidad de Silver forman un catálogo explícito: 33 reglas con id, entidad, descripción, severidad y acción | 3 | experto (reglas de calidad explícitas) | `src/silver/rules.py`: `RULES` (33 `_r(...)`); `tests/test_silver_rules.py::test_rule_catalog_is_consistent` |
| DQO-02 | Cada regla del catálogo se usa en la transformación Spark | 2 | buena práctica | `tests/test_silver_static.py::test_every_rule_is_used_by_transform_and_every_reference_exists` |
| DQO-03 | Las filas rechazadas van a `quarantine.<entidad>` (4 tablas) con la fila cruda, `rule_id`, `reason` y `_batch_id` | 3 | buena práctica (`docs/03` §5.2) / experto (no descartar nulos sin rastro) | `src/silver/sql/01_silver_ddl.sql`: 4 `CREATE TABLE ... quarantine.*` con esas columnas |
| DQO-04 | `ops.dq_results` registra por corrida y por regla las filas evaluadas y fallidas | 3 | buena práctica (`docs/03` §6) | `src/silver/sql/01_silver_ddl.sql`: `ops.dq_results` con `rows_evaluated`, `rows_failed` |
| DQO-05 | Cada capa registra sus corridas: `ops.bronze_runs` (incluso `FAILED`) y `ops.gold_runs` (con `checks`) | 2 | buena práctica | `src/bronze/ingest.py`: escritura a `ops.bronze_runs` en éxito y error; `src/gold/transform.py`: escritura a `ops.gold_runs` con `checks` |
| DQO-06 | Gold falla la tarea si una reconciliación no cuadra (no solo la registra) | 2 | buena práctica (`docs/03` §6: "si no cuadra, el job falla") | `src/gold/transform.py`: `check()` hace `raise RuntimeError` cuando `ok` es falso; `tests/test_gold_static.py::test_every_table_reconciles_and_logs` |
| DQO-07 | La conciliación Bronze = Silver + cuarentena + duplicados descartados está documentada por entidad y compañía | 3 | decisión D19 **(propuesta)** | `docs/08-silver-implementacion.md` §5 (tabla con las 4 columnas por entidad·compañía) |
| DQO-08 | Cada anomalía sembrada por datagen se detecta con su regla y pytest compara el conteo con `MANIFEST.json` | 3 | buena práctica | `tests/test_silver_rules.py::test_hija_*_seeded_anomalies_detected`; `tests/test_silver_expected.py`; tabla de anomalías de `docs/08` §3 |
| DQO-09 | Los datos limpios de la madre no disparan ninguna regla | 2 | buena práctica | `tests/test_silver_rules.py::test_clean_madre_triggers_no_rule` |
| DQO-10 | La suite pytest cubre datagen, Bronze, Silver y Gold y el número de pruebas está documentado | 3 | referencia (brecha 6: sin pruebas reales) | `tests/test_*.py` por capa; conteo en `docs/09` §1 ("132 en total") o `docs/ESTADO.md` (sin cifras contradictorias entre ambos) |
| DQO-11 | Hay pruebas de contrato de esquema: columnas crudas = registro de Bronze, columnas Gold copiadas de Silver existen con el mismo tipo y los `.sql` generados coinciden con su generador | 2 | buena práctica (`docs/03` §6) | `tests/test_gold_ddl.py::test_passthrough_columns_exist_in_silver`, `::test_silver_types_preserved_in_gold`, `::test_sql_file_in_sync_with_generator`; `tests/test_silver_ddl.py` |
| DQO-12 | El código Spark es apto para serverless: sin RDD, `sparkContext`, `cache`, `persist` ni `toPandas` | 1 | decisión D6 / buena práctica | `tests/test_silver_static.py::test_transform_is_serverless_friendly`; `tests/test_gold_static.py::test_transform_is_serverless_friendly` |
| DQO-13 | Las métricas de calidad son consultables desde Gold: vista de última corrida por regla con su descripción y resumen de cuarentena | 2 | buena práctica | `src/gold/sql/01_gold_ddl.sql`: `v_kpi_data_quality`, `v_kpi_quarantine_summary`; `tests/test_gold_ddl.py::test_quality_view_embeds_all_silver_rules` |
| DQO-14 | Hay CI que corre pytest en cada push o PR | 2 | referencia (promete GitHub Actions) | `.github/workflows/*.yml` con `pytest` y disparadores `push`/`pull_request` |
| DQO-15 | La observabilidad de plataforma está documentada: consultas a system tables (costo y corridas de jobs) o Data Quality Monitoring sobre Gold | 1 | buena práctica (`docs/06`: "System tables y Data quality") | archivo `.sql` en el repo o sección en `docs/` con consultas a `system.billing.*` / `system.lakeflow.*` o la configuración del monitor |

## 9. Documentación, README y publicación (DOC)

| ID | Criterio | P | Fuente | Evidencia |
|---|---|---|---|---|
| DOC-01 | El estado que declara el README coincide con `docs/ESTADO.md` | 3 | buena práctica | `README.md` línea "Estado:" (hoy dice "H0") vs hitos de `docs/ESTADO.md` |
| DOC-02 | Todo archivo, herramienta o comando que menciona el README existe en el repo (sin promesas vacías como Terraform o `bronze.py`) | 3 | referencia (brecha 4: README promete lo que no existe) | cada ruta citada en `README.md` aparece en `git ls-files` |
| DOC-03 | El README explica cómo reproducir desde cero, en orden: provisión → UC → datagen → landing → `bundle deploy` → jobs → dashboard/Genie | 3 | buena práctica (criterio de salida H8) | `README.md`: sección de pasos que cita `infra/01`, `infra/02`, `02b`, `python -m datagen`, `infra/03-upload-landing.sh`, `databricks bundle deploy/run` |
| DOC-04 | El README muestra el diagrama de arquitectura, exportado desde el `.drawio` versionado | 2 | referencia (`docs/Architecture.png`) | `docs/arquitectura-databricks.drawio` + imagen exportada (`.png`/`.svg`) enlazada en `README.md` |
| DOC-05 | El README resume el modelo Gold y las decisiones clave, con enlace a `docs/` | 2 | buena práctica | `README.md`: sección de capas/modelo con enlaces a `docs/02`, `docs/09` |
| DOC-06 | El registro de decisiones tiene D9–D25 y G1–G9 resueltas (aceptadas, ajustadas o rechazadas por los expertos) | 2 | buena práctica | `docs/02-decisiones.md`: filas D9…D25 y G1…G9 con origen; sin la nota "sin registrar, pendientes de revisión" |
| DOC-07 | La moneda es coherente en toda la documentación vigente: PEN, sin pendientes de pasar a USD | 2 | decisión D26 | `docs/ESTADO.md` sin "Moneda a USD" como pendiente ni "Moneda USD" como decisión (hoy líneas 20 y 30); `docs/08` §6 duda 4 cerrada |
| DOC-08 | Cada capa (Bronze, Silver, Gold) tiene documento de requisitos que separa lo que pide la referencia de lo que suma la sala | 2 | experto | `docs/04-bronze-requisitos.md`, `docs/07-silver-requisitos.md` y un equivalente de Gold con las 5 respuestas de la sala (hoy falta) |
| DOC-09 | `docs/ESTADO.md` distingue verificado con ejecución de asumido, con evidencia por hito | 2 | buena práctica | `docs/ESTADO.md`: leyenda ✅/🟡/⛔/⏳, columna "Evidencia" y sección "Verificado vs asumido" al día con H4–H5 |
| DOC-10 | Los riesgos y lo no probado están escritos por capa | 1 | buena práctica | `docs/05` §5, `docs/08` §7–8, `docs/09` §4–5 |
| DOC-11 | El README atribuye la inspiración al repo de referencia con su commit | 1 | buena práctica | `README.md`: `takadlag/databricks-medallion-lakehouse-fmcg` y `5e99b2b` |
| DOC-12 | El repo tiene licencia | 1 | buena práctica | archivo `LICENSE` en la raíz |
| DOC-13 | El repo está publicado en GitHub y el README lo refleja | 3 | decisión D4 | `git remote -v` con `github.com`; `docs/ESTADO.md` H8 en ✅ |
| DOC-14 | Ni commits ni documentos llevan atribuciones a IA | 2 | buena práctica (regla del autor) | `git log --format=%B \| grep -iE "co-authored-by: claude\|generated with"` vacío; mismo `grep` sobre `git ls-files` vacío |
| DOC-15 | El repo publicado no expone datos personales ni identificadores de la suscripción (correos, id de suscripción o tenant) | 2 | buena práctica | `git grep -nE "@outlook\.|@gmail\.|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"` sin identificadores de suscripción/tenant |
| DOC-16 | Costos y limpieza están documentados: sin NAT, serverless, auto-stop y cómo borrar todo (`az group delete`) | 1 | buena práctica (`docs/03` §9) | `README.md` o `docs/`: sección de costos y comando de desmontaje |
| DOC-17 | El historial de git es trazable por capa y hito (mensajes con tipo y alcance) | 1 | decisión D4 | `git log --oneline`: mensajes `feat(bronze\|silver\|gold)`, `docs(...)` |

---

## Resumen

| Capa | Prefijo | Criterios | Peso total | De ellos (propuesta) |
|---|---|---|---|---|
| Infraestructura y Unity Catalog | INF | 16 | 34 | 1 |
| Datos sintéticos y landing | DAT | 13 | 28 | 1 |
| Bronze | BRZ | 17 | 39 | 3 |
| Silver | SLV | 20 | 54 | 5 |
| Gold | GLD | 20 | 48 | 7 |
| Orquestación e incremental | ORQ | 15 | 29 | 2 |
| Serving (dashboard + Genie) | SRV | 14 | 33 | 0 |
| Calidad y observabilidad | DQO | 15 | 34 | 1 |
| Documentación, README y publicación | DOC | 17 | 33 | 0 |
| **Total** | | **147** | **332** | **20** |

## Fórmula de cumplimiento

Cada criterio recibe un valor: **cumplido = 1**, **parcial = 0,5**, **no cumplido = 0**.

```
cumplimiento = Σ (peso_i × valor_i) / Σ peso_i      (sobre los criterios aplicables)
```

- **Umbral de aceptación: 93 %** (con el total actual, al menos 309 de 332 puntos).
- Se calcula global y por capa; la tabla de resumen da el denominador de cada capa.
- **Parcial** = la evidencia existe pero incompleta (p. ej. el archivo existe pero falta una de las partes enumeradas en el criterio, o el resultado está en código pero no documentado como ejecutado).
- Criterios **(propuesta)**: si los expertos cambian la decisión, el criterio se reescribe según lo decidido antes de evaluarlo; si la descartan sin reemplazo, el criterio sale del denominador (no aplica) y se anota en `docs/02-decisiones.md`.
- El juez anota por criterio: valor, ruta revisada y línea o resultado que lo sustenta.
