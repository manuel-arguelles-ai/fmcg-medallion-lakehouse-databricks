# Bronze — implementación

Estado: **código listo y probado localmente; nada ejecutado en Azure/Databricks** (regla de la sesión: solo lectura en la nube).
Fuente de verdad de requisitos: `04-bronze-requisitos.md`.

## 1. Qué se construyó

| Pieza | Archivo(s) | Notas |
|---|---|---|
| Generador sintético | `src/datagen/` | stdlib pura, semilla fija, 2 compañías × 4 fuentes, `MANIFEST.json` |
| Registro de fuentes | `src/bronze/registry.yaml`, `registry.py` | 8 entradas (compañía × fuente) como datos; DDL de `ops.*` y siembra de `ops.source_registry` |
| Esquemas | `src/bronze/schemas.py` | columnas de negocio todas STRING, DDL de tablas Bronze/staging con propiedades Delta |
| Validaciones | `src/bronze/validation.py` | 6 reglas puras (R01–R06) + escaneo de landing |
| Ingesta | `src/bronze/ingest.py` | Auto Loader `availableNow`, append-only, staging, `ops.bronze_runs` |
| Unity Catalog | `infra/02-unity-catalog.sql`, `infra/02b-seed-source-registry.sql` | **no ejecutados** |
| Subida a landing | `infra/03-upload-landing.sh` | **no ejecutado**, con `--dry-run` |
| Orquestación | `databricks.yml` | job `bronze_ingest`, 8 tareas serverless en paralelo; `bundle validate -t dev` OK |
| Pruebas | `tests/` | 31 pruebas pytest |

### Datos sintéticos
- Dominio propio (consumo masivo andino, PEN implícito): `madre` = ~28 productos y 1.500 clientes, `hija` = ~17 productos y 600 clientes (bares deportivos).
- Layout: `data/generated/<compañía>/<fuente>/ingest_date=YYYY-MM-DD/<fuente>_<compañía>_<YYYYMMDD>.csv`.
- Volumen por defecto (24 meses, 2024-01..2025-12, semilla 42): **1.606 archivos, 1.005.642 líneas de pedido, 36,7 MB, 2,8–3,9 s**.
  `customers` y `products` se emiten como snapshot **mensual** por defecto (`--dim-snapshots daily` para diario, ~730 archivos por fuente).
- Orders: un CSV por día; la `hija` mezcla 4 formatos de fecha por fila; algunos duplicados de pedido se reenvían en el archivo del día siguiente.
- Anomalías (solo `hija`; la `madre` da 0 en todas), catálogo completo en `MANIFEST.json → anomaly_catalog`:
  `dup_rows`, `dup_cross_file`, `name_whitespace`, `name_case_mixed`, `city_typo`, `city_null`, `category_lowercase`,
  `name_typo_term` (Energizante→Energisante), `product_id_non_numeric` (`PX-<id>`), `date_format_*` (4), `negative_price`,
  `non_numeric_price`, `customer_id_invalid`, `order_qty_null`.
- Los conteos del manifest son sobre filas **emitidas** (las copias duplicadas heredan las anomalías de su original).
  Para reconciliar en Silver: `rows` por archivo/compañía/fuente + conteos por anomalía.

## 2. Cómo se corre

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/pytest                                   # pruebas locales
cd src && ../.venv/bin/python -m datagen --out ../data/generated          # completo (o --small)
```
Pendiente de Manu, en este orden (cada paso con su confirmación):
1. Crear la storage credential por CLI y ejecutar `infra/02-unity-catalog.sql` y luego `02b-seed-source-registry.sql`.
2. `./infra/03-upload-landing.sh --dry-run` y después sin `--dry-run`.
3. `databricks bundle deploy -t dev` y `databricks bundle run bronze_ingest -t dev`.
4. Reconciliar: `SELECT _company, _source, count(*) FROM fmcg_lakehouse.bronze.<tabla> GROUP BY ...` contra `MANIFEST.json`; segunda corrida debe dar 0 filas nuevas.

### Flujo de `ingest.py` (una compañía × fuente)
1. Crea (si faltan) tablas `ops.*`, `bronze.<c>_<f>` y `bronze.staging_<c>_<f>`.
2. **Pre-validación**: recorre el Volume de landing, ignora archivos ya cargados (`_source_file` en Bronze) o ya rechazados, valida el resto y escribe los fallidos en `ops.landing_rejects`.
3. **Auto Loader** (`cloudFiles`, CSV, esquema declarado STRING, `rescuedDataColumn=_rescued_data`, `availableNow`), con metadatos desde `_metadata`.
4. `foreachBatch`: descarta las filas de archivos rechazados y hace `append` a Bronze (con `txnAppId`/`txnVersion` para que un reintento no duplique).
5. Staging = solo el lote nuevo (`_batch_id` de la corrida), sobrescrito por corrida.
6. Registra la corrida en `ops.bronze_runs` (también si falla, con `status=FAILED`).

## 3. Cumplimiento contra `04-bronze-requisitos.md`

| Requisito | Dónde se cumple | Validación |
|---|---|---|
| Ingerir crudo, sin transformar | `ingest.py` (solo agrega metadatos; sin limpieza) | revisión de código |
| Todas las columnas STRING, sin inferSchema | `schemas.py` (`ddl_schema_string`, `table_ddl`); `.schema()` declarado en `ingest.py` | pytest |
| Esquema declarado, no inferido | `schemas.py` generado desde `registry.yaml` | pytest |
| Auto Loader con checkpoint (carga exacta una vez) | `ingest.py` (`checkpointLocation` en Volume `ops.checkpoints`) | **pendiente en workspace** |
| `_rescued_data` | opción `rescuedDataColumn`, columna en la tabla | **pendiente en workspace** |
| Metadatos `_company … _load_type` | `schemas.METADATA_COLUMNS`, `ingest.py` | columnas: pytest; valores: **pendiente en workspace** |
| Staging solo con el lote nuevo | `ingest.py` paso 5 (`bronze.staging_<c>_<f>`) | **pendiente en workspace** |
| Append-only también en dimensiones | `ingest.py` (`mode("append")`, sin overwrite en Bronze) | revisión de código |
| Archivos de landing no se mueven | no hay `mv`/`delete` en el código; checkpoint = verdad | revisión de código |
| Configuración como datos (`ops.source_registry`) | `registry.yaml` → `ops.source_registry` (`02b`), test de sincronía | pytest |
| Sistemas, frecuencia, formato, carpeta | `registry.yaml` (`madre_erp`/`hija_export`, daily/monthly, csv, `landing_path`) | pytest |
| Retención landing 90 d | campo `landing_retention_days`; la política de storage **no está aplicada** (ver §5) | pendiente de Manu |
| Retención Bronze (histórico; log 30 d, deleted files 7 d) | `schemas.TABLE_PROPERTIES` en el DDL | pytest (DDL) |
| Seguridad: solo UC con credencial del Access Connector | `infra/02` (credential + external locations + volume); sin blob público ya desde 01 | pendiente de ejecutar |
| Validaciones mínimas pre-carga (6) | `validation.py` R01–R06 | pytest (incluye 1.606 archivos del datagen y casos malos) |
| Rechazados a `ops.landing_rejects`, no entran a Bronze | `ingest.py` pasos 2 y 4 | lógica: pytest; escritura: **pendiente en workspace** |
| Idempotente (re-ejecutar sin archivos nuevos no cambia nada) | checkpoint + `txnAppId`; solo se escribe un `bronze_runs` | **pendiente en workspace** |
| Serverless-compatible | sin RDD, `sparkContext`, cache ni globs; tarea sin cluster en el bundle | revisión de código; `bundle validate` OK |

## 4. Decisiones nuevas propuestas (para registrar en `02-decisiones.md`)

| # | Propuesta |
|---|---|
| D9 | Staging conserva el **último lote con datos**: si una corrida no trae filas nuevas no se vacía, así un reintento de Silver tras un fallo no pierde el lote. Silver debe ser idempotente (MERGE) |
| D10 | Pre-validación en el driver (Python) sobre los archivos nuevos, no dentro de Spark; los rechazados se leen por el checkpoint pero se filtran antes de escribir (lista negra por `_source_file`) |
| D11 | Checkpoints y schema location de Auto Loader en volume managed `fmcg_lakehouse.ops.checkpoints` |
| D12 | `customers`/`products` se emiten como snapshot mensual por defecto (el registro declara `daily`, que es la frecuencia real objetivo) |
| D13 | Celdas vacías del CSV llegan como `NULL` (comportamiento del lector CSV); `""` y vacío no se distinguen en Bronze |
| D14 | `txnAppId` lleva una *generación* (`--generation`): subirla solo si se recrea el checkpoint, o Delta ignoraría los lotes como ya escritos |
| D15 | Tablas Bronze **managed** en `uc-managed`; los contenedores `bronze/silver/gold` quedan como external locations reservadas (¿usarlos o eliminarlos?) |

## 5. Pendientes y riesgos

Pendientes de Manu: ejecutar `02`, `02b`, `03`, desplegar el bundle y correr el job; **aplicar el ciclo de vida de landing a 90 días** (propuesta, no ejecutada):
```bash
az storage account management-policy create --account-name stfmcglakehousedevcus001 -g rg-fmcg-lakehouse-dev-cus-001 \
  --policy '{"rules":[{"name":"landing-90d","enabled":true,"type":"Lifecycle","definition":{"filters":{"blobTypes":["blockBlob"],"prefixMatch":["landing/"]},"actions":{"baseBlob":{"delete":{"daysAfterModificationGreaterThan":90}}}}}]}'
```
(Ojo: borrar de landing a los 90 días es compatible con Auto Loader, pero impide reproceso desde cero más allá de ese plazo; Bronze es el histórico.)

Riesgos / dudas para los expertos:
- **No ejecutado en Spark**: `ingest.py` solo está validado por sintaxis y por la lógica pura de sus módulos (sin Java local). Puntos a verificar en el workspace: `rescuedDataColumn` junto a `.schema()`, `_metadata` tras `withColumn`, `recentProgress.numInputRows` como `rows_read`, `cloudFiles.schemaLocation` con esquema declarado, `txnVersion` en `foreachBatch` sobre serverless.
- Carrera: un archivo que llega entre la pre-validación y el inicio del stream no se valida y entra a Bronze aunque fuera inválido (se vería en Silver).
- La pre-validación recorre todo el Volume en cada corrida (barato a 1.600 archivos; con decenas de miles convendría particionar por fecha).
- El metastore es compartido y ya existen los catálogos `fmcg`, `fmcg_dev`, `pede_dev` y `dbw_fmcg_lakehouse_dev_cus_001` (este último y su external location parecen del propio workspace); `fmcg_lakehouse` no existe aún.
- El external volume sobre todo el contenedor `landing` exige que no haya tablas ni otros volumes bajo esa ruta.
- Sin clustering/particionado en Bronze (abierto en el diseño §5.1); a 1 M de filas no es necesario, a evaluar con los expertos.
- `PyYAML` debe estar disponible en serverless: el bundle lo declara como dependencia del entorno.
