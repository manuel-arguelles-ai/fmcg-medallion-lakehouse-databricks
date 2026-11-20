# Bronze — requisitos (referencia + expertos)

Fuentes de estos requisitos: (a) el repo de referencia (`/Users/macdenix/databricks-medallion-lakehouse-fmcg`, solo lectura), (b) las respuestas de la sala de expertos del 2026-11-19, (c) `03-diseno-solucion.md` §5.1.

## Principio
Bronze = zona de aterrizaje. **Capturar todo, no transformar nada.** Dato tal cual llega, para máxima fidelidad y reproceso.

## Lo que pide la sala (literal resumido)
- Ingerir crudo, sin limpieza ni transformación; "solo subir los archivos crudos".
- **Todas las columnas como STRING** (varios expertos de la sala coinciden).
- Saber **cuál es la fuente y cada cuánto se actualiza** (frecuencia).
- Definir: qué **sistemas se migran**, **frecuencia**, **formato**, **carpeta de destino**.
- **Metadatos de trazabilidad**: origen, fecha de ingesta, tipo (carga), y reglas básicas de validación.
- Sumar a lo que el proyecto de referencia no tiene: frecuencia de actualización, **reglas de retención y seguridad**, **sistemas a migrar**, **validaciones mínimas antes de la carga**.

## Lo que ya hace la referencia (conservar)
- Esquema **declarado**, no inferido (la referencia usa `inferSchema`; nosotros declaramos todo STRING).
- Auto Loader con **checkpoint**: cada archivo se carga exactamente una vez.
- Metadata de ingesta: `file_name`, `file_size`, `read_timestamp`, tipo de carga.
- `_rescued_data` para lo que no encaja en el esquema.
- **Tabla de paso (staging) con solo el lote nuevo** para la carga incremental (`bronze.staging_<fuente>`).

## Decisiones de diseño ya tomadas (D-nuevas, a registrar)
- Bronze **append-only** también en dimensiones (la referencia hacía `overwrite`); historial inmutable.
- Los archivos de landing **no se mueven** a `processed/`; el checkpoint es la fuente de verdad.
- Configuración como **datos**: `ops.source_registry` (sistema, compañía, fuente, formato, frecuencia, ruta landing, columnas esperadas, tipo de carga, retención).

## Propuestas a validar con los expertos
| Tema | Propuesta |
|---|---|
| Sistemas a migrar | `madre_erp` (simulado) y `hija_export` (CSV de la empresa adquirida) |
| Frecuencia | customers/products: snapshot **diario**; prices: **mensual**; orders: **diario incremental** (un archivo por día) |
| Formato | CSV con encabezado, UTF-8, separador `,` |
| Carpeta | `landing/<compania>/<fuente>/ingest_date=YYYY-MM-DD/<archivo>.csv` |
| Retención landing | 90 días (política de ciclo de vida del storage; **propuesta, se aplica con confirmación**) |
| Retención bronze | histórico completo; `delta.logRetentionDuration=30 days`, `deletedFileRetentionDuration=7 days` |
| Seguridad | acceso solo por Unity Catalog (credencial del Access Connector); sin blob público; TLS 1.2; permisos por grupo |
| Validaciones mínimas pre-carga | archivo no vacío; extensión `.csv`; nombre cumple patrón; encabezado == columnas esperadas del registro; tamaño > 0; carpeta `ingest_date` válida. Los archivos que fallan se registran en `ops.landing_rejects` y **no** entran a bronze |
| Metadatos | `_company`, `_source_system`, `_source`, `_source_file`, `_file_size`, `_file_modification_time`, `_ingest_ts`, `_ingest_date`, `_batch_id`, `_load_type` (`full`/`incremental`), `_rescued_data` |

## Fuera de alcance de Bronze
Tipado, deduplicación, corrección de typos, parseo de fechas, joins, reglas de negocio: eso es Silver.
