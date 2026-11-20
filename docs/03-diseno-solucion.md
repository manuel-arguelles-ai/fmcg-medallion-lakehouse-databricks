# Diseño de la solución (v0 — propuesta para validar con los expertos)

Estado: borrador. Cada capa tiene una **propuesta base** y las **decisiones abiertas** que se cierran con los data engineers asesores y se registran en `02-decisiones.md`.

## 1. Escenario de negocio

Una empresa **madre** de consumo masivo (FMCG) adquiere una empresa **hija** que vende por un canal distinto (bares deportivos). La hija entrega sus datos en CSV con calidad irregular. El lakehouse debe:

1. Ingerir y conservar el dato crudo de **ambas** compañías (madre e hija).
2. Limpiarlo, conformarlo y **consolidarlo** en un único modelo dimensional.
3. Servir KPIs comerciales (ingresos, cantidad, top productos, mix por categoría) en un dashboard y en un espacio Genie.

Dominio propio (no clon): mantenemos FMCG con datos 100 % sintéticos y reproducibles.

## 2. Arquitectura objetivo

```
 generador sintético (semilla fija)
        │  CSV
        ▼
 ADLS Gen2  landing/<compañía>/<fuente>/ingest_date=YYYY-MM-DD/*.csv      (external volume UC)
        │  Auto Loader (cloudFiles, trigger availableNow)
        ▼
 BRONZE  fmcg_lakehouse.bronze.*   append-only, esquema tal cual llega + metadatos + _rescued_data
        │  reglas de calidad + conformación
        ├──────────────► fmcg_lakehouse.quarantine.*   (filas rechazadas + motivo)
        ▼
 SILVER  fmcg_lakehouse.silver.*   limpio, tipado, deduplicado, claves conformadas madre+hija
        │  modelado dimensional
        ▼
 GOLD    fmcg_lakehouse.gold.*     estrella: dim_* + fact_* + agregados + vistas de KPI
        │
        ├──► Databricks SQL Dashboard
        └──► Genie space
```

Orquestación: **Databricks Workflows** definido como código con **Asset Bundles** (`databricks.yml`). Cómputo: serverless (sin clusters permanentes y sin NAT).

## 3. Plataforma (ya aprovisionada)

Ver `01-inventario-azure.md`. Pendiente dentro de Databricks (se ejecuta con confirmación comando a comando):

| Objeto UC | Detalle |
|---|---|
| Storage credential | basada en el Access Connector `ac-fmcg-lakehouse-dev-cus-001` |
| External locations | una por contenedor: `landing`, `bronze`, `silver`, `gold`, `uc-managed` |
| Catálogo | `fmcg_lakehouse`, managed location en `uc-managed` |
| Esquemas | `bronze`, `silver`, `gold`, `quarantine`, `ops` (auditoría y métricas de calidad) |
| Volume externo | `fmcg_lakehouse.bronze.landing` sobre el contenedor `landing` |

Permisos: grupos `de_engineers` (escritura) y `analysts` (solo lectura en gold), aunque al inicio solo exista el usuario admin.

## 4. Modelo de datos fuente

Dos compañías × cuatro fuentes. Mismas entidades, distinto formato y calidad.

| Fuente | Madre | Hija (anomalías sembradas) |
|---|---|---|
| customers | limpio | duplicados, espacios, mayúsculas/minúsculas mixtas, typos de ciudad, ciudades nulas |
| products | limpio | duplicados, categoría en minúsculas, typo recurrente en un término, `product_id` no numérico |
| prices (mensual) | limpio | 4 formatos de fecha, precios negativos o no numéricos |
| orders | limpio | 4 formatos de fecha (incluye día de semana en texto), `customer_id` inválido, `order_qty` nulo, duplicados, llegada incremental por archivos |

Volumen sintético inicial: ~1 M de líneas de pedido en 24 meses, suficiente para que particionado, OPTIMIZE y el incremental tengan sentido real.

## 5. Diseño por capa

### 5.1 Landing → Bronze
**Propuesta base**
- Auto Loader con `availableNow`, esquema leído como **string** (sin `inferSchema`) para no perder datos por tipado temprano; columna `_rescued_data` activa.
- Metadatos: `_ingest_ts`, `_source_file`, `_file_size`, `_batch_id`, `_company`.
- **Append-only** en todas las fuentes (también dimensiones): bronze es el historial inmutable. Nada de `overwrite`.
- Tras procesar, los archivos **no se mueven**; el estado de Auto Loader (checkpoint) es la fuente de verdad de "ya procesado".
- Change Data Feed activado solo donde alimenta SCD2.

**Decisiones abiertas (expertos)**: Auto Loader vs `COPY INTO`; política de evolución de esquema (`rescue` vs `addNewColumns`); retención y `VACUUM`; particionado de bronze de pedidos (por `ingest_date`).

### 5.2 Bronze → Silver
**Propuesta base**
- **Deduplicación determinista**: `row_number()` por clave de negocio ordenando por `_ingest_ts` desc (gana el registro más reciente), no `dropDuplicates` arbitrario.
- **Calidad con cuarentena**: cada regla produce un flag; las filas que fallan van a `quarantine.<tabla>` con `rule_id`, `reason`, `_batch_id`. No se corrigen silenciosamente ni se descartan sin rastro.
- **Corrección vs rechazo**:
  - Se corrige lo determinista (trim, initcap, typos mapeados desde una **tabla de referencia** versionada, no diccionarios en código).
  - Se rechaza lo irrecuperable (fecha no parseable, `order_qty` nulo).
  - Lo ambiguo se conserva marcado (p. ej. precio negativo → `price_was_negative = true` en lugar de convertirlo sin avisar).
- **Claves conformadas**: clave natural + `company` → clave de negocio única. Los huérfanos se asignan a un **miembro desconocido explícito** (`-1`, "Unknown") que existe en la dimensión, en lugar de `999999` que colisiona y no existe.
- **Pedidos huérfanos** (producto inexistente): `LEFT JOIN`, no `INNER`; los que no resuelven se registran en cuarentena y se cuentan en `ops`.
- Silver incremental con `MERGE` idempotente: reprocesar el mismo lote no duplica.

**Decisiones abiertas (expertos)**: Lakeflow Spark Declarative Pipelines con *expectations* vs notebooks/jobs con reglas propias; SCD2 en clientes y productos desde silver o solo en gold; qué se corrige y qué se rechaza regla por regla (tabla de reglas `docs/reglas-calidad.md`).

### 5.3 Silver → Gold
**Propuesta base**: esquema en estrella con dimensiones conformadas para ambas compañías.

| Tabla | Grano | Notas |
|---|---|---|
| `dim_date` | día (con mes, trimestre, año, fin de semana) | el original solo tenía grano mensual |
| `dim_customer` | cliente | SCD2 (`valid_from`, `valid_to`, `is_current`), clave sustituta, miembro `-1` |
| `dim_product` | producto | SCD2, `division`, `category`, `variant`, miembro `-1` |
| `dim_price` | producto × vigencia | precio **con vigencia** (efectivo desde/hasta): conserva el historial que la referencia pisaba |
| `fact_orders` | línea de pedido (diario) | cantidad y claves; fuente de verdad |
| `fact_sales` | línea de pedido | `total_amount_inr = qty × precio vigente en la fecha del pedido` |
| `agg_sales_monthly` | mes × producto × cliente | agregado derivado de `fact_sales`, reconstruible; es lo que consume el dashboard |
| Vistas de KPI | — | ingresos, cantidad, top 10 productos, mix por categoría, comparativo madre vs hija |

Optimización: `OPTIMIZE` + clustering líquido (`CLUSTER BY`) en hechos en lugar de Z-order manual; estadísticas y `ANALYZE TABLE`.

**Decisiones abiertas (expertos)**: grano único diario con agregado derivado (propuesto) vs hecho mensual como la referencia; modelado de precio (vigencia vs tabla de hechos de precio); moneda única o multimoneda (**vigente: PEN, D26**); metric views de Unity Catalog para los KPI.

### 5.4 Serving
- Dashboard de Databricks SQL equivalente al "Sales Insights" (filtros año / trimestre / mes, ingresos, cantidad, top 10 productos, mix por categoría) **más** comparativo madre vs hija y panel de calidad (filas rechazadas por regla).
- Espacio Genie sobre `gold` con descripciones de columnas y preguntas de ejemplo.
- SQL Warehouse serverless con auto-stop corto.

## 6. Calidad, observabilidad y pruebas

- Tabla `ops.dq_results`: por corrida, regla, filas evaluadas / fallidas.
- Tabla `ops.pipeline_runs`: conteos por capa (landing → bronze → silver → gold) con reconciliación; si no cuadra, el job falla.
- Pruebas unitarias de las transformaciones puras (pytest + PySpark local o `databricks-connect`) y pruebas de contrato de esquema.
- Prueba de **idempotencia**: correr dos veces el mismo lote deja las tablas iguales.
- Prueba de **incremental**: un segundo lote solo modifica los meses afectados.

## 7. Estructura propuesta del repo

```
fmcg-medallion-lakehouse-azure/
├── databricks.yml              # Asset Bundle (jobs, pipelines, dashboards)
├── infra/                      # aprovisionamiento Azure (+ SQL de Unity Catalog)
├── src/
│   ├── datagen/                # generador sintético reproducible
│   ├── bronze/  silver/  gold/ # transformaciones
│   └── common/                 # reglas de calidad, utilidades
├── tests/
├── docs/                       # análisis, decisiones, diseño, reglas de calidad, diagramas
└── .github/workflows/          # CI (cuando haya remoto)
```

## 8. Hoja de ruta (cada hito = commits trazables)

| Hito | Contenido | Criterio de salida |
|---|---|---|
| H0 | Infra Azure + diseño | **hecho** (infra), diseño en validación |
| H1 | Unity Catalog: credencial, external locations, catálogo, esquemas, volume | `SHOW` de cada objeto OK; escritura de prueba en cada contenedor |
| H2 | Generador sintético + carga a `landing` | conteos y anomalías sembradas verificadas |
| H3 | Bronze con Auto Loader | reconciliación landing = bronze; reproceso sin duplicados |
| H4 | Silver + cuarentena + `ops` | reglas aprobadas por expertos; conteos cuadran |
| H5 | Gold estrella + KPIs | KPIs coinciden con cálculo independiente en Python |
| H6 | Orquestación (Workflow/Bundle) + incremental | corrida end-to-end y corrida incremental verdes |
| H7 | Dashboard + Genie + capturas | dashboard publicado |
| H8 | README final, diagramas, publicación a GitHub | repo reproducible desde cero |

## 9. Costos (Free Trial)

- Sin NAT gateway (decisión de Manu); nodos clásicos con IP pública si se usan clusters; preferencia por **serverless**.
- Storage LRS estándar; un warehouse con auto-stop; sin recursos always-on más allá del propio workspace.
- Revisión de costo acumulado al cerrar cada hito.

## 10. Riesgos

| Riesgo | Mitigación |
|---|---|
| Metastore compartido con otros trabajos | no tocar objetos ajenos; nombres con prefijo propio; documentado |
| Cuota o tipo de cómputo en Free Trial | validar serverless en H1; plan B cluster pequeño |
| Cambios de criterio de los expertos en vivo | `02-decisiones.md` como registro; reglas en tablas de referencia, no en código |
| Datos sintéticos poco realistas | semilla fija + distribuciones documentadas + revisión de expertos |

## 11. Qué necesito para arrancar H1

1. Tu visto bueno al **diseño base** (o los cambios que digan los expertos).
2. Confirmación para crear en Databricks, comando a comando: storage credential, 5 external locations, catálogo `fmcg_lakehouse`, 5 esquemas y el volume.
