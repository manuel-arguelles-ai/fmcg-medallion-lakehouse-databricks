# Gold — implementación

Estado: **código escrito y probado localmente en su parte pura; nada ejecutado en Azure/Databricks ni en Spark** (no hay Java local).
Diseño: `03-diseno-solucion.md` §5.3–5.4. Insumo: Silver (`08-silver-implementacion.md`).

## 1. Qué se construyó

| Pieza | Archivo | Notas |
|---|---|---|
| Lógica pura | `src/gold/model.py` | calendario (`date_rows`), vigencia de precio, precio del mes anterior, features ML con lags de calendario, `top_n`/`shares` (oráculos de las vistas) |
| DDL + vistas | `src/gold/ddl.py` → `src/gold/sql/01_gold_ddl.sql` (**NO EJECUTADO**) | 7 tablas `gold.*`, `ops.gold_runs`, 6 vistas de KPI; el `.sql` se genera del código y un test comprueba que coinciden y que `infra/run_sql.py` lo separa en las mismas 14 sentencias |
| Transformaciones Spark | `src/gold/transform.py` | una función por tabla; recarga completa con `INSERT OVERWRITE`; reconciliación y registro en `ops.gold_runs` |
| Entrypoint | `src/gold/run.py` | `--catalog --task (setup\|<tabla>\|all) [--skip-setup] [--run-id]`; ruta vía `co_filename` (sin depender de `__file__`) |
| Job | `resources/gold_job.yml` | `gold_build`: `setup` → `dim_date` ‖ `dim_customer` ‖ `dim_product` → `dim_price` (← `dim_product`) → `fact_sales` (← 4 dims) → `agg_sales_monthly` → `feature_sales_monthly`. **No enlazado** a `databricks.yml` |
| Pruebas | `tests/test_gold_*.py` | 31 nuevas (132 al cierre de Gold; 164 hoy con todo el repo) |

### Modelo

| Tabla | Grano / clave única | Origen | Notas |
|---|---|---|---|
| `dim_date` | día · `date_sk` (yyyymmdd) | generada en el driver con `model.date_rows` | años completos que cubren `silver.orders` y `silver.prices`; atributos en español; ISO (lunes = 1); miembro `-1` |
| `dim_customer` | `customer_sk` · (`company`, `customer_id`) | `silver.customers` | SCD1; `customer_sk = xxhash64(customer_key)`; miembro `-1` único (`company = 'Unknown'`) |
| `dim_product` | `product_sk` · (`company`, `product_id`) | `silver.products` | SCD1; `product_sk = xxhash64(product_key)`; miembro `-1` |
| `dim_price` | `price_sk` · (`company`, `product_id`, `valid_from`) | `silver.prices` + `gold.dim_product` | vigencia `valid_from` = 1.º del mes, `valid_to` = fin del mismo mes; `prev_unit_price` y `price_change_pct` del mes calendario anterior; miembro `-1` |
| `fact_sales` | línea de pedido · (`company`, `order_id`, `line_no`) | `silver.orders` + 4 dims | `line_amount` **se reutiliza de Silver** (D28), no se recalcula; `price_sk` = precio vigente en `order_date` (join por rango); `CLUSTER BY (order_month_start, product_sk)` |
| `agg_sales_monthly` | mes × compañía × cliente × producto × moneda | `gold.fact_sales` | `order_lines`, `orders` (distintos, **no aditivo**), `total_qty`, `revenue`, `lines_without_price` |
| `feature_sales_monthly` | igual que el agregado | `gold.agg_sales_monthly` | ML: lags 1–3 de ingreso y cantidad por **mes calendario**, `revenue_avg_3m` (sin el mes actual), `months_since_prev_purchase`, `purchase_month_seq`, etiqueta `target_revenue_next_month` |

Vistas (`gold.v_kpi_*`): `sales_monthly` (ingresos, cantidad, pedidos, ticket por mes y compañía), `top10_products` (por año y `Total`, `RANK`), `category_mix` (participación por división/categoría), `company_comparison` (madre vs hija: participación del mes y variación mensual), `data_quality` (última corrida de Silver por entidad desde `ops.dq_results`, con la descripción de cada regla de `silver/rules.py` embebida), `quarantine_summary` (conteo por entidad, compañía y regla).

### Pregunta de negocio → objeto Gold → consumidor

Cada tabla y vista lleva esta pregunta y su consumidor en su `COMMENT` de Unity Catalog (formato `... Responde: <pregunta>. Consumidor: <BI|ML|AI>`), generado desde `ddl.BUSINESS` en `src/gold/ddl.py`. Consumidores: **BI** = dashboard AI/BI `FMCG Sales Insights`; **AI** = espacio Genie `FMCG Sales Genie`; **ML** = entrenamiento de modelos.

| Pregunta de negocio | Objeto Gold | Tipo | Consumidor |
|---|---|---|---|
| ¿Cómo se filtran y agrupan las ventas por año, trimestre, mes, día de semana o fin de semana? | `dim_date` | tabla | BI, AI |
| ¿Quién compra: cliente, ciudad, mercado y canal de cada venta? | `dim_customer` | tabla | BI, AI |
| ¿Qué se vende: producto, variante, categoría y división? | `dim_product` | tabla | BI, AI |
| ¿Cuál era el precio vigente de un producto en un mes y cuánto cambió respecto del mes anterior? | `dim_price` | tabla | BI, ML |
| ¿Cuánto se vendió (ingreso, cantidad, pedidos) por día, cliente, producto y compañía? | `fact_sales` | tabla | BI, AI, ML |
| ¿Cuánto vendió cada par cliente-producto en cada mes? (base del top 10 y del mix) | `agg_sales_monthly` | tabla | BI, AI, ML |
| ¿Cuánto comprará un cliente de un producto el mes siguiente? (etiqueta de entrenamiento) | `feature_sales_monthly` | tabla | ML |
| ¿Cómo evolucionan ingresos, cantidad, pedidos y ticket promedio mes a mes por compañía? | `v_kpi_sales_monthly` | vista | BI, AI |
| ¿Qué 10 productos generan más ingresos por año y en todo el período? | `v_kpi_top10_products` | vista | BI, AI |
| ¿Qué peso tiene cada división y categoría en los ingresos del mes de cada compañía? | `v_kpi_category_mix` | vista | BI, AI |
| ¿Cómo se compara la hija con la madre: participación, variación mensual y clientes activos? | `v_kpi_company_comparison` | vista | BI, AI |
| ¿Qué reglas de calidad fallaron en la última corrida de Silver y con qué tasa? | `v_kpi_data_quality` | vista | BI, AI |
| ¿Cuántas filas quedaron en cuarentena por entidad, compañía y regla? | `v_kpi_quarantine_summary` | vista | BI, AI |

(`ops.gold_runs` también lo declara, con consumidor `operacion`: qué escribió cada tarea de Gold y si pasó sus reconciliaciones.)

**Comentarios de columna:** todas las columnas de las 7 tablas, de `ops.gold_runs` y de las 6 vistas tienen `COMMENT` (las vistas declaran la lista de columnas con comentario en su `CREATE OR REPLACE VIEW`, porque las columnas calculadas no heredan comentario). Como `CREATE TABLE IF NOT EXISTS` no altera tablas ya creadas, los comentarios se llevan a UC con `src/gold/sql/02_gold_comments.sql` (generado: `cd src && python -m gold.ddl fmcg_lakehouse --comments > gold/sql/02_gold_comments.sql`; solo `COMMENT ON TABLE`, `ALTER TABLE ... ALTER COLUMN ... COMMENT` y las mismas `CREATE OR REPLACE VIEW` de `setup`; idempotente):
```
python3 infra/run_sql.py src/gold/sql/02_gold_comments.sql --warehouse <warehouse-id>
```

**Moneda:** Gold no fija ninguna. Todo monto (`unit_price`, `line_amount`, `revenue`) viaja con la columna `currency` que viene de Silver (hoy `PEN`), y todas las agregaciones agrupan también por `currency`; un test verifica que el SQL no contiene `INR`/`PEN`/`USD`. El nombre `total_amount_inr` del diseño §5.3 se descarta.

## 2. Supuestos y decisiones propuestas (para registrar en `02-decisiones.md`)

| # | Propuesta |
|---|---|
| G1 | **SCD1** en clientes y productos (estado actual de Silver). SCD2 queda como decisión abierta: Silver ya tiene CDF en `customers`/`products`, que es el insumo natural si se decide |
| G2 | Claves sustitutas = `xxhash64` de la clave de negocio: estables entre recargas sin secuencias ni `IDENTITY` (que cambiarían al reconstruir). Cada recarga verifica unicidad de la clave sustituta (incluye que ninguna choque con `-1`) |
| G3 | Un **único** miembro `-1` por dimensión (no uno por compañía como en Silver): la compañía de la venta vive en `fact_sales.company` |
| G4 | **Recarga completa** (`INSERT OVERWRITE`) por tabla: idempotente y reconstruible desde Silver; conserva el DDL (comentarios, `CLUSTER BY`). Con ~1 M de líneas es barato; el incremental por meses afectados (`replaceWhere`) es el siguiente paso si hace falta |
| G5 | Vigencia de precio = el mes del precio, **sin estirarla** hasta el siguiente precio: así Gold coincide con Silver (si falta el precio del mes, `line_amount` es NULL y `price_missing`; el `price_sk` queda `-1`) |
| G6 | Grano diario en el hecho + agregado mensual derivado (como proponía §5.3); `fact_orders` y `fact_sales` se fusionan en uno solo (`fact_sales` ya tiene cantidad, claves y monto) |
| G7 | Las ventas con cliente desconocido (`-1`) **cuentan** en ingresos (coherente con D19 de Silver); `v_kpi_company_comparison` muestra cuántas líneas son de cliente desconocido |
| G8 | Una tarea `setup` crea tablas y vistas antes que el resto (evita `CREATE` concurrentes entre tareas paralelas) |
| G9 | Tabla de features ML con lags simples (costó poco). Solo hay filas en meses con compra (panel **no denso**); los lags lo compensan buscando por mes calendario (mes ausente = 0) |

## 3. Cómo se corre (pendiente de Manu, con confirmación)

Prerrequisitos: Silver cargado (`silver_transform` en modo `full`) y su DDL aplicado. Ninguno de estos comandos se ejecutó.

```
cd /Users/macdenix/fmcg-medallion-lakehouse-azure
export DATABRICKS_HOST=https://<workspace-host> DATABRICKS_AUTH_TYPE=azure-cli DATABRICKS_BUNDLE_ENGINE=direct
# 1) revisar el DDL sin ejecutarlo
python3 infra/run_sql.py src/gold/sql/01_gold_ddl.sql --warehouse <warehouse-id> --dry-run
# 2) aplicar el DDL (crea tablas gold.*, ops.gold_runs y vistas) — opcional: la tarea setup hace lo mismo
python3 infra/run_sql.py src/gold/sql/01_gold_ddl.sql --warehouse <warehouse-id>
# 3) enlazar el job: en databricks.yml agregar resources/gold_job.yml a include y src/gold/** a sync.include
databricks bundle validate -t dev
databricks bundle deploy -t dev
# 4) correr Gold
databricks bundle run gold_build -t dev
```

Verificación posterior (lectura): `SELECT * FROM fmcg_lakehouse.ops.gold_runs ORDER BY started_at DESC` (todas `ok`, con `checks`), y que `fact_sales` = filas de `silver.orders` (madre 699.699 + hija 296.754 según `docs/08` §5) y `amount_without_price_sk=0`.

## 4. Qué quedó validado y qué no

**Validado localmente (pytest):**
- `dim_date` completo (se construye con la misma función que prueba pytest): rango por años completos, bisiestos, semana ISO, fin de mes, miembro `-1`, columnas en el orden del DDL.
- Vigencia de precio y precio del mes anterior con huecos de calendario; `pct_change` con bordes.
- Features: lags de calendario con huecos, sin mezclar grupos, etiqueta NULL cuando el mes siguiente no se conoce, ingreso NULL como 0.
- `top_n` con empates (semántica `RANK`), participaciones.
- DDL: `.sql` sincronizado con el generador y separable por `run_sql.py`; solo `CREATE ... IF NOT EXISTS`/`CREATE OR REPLACE VIEW` en `gold`/`ops.gold_runs`; columnas copiadas de Silver existen en el DDL de Silver con el mismo tipo; sin moneda fija; columnas citadas en las vistas existen; todas las reglas de Silver embebidas en la vista de calidad.
- Job: tareas = tablas + `setup`, dependencias = `transform.DEPENDS`, parámetros.
- Estática de `transform.py`: compila, sin RDD/caché/`toPandas`, sin escribir en Silver/Bronze, cada tabla reconcilia y registra.

**Riesgos no probados en Spark (mirar primero en la 1.ª corrida):**
- `INSERT OVERWRITE TABLE ... SELECT * FROM <vista temporal>` por posición sobre tablas con `CLUSTER BY`; los casts de `_conform` con ANSI (p. ej. una `price_change_pct` fuera de `DECIMAL(12,4)` haría fallar la tarea).
- `createDataFrame` de `dim_date` con esquema DDL en texto (backticks, `DATE` con `None` en la fila `-1`).
- `unionByName` del miembro `-1` (literales `INT` contra `BIGINT` de `xxhash64`: se espera ensanchamiento).
- Ventanas `rangeBetween(k, k)` sobre el índice de mes (lags de calendario) y `F.lag`/`row_number` en la misma ventana.
- Join por rango `order_date BETWEEN valid_from AND valid_to` con `broadcast` (~1.000 precios).
- Vistas: `COUNT_IF`, `SUM(SUM(x)) OVER (...)`, `FROM VALUES ... AS r(...)` dentro de un CTE de vista.
- `ops.gold_runs`: `createDataFrame` con `datetime` con zona horaria.
- Rendimiento: `fact_sales` recalcula desde Silver completo cada vez (1 M de filas; esperado < 2 min en serverless, sin medir).

## 5. Riesgos y limitaciones

- Gold depende de que Silver esté al día; la dependencia entre jobs (Silver → Gold) no está cableada (H6).
- `INSERT OVERWRITE` deja la tabla momentáneamente con la versión nueva completa (atómico por tabla), pero entre tareas el modelo puede estar mezclado si una falla a mitad: reintentar el job completo deja todo coherente.
- `agg_sales_monthly.orders` no se puede sumar entre productos/clientes: para pedidos totales usar `v_kpi_sales_monthly` (cuenta distintos desde `fact_sales`).
- `xxhash64` tiene probabilidad de colisión despreciable (~10⁻¹³ con miles de filas); si ocurre, la recarga falla con un mensaje claro (no corrompe datos).
- La vista de calidad toma la última corrida por **entidad** (máximo `ts`); si una corrida se hace con `--company hija`, la "última" solo trae la hija.

## 6. Preguntas abiertas (expertos / Manu)

1. **SCD2** en clientes/productos: ¿desde el CDF de Silver o snapshot en Gold? ¿Qué atributos disparan versión (ciudad, categoría)?
2. ¿`line_amount` congelado en Silver (hoy) o recalculado en Gold con `dim_price`? Con G5 ambos dan lo mismo; cambiaría si la vigencia se estirara a meses sin precio.
3. Ventas con cliente desconocido: ¿cuentan en ingresos (hoy sí) o se excluyen de los KPI?
4. **Moneda**: hoy PEN (decisión de Manu de no pasar a USD por ahora). Si llegara multimoneda, Gold ya agrupa por `currency`; faltaría una tabla de tipos de cambio para un total consolidado.
5. Metric views de Unity Catalog en lugar de vistas SQL para Genie.
6. Incremental de Gold (por meses afectados) vs recarga completa: ¿cuándo justifica la complejidad?
7. Features ML: ¿panel denso (fila con 0 en meses sin compra)? ¿horizonte de predicción (1 mes) y grano (cliente × producto vs cliente)?

## 7. Siguientes pasos

1. Ejecutar DDL y job (§3) y validar los riesgos de §4.
2. Cálculo independiente de KPIs en Python sobre los CSV de datagen (criterio de salida de H5): reutilizar `silver.expected` para obtener las líneas limpias y comparar ingresos por compañía × año contra `v_kpi_sales_monthly`.
3. Prueba de idempotencia: correr `gold_build` dos veces y comparar conteos/sumas (deben ser idénticos; las claves sustitutas también).
4. Dashboard y Genie (H7) sobre `gold.v_kpi_*`.

## 8. Verificación independiente de KPIs

`scripts/verify_gold_kpis.py` calcula los KPI en Python puro desde `data/generated` (reglas de `src/silver` + dedup, misma lógica que `silver.expected`; sin Spark ni Gold) y los compara con Gold vía Statement Execution API (solo `SELECT`, warehouse `<warehouse-id>`, tolerancia 0.01; razones 1e-6; sale con código ≠ 0 si difiere). Pruebas puras: `tests/test_verify_gold_kpis.py` (10).

Resultado (19-nov-2026, Silver y Gold ya cargados): **coinciden todos los chequeos**.

| KPI | Filas comparadas | Resultado |
|---|---|---|
| Total `fact_sales` (líneas / ingreso / cantidad) | 996.453 / 37.450.346,40 / 3.245.701 en ambos lados | OK |
| Ingresos, cantidad, líneas, pedidos y líneas sin precio por (company, mes) — `v_kpi_sales_monthly` | 48 | OK |
| Top 10 productos por ingreso, por año y Total (rank, ingreso, cantidad) — `v_kpi_top10_products` | 30 | OK |
| Mix por categoría (ingreso, cantidad, participación) — `v_kpi_category_mix` | 264 | OK |
| Comparativo madre vs hija (participación y variación mensual) — `v_kpi_company_comparison` | 48 | OK |

Ingreso por compañía y año (esperado = Gold): madre 2024 13.280.110,26 · madre 2025 14.248.977,29 · hija 2024 4.791.700,43 · hija 2025 5.129.558,42. Con esto se cierra el criterio de salida de H5; Gold reproduce las reglas de Silver sin diferencias.
