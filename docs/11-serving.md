# Serving — dashboard AI/BI y espacio Genie (H7)

Estado: **creado y verificado en el workspace dev** el 2026-11-20 (UTC); actualizado el mismo día ~03:45 UTC (widget de variación mensual, dashboard en el bundle, comentarios de columnas en UC, 7 preguntas de Genie comprobadas con el lote incremental). Diseño: `03-diseno-solucion.md` §5.4. Insumo: tablas y vistas `fmcg_lakehouse.gold.*` (`09-gold-implementacion.md`).

## 1. Qué se creó

| Recurso | Nombre | ID | URL |
|---|---|---|---|
| Dashboard AI/BI (Lakeview), publicado con credenciales embebidas; recurso del bundle | `[dev <user>] FMCG Sales Insights` (el target `dev` del bundle antepone el prefijo) | `<dashboard-id>` | publicado: https://<workspace-host>/dashboardsv3/<dashboard-id>/published · borrador: https://<workspace-host>/sql/dashboardsv3/<dashboard-id> |
| Espacio Genie | `FMCG Sales Genie` | `<genie-space-id>` | https://<workspace-host>/genie/rooms/<genie-space-id> |

Ambos en `/Users/<owner-email>`, sobre el SQL Warehouse `<warehouse-id>` (Serverless Starter Warehouse, auto-stop 10 min). El dashboard se despliega con el bundle (`resources/dashboard.yml`, §5); Genie con su script.

### Archivos

| Archivo | Rol |
|---|---|
| `dashboards/fmcg_sales_insights.lvdash.json` | dashboard serializado (fuente que se despliega) |
| `resources/dashboard.yml` | recurso `dashboards.fmcg_sales_insights` del bundle, enlazado al dashboard existente |
| `dashboards/build_fmcg_sales_insights.py` | generador del JSON (datasets, widgets, posiciones) |
| `infra/03-deploy-dashboard.py` | crea o actualiza (PATCH si existe un dashboard activo con el mismo nombre en la carpeta), publica y opcionalmente verifica cada dataset por la Statement API; `--export` baja el borrador al JSON tras editar en la UI |
| `genie/fmcg_sales_genie.json` | definición legible del espacio: tablas, descripciones de columnas, instrucciones, preguntas y SQL de ejemplo |
| `infra/04-deploy-genie.py` | convierte la definición al `serialized_space`, crea o actualiza (PATCH por título) y `--ask` prueba preguntas por la API de conversaciones |
| `docs/img/*.png` | capturas (sección 6) |

## 2. Dashboard `FMCG Sales Insights`

**Página "Ventas"**
- Filtros (selección única, valor por defecto `Todos`): año, trimestre, mes, compañía.
- Contadores: ingresos (PEN), cantidad vendida, pedidos.
- Línea: tendencia mensual de ingresos madre vs hija.
- Barras: top 10 productos por ingresos (responde a los 4 filtros; `RANK`, los empates comparten puesto).
- Dona: mix de ingresos por categoría.
- Barras: comparativo madre vs hija (ingresos) y participación mensual apilada (`revenue_share_month`).
- Línea: **variación mensual de ingresos** madre vs hija (`revenue_mom_pct` de `v_kpi_company_comparison`, formato %; responde a los 4 filtros). Junto con la participación cubre el comparativo del diseño §5.4.

**Página "Calidad de datos"**
- Contadores: filas en cuarentena, reglas con filas fallidas, filas marcadas/corregidas/rechazadas.
- Barras y tabla de cuarentena por regla (`v_kpi_quarantine_summary`).
- Tabla de reglas con `rows_failed > 0` en la última corrida de Silver por entidad (`v_kpi_data_quality`), con descripción, severidad, acción y tasa de fallo.

**Datasets** (todos leen `gold`; ninguno escribe):

| Dataset | Fuente | Filtros |
|---|---|---|
| `ds_ventas_mes` | `v_kpi_sales_monthly` | sí |
| `ds_categoria` | `agg_sales_monthly` ⋈ `dim_product`, agregado a mes × compañía × categoría | sí |
| `ds_top10` | `agg_sales_monthly` ⋈ `dim_product`, filtra y luego rankea (`QUALIFY rank <= 10`) | sí |
| `ds_comparativo` | `v_kpi_company_comparison` | sí |
| `ds_f_anio`, `ds_f_trimestre`, `ds_f_mes`, `ds_f_compania` | valores distintos + `Todos` | — (llenan los desplegables) |
| `ds_calidad` | `v_kpi_data_quality WHERE rows_failed > 0` | no |
| `ds_cuarentena` | `v_kpi_quarantine_summary` | no |

**Diseño de filtros (decisión):** los filtros son **parámetros por consulta** (`:p_anio`, `:p_trimestre`, `:p_mes`, `:p_compania`, por defecto `'Todos'`) aplicados en el `WHERE` de los cuatro datasets de ventas. Motivo: el top 10 debe recalcular el ranking dentro del período filtrado, y eso no se logra con filtros por campo (filtran filas, no recalculan el `RANK`). Mezclar filtro por campo y parámetro en el mismo widget no funcionó: con valor por defecto `Todos` el filtro por campo aplicaba `year IN ('Todos')` (error de cast) y con valor vacío el visor reportaba *Conflicting selections for parameters*. Con parámetros en todos los datasets los filtros son coherentes entre widgets.

### Verificación (2026-11-20 ~03:00–03:15 UTC)
- `python3 infra/03-deploy-dashboard.py --warehouse <warehouse-id> --verify`: los 12 datasets ejecutan por la Statement API con sus parámetros por defecto (`ds_ventas_mes` 48 filas, `ds_categoria` 264, `ds_top10` 10, `ds_comparativo` 48, filtros 3/5/13/3, `ds_calidad` 19, `ds_cuarentena` 2).
- Visto renderizado en el navegador (sesión ya iniciada del usuario): todos los widgets de las dos páginas se dibujan sin error.
- Números contra SQL directo hecho en el momento:
  - Sin filtros: ingresos 37,45 M (SQL: `SUM(line_amount)` de `fact_sales` = 37.450.346,40 PEN sobre 996.453 líneas), cantidad 3,25 M, pedidos 420,3 k.
  - Filtro Año = 2024: ingresos 18,07 M, cantidad 1,62 M, pedidos 210,12 k; SQL sobre `v_kpi_sales_monthly WHERE year = 2024`: 18.071.810,69 / 1.622.158 / 210.117.
  - Top 10 sin filtros: encabeza "Papas Andinas (Original 150g) (madre)".
  - Calidad: cuarentena 4,66 K = 4.633 (`ORD_03`, hija) + 24 (`PRC_05`, hija); 17 reglas con filas fallidas.
- Segunda corrida del script: hace `PATCH` sobre el mismo `dashboard_id` (no duplica).

### Verificación del widget de variación mensual (2026-11-20 ~03:35 UTC)
- Antes de desplegar se bajó el borrador (`--export` a un archivo temporal) y era idéntico al JSON de `HEAD`: no había cambios hechos en la UI que se perdieran.
- `python3 infra/03-deploy-dashboard.py --warehouse <warehouse-id> --verify`: los 12 datasets OK (`ds_comparativo` 48 filas) y `PATCH <dashboard-id>` + publicado (mismo id).
- Valores que dibuja el widget (SQL directo sobre `v_kpi_company_comparison`): dic-2025 madre +1,73 % · hija +10,19 %; nov-2025 madre −5,09 % · hija −3,15 %; oct-2025 madre +8,46 % · hija +2,54 %. El primer mes de cada compañía es `NULL` (no hay mes anterior).
- Datos con el lote incremental: `fact_sales` = 996.475 líneas y 37.450.922,98 PEN (los números de la verificación anterior, 996.453 / 37.450.346,40, son previos a ese lote).

## 3. Espacio Genie `FMCG Sales Genie`

- **Tablas (11):** `fact_sales`, `dim_date`, `dim_customer`, `dim_product`, `agg_sales_monthly` y las 6 vistas `v_kpi_*`. Se excluyen `dim_price` y `feature_sales_monthly` (no aportan a preguntas de negocio y confunden al modelo).
- **Descripciones de columnas:** todas las columnas de las 7 tablas, de las 6 vistas y de `ops.gold_runs` tienen `COMMENT` en Unity Catalog. Se aplicó `src/gold/sql/02_gold_comments.sql` (149 sentencias, todas OK) y luego `information_schema.columns` devolvió **198 columnas en 14 objetos, 0 sin comentario**. Cada tabla y vista declara además en su `COMMENT` la pregunta que responde y su consumidor (`Responde: ... Consumidor: BI|ML|AI`; 13 de 13 en `information_schema.tables`; ver `09` §1). Las descripciones de columnas en el espacio Genie (`column_configs`) se mantienen.
- **Instrucciones:** responder en español; compañías `madre`/`hija`; ingresos = `SUM(line_amount)`/`SUM(revenue)` en la moneda de `currency` (PEN); líneas sin precio no suman ingresos; pedidos con `COUNT(DISTINCT order_id)` y nunca sumando `agg_sales_monthly.orders`; filtros de fecha por `dim_date` o columnas de las vistas; miembro `-1` cuenta en totales pero se excluye de rankings; calidad desde `v_kpi_data_quality`/`v_kpi_quarantine_summary`.
- **SQL de ejemplo en el espacio (3):** ingresos y líneas por compañía, ingresos mensuales por compañía en un año, top productos por año.
- **Preguntas de ejemplo (7):**
  1. ¿Cuáles son los ingresos totales y cuántas líneas de pedido hay?
  2. ¿Cuántos ingresos tuvo cada compañía (madre e hija) en 2025?
  3. ¿Cuáles son los 5 productos con más ingresos en 2024?
  4. ¿Cómo evolucionaron los ingresos mensuales de la hija en 2025?
  5. ¿Qué categoría aporta más ingresos a la madre?
  6. ¿Cuántas filas hay en cuarentena por regla?
  7. ¿Qué reglas de calidad tuvieron filas fallidas en pedidos de la hija?

### Preguntas de ejemplo: SQL de referencia, valor esperado y respuesta de Genie (2026-11-20 ~03:35 UTC)

Datos con el lote incremental (`fact_sales` 996.475 líneas, 37.450.922,98 PEN). Para cada pregunta se ejecutó el SQL de referencia directo por la Statement API (warehouse `<warehouse-id>`) y la misma pregunta en Genie con `infra/04-deploy-genie.py --skip-deploy --ask ...`; las 7 coinciden. Catálogo omitido abajo: todas las tablas son `fmcg_lakehouse.gold.*`.

| # | Pregunta | SQL de referencia | Valor esperado (SQL directo) | SQL de Genie (resumen) | ¿Coincide? |
|---|---|---|---|---|---|
| 1 | ¿Cuáles son los ingresos totales y cuántas líneas de pedido hay? | `SELECT currency, SUM(line_amount), COUNT(*) FROM fact_sales GROUP BY currency` | PEN 37.450.922,98 · 996.475 líneas | igual, sobre `fact_sales` | sí |
| 2 | ¿Cuántos ingresos tuvo cada compañía (madre e hija) en 2025? | `SELECT company, SUM(revenue) FROM v_kpi_sales_monthly WHERE year = 2025 GROUP BY company` | madre 14.248.977,29 · hija 5.130.135,00 | `SUM(revenue)` de `v_kpi_sales_monthly WHERE year = 2025` | sí |
| 3 | ¿Cuáles son los 5 productos con más ingresos en 2024? | `SELECT revenue_rank, company, product_name, revenue FROM v_kpi_top10_products WHERE period = '2024' AND revenue_rank <= 5 ORDER BY revenue_rank` | 1 Papas Andinas (Original 150g) 797.452,50 · 2 Nectar Frutal (Maracuya 1L) 784.868,13 · 3 Galleta Quinua (Vainilla 6u) 726.892,33 · 4 Camote Chips (Miel 120g) 714.493,29 · 5 Nectar Frutal (Mango 1L) 650.169,79 (todos madre) | `v_kpi_top10_products WHERE period = '2024' AND revenue_rank <= 5` | sí |
| 4 | ¿Cómo evolucionaron los ingresos mensuales de la hija en 2025? | `SELECT month_start, revenue, revenue_mom_pct FROM v_kpi_company_comparison WHERE year = 2025 AND company = 'hija' ORDER BY month_start` | ene 419.956,67 · feb 378.473,04 (mín., −9,88 %) · mar 413.911,37 · abr 412.531,87 · may 459.131,70 · jun 437.899,87 · jul 446.797,00 · ago 416.021,58 · sep 424.390,65 · oct 435.169,21 · nov 421.460,60 · dic 464.391,44 (máx., +10,19 %) | `revenue` mensual de `v_kpi_sales_monthly WHERE year = 2025` (hija) | sí (12 meses, mismos montos) |
| 5 | ¿Qué categoría aporta más ingresos a la madre? | `SELECT category, SUM(revenue) FROM v_kpi_category_mix WHERE company = 'madre' GROUP BY category ORDER BY 2 DESC` | Snacks Salados 5.490.985,93 (siguen Bebidas 4.692.264,03 y Lacteos 4.560.454,89) | `SUM(revenue)` de `v_kpi_category_mix` (madre) + `RANK` = 1 | sí |
| 6 | ¿Cuántas filas hay en cuarentena por regla? | `SELECT rule_id, SUM(rows_quarantined) FROM v_kpi_quarantine_summary GROUP BY rule_id` | ORD_03 4.634 · PRC_05 24 | igual | sí |
| 7 | ¿Qué reglas de calidad tuvieron filas fallidas en pedidos de la hija? | `SELECT rule_id, description, action, rows_failed, fail_rate FROM v_kpi_data_quality WHERE entity = 'orders' AND company = 'hija' AND rows_failed > 0 ORDER BY rows_failed DESC` | ORD_06 23 (100 %) · ORD_10 2 (8,7 %) · ORD_03 1 · ORD_07 1 · ORD_11 1 — de la **última corrida** de Silver (incremental `silver_orders_20261120T033019_f957cf`, 23–25 filas evaluadas) | `v_kpi_data_quality` filtrando hija y `entity ILIKE '%order%'` | sí |

Nota sobre la 7: `v_kpi_data_quality` muestra la última corrida por entidad (aquí el lote incremental chico), no el acumulado histórico; es el comportamiento documentado en §5. Los cambios frente a la prueba anterior (ORD_03 4.633 → 4.634; hija 2025 5.129.558,42 → 5.130.135,00) son las filas del lote incremental. No hizo falta tocar las instrucciones del espacio.

Para repetir la prueba (compara siempre contra un SQL directo hecho en el momento):
```
python3 infra/04-deploy-genie.py --warehouse <warehouse-id> --skip-deploy \
  --ask "¿Cuáles son los ingresos totales y cuántas líneas de pedido hay?"
```

## 4. Cómo recrear / actualizar

```
cd /Users/macdenix/fmcg-medallion-lakehouse-azure
export DATABRICKS_HOST=https://<workspace-host> DATABRICKS_AUTH_TYPE=azure-cli
# dashboard: (opcional) regenerar el JSON desde el generador
python3 dashboards/build_fmcg_sales_insights.py dashboards/fmcg_sales_insights.lvdash.json
python3 infra/03-deploy-dashboard.py --warehouse <warehouse-id> --verify --dry-run   # prueba los datasets
DATABRICKS_BUNDLE_ENGINE=direct databricks bundle deploy -t dev                         # actualiza y publica (vía bundle)
python3 infra/03-deploy-dashboard.py --warehouse <warehouse-id> --export   # si se editó en la UI
# alternativa sin bundle: python3 infra/03-deploy-dashboard.py --warehouse <warehouse-id> (PATCH del mismo id)
# Genie
python3 infra/04-deploy-genie.py --warehouse <warehouse-id>               # crea o actualiza
```
Ambos scripts son idempotentes (buscan por nombre/título y hacen `PATCH`). Si se edita el dashboard en la UI, exportarlo antes de volver a desplegar o el despliegue pisa los cambios (y entonces el generador queda desactualizado respecto del JSON).

## 5. Limitaciones y pendientes

- **Dashboard en el Asset Bundle (hecho, sin duplicar):** `resources/dashboard.yml` declara `dashboards.fmcg_sales_insights` con `file_path: ../dashboards/fmcg_sales_insights.lvdash.json` y `parent_path` = la carpeta donde ya vivía. Antes de desplegar se enlazó al existente con `databricks bundle deployment bind fmcg_sales_insights <dashboard-id> -t dev` (solo escribe el estado del bundle; reversible con `bundle deployment unbind`). `bundle plan` mostró `update` (no `create`) y como único cambio el `display_name`: el contenido serializado era idéntico. Tras `bundle deploy`: un solo dashboard `FMCG` en el workspace, mismo id y URLs, publicado con credenciales embebidas; `bundle plan` posterior: 0 cambios. Efecto visible: `mode: development` antepone `[dev <user>] ` al nombre; `infra/03` ya reconoce ese nombre (si no, habría creado un duplicado). Riesgo residual: si se edita en la UI y luego se hace `bundle deploy`, el bundle pisa la edición (exportar antes). Genie no se declaró en el bundle (CLI v0.288.0; no se probó como recurso).
- **Filtros de selección única:** "Todos" es un valor más del desplegable; no hay multi-selección (p. ej. Q1 y Q2 juntos). Pasar a multi-selección exige parámetros multivalor (`ARRAY_CONTAINS`).
- **Moneda:** todos los montos están en PEN porque Silver fija `currency = 'PEN'`; los datasets no agrupan por `currency`. Si llegara multimoneda, los contadores sumarían monedas distintas: habría que agregar un filtro de moneda o una tabla de tipos de cambio.
- **Pedidos:** el contador suma `orders` de `v_kpi_sales_monthly` (distintos por mes y compañía). Es exacto porque un pedido pertenece a un solo día; no lo sería si un pedido abarcara varios meses.
- **Calidad:** `v_kpi_data_quality` muestra la última corrida **por entidad**; si Silver se corre solo para una compañía, la otra desaparece de esa vista. El contador "filas marcadas/corregidas/rechazadas" suma reglas `info` (duplicados, normalizaciones), así que no es un número de "errores".
- **Genie es probabilístico:** las 7 preguntas de ejemplo coinciden con su SQL de referencia (§3), pero otras preguntas pueden generar SQL distinto; conviene marcar como confiables (thumbs up / SQL de ejemplo) las que se usen en demos. Las conversaciones de prueba quedaron en el historial del espacio.
- **Permisos:** solo el creador tiene acceso. Compartir el dashboard publicado y el espacio Genie con el grupo `analysts` cuando exista (hoy los grupos están comentados en `infra/02`).
- Comentarios de columnas de las vistas: ahora están también en UC (lista de columnas comentada en el `CREATE OR REPLACE VIEW` de `src/gold/ddl.py`; la tarea `setup` de Gold los mantiene al recrear las vistas).

## 6. Capturas

Tomadas con el MCP de Playwright sobre la sesión del navegador del usuario que ya estaba iniciada en el workspace (no se inició sesión).

| Archivo | Contenido |
|---|---|
| `docs/img/dashboard-ventas.png` | página Ventas publicada, sin filtros |
| `docs/img/dashboard-calidad.png` | página Calidad de datos publicada |
| `docs/img/genie-ingresos-totales.png` | Genie: ingresos totales y líneas (37.450.346,40 PEN; 996.453) |
| `docs/img/genie-top5-2024.png` | Genie: top 5 productos 2024 con tabla y gráfico |
