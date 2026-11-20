# Serving — dashboard AI/BI y espacio Genie (H7)

Estado: **creado y verificado en el workspace dev** el 2026-11-20 (UTC). Diseño: `03-diseno-solucion.md` §5.4. Insumo: tablas y vistas `fmcg_lakehouse.gold.*` (`09-gold-implementacion.md`).

## 1. Qué se creó

| Recurso | Nombre | ID | URL |
|---|---|---|---|
| Dashboard AI/BI (Lakeview), publicado con credenciales embebidas | `FMCG Sales Insights` | `<dashboard-id>` | publicado: https://<workspace-host>/dashboardsv3/<dashboard-id>/published · borrador: https://<workspace-host>/sql/dashboardsv3/<dashboard-id> |
| Espacio Genie | `FMCG Sales Genie` | `<genie-space-id>` | https://<workspace-host>/genie/rooms/<genie-space-id> |

Ambos en `/Users/<owner-email>`, sobre el SQL Warehouse `<warehouse-id>` (Serverless Starter Warehouse, auto-stop 10 min). No se modificó ningún objeto de Unity Catalog ni `databricks.yml`.

### Archivos

| Archivo | Rol |
|---|---|
| `dashboards/fmcg_sales_insights.lvdash.json` | dashboard serializado (fuente que se despliega) |
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

## 3. Espacio Genie `FMCG Sales Genie`

- **Tablas (11):** `fact_sales`, `dim_date`, `dim_customer`, `dim_product`, `agg_sales_monthly` y las 6 vistas `v_kpi_*`. Se excluyen `dim_price` y `feature_sales_monthly` (no aportan a preguntas de negocio y confunden al modelo).
- **Descripciones de columnas:** las tablas `gold.*` ya tienen `COMMENT` en Unity Catalog en todas sus columnas (revisado en `information_schema.columns`; solo `dim_date.year` sin comentario). Las vistas tenían columnas calculadas sin comentario (`revenue`, `orders`, `revenue_share`, `revenue_mom_pct`, todas las de `v_kpi_quarantine_summary`, etc.): se describieron **en el espacio Genie** (`column_configs`), sin alterar UC.
- **Instrucciones:** responder en español; compañías `madre`/`hija`; ingresos = `SUM(line_amount)`/`SUM(revenue)` en la moneda de `currency` (PEN); líneas sin precio no suman ingresos; pedidos con `COUNT(DISTINCT order_id)` y nunca sumando `agg_sales_monthly.orders`; filtros de fecha por `dim_date` o columnas de las vistas; miembro `-1` cuenta en totales pero se excluye de rankings; calidad desde `v_kpi_data_quality`/`v_kpi_quarantine_summary`.
- **SQL de ejemplo (3):** ingresos y líneas por compañía, ingresos mensuales por compañía en un año, top productos por año.
- **Preguntas de ejemplo (7):**
  1. ¿Cuáles son los ingresos totales y cuántas líneas de pedido hay?
  2. ¿Cuántos ingresos tuvo cada compañía (madre e hija) en 2025?
  3. ¿Cuáles son los 5 productos con más ingresos en 2024?
  4. ¿Cómo evolucionaron los ingresos mensuales de la hija en 2025?
  5. ¿Qué categoría aporta más ingresos a la madre?
  6. ¿Cuántas filas hay en cuarentena por regla?
  7. ¿Qué reglas de calidad tuvieron filas fallidas en pedidos de la hija?

### Prueba por la API de conversaciones (2026-11-20 03:11–03:14 UTC)

Referencia: SQL directo ejecutado justo antes y justo después (03:11:35 y 03:14:44 UTC), ambos con `SUM(line_amount)` = 37.450.346,40 y 996.453 líneas (no hubo carga entre medio).

| Pregunta | SQL generado por Genie (resumen) | Respuesta de Genie | SQL directo | ¿Coincide? |
|---|---|---|---|---|
| ¿Cuáles son los ingresos totales y cuántas líneas de pedido hay? | `SUM(line_amount)`, `COUNT(*)` de `fact_sales` por `currency` | PEN 37.450.346,40 · 996.453 líneas | 37.450.346,40 · 996.453 | sí |
| ¿Cuántos ingresos tuvo cada compañía (madre e hija) en 2025? | `SUM(revenue)` de `v_kpi_sales_monthly WHERE year = 2025` por compañía | hija 5.129.558,42 · madre 14.248.977,29 | `fact_sales` ⋈ `dim_date` (year 2025): hija 5.129.558,42 · madre 14.248.977,29 | sí |
| ¿Cuáles son los 5 productos con más ingresos en 2024? | `v_kpi_top10_products WHERE period = '2024' AND revenue_rank <= 5` | Papas Andinas (Original 150g) 797.452,50; Nectar Frutal (Maracuya 1L) 784.868,13; Galleta Quinua (Vainilla 6u) 726.892,33; Camote Chips (Miel 120g) 714.493,29; Nectar Frutal (Mango 1L) 650.169,79 (todos madre) | mismo orden y montos desde `fact_sales` ⋈ `dim_product` ⋈ `dim_date` | sí |
| ¿Cuántas filas hay en cuarentena por regla? | `SUM(rows_quarantined)` de `v_kpi_quarantine_summary` por `rule_id` | ORD_03 4.633 · PRC_05 24 | 4.633 · 24 | sí |

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
python3 infra/03-deploy-dashboard.py --warehouse <warehouse-id> --verify   # crea o actualiza y publica
python3 infra/03-deploy-dashboard.py --warehouse <warehouse-id> --export   # si se editó en la UI
# Genie
python3 infra/04-deploy-genie.py --warehouse <warehouse-id>               # crea o actualiza
```
Ambos scripts son idempotentes (buscan por nombre/título y hacen `PATCH`). Si se edita el dashboard en la UI, exportarlo antes de volver a desplegar o el despliegue pisa los cambios (y entonces el generador queda desactualizado respecto del JSON).

## 5. Limitaciones y pendientes

- **No está en el Asset Bundle:** el dashboard y el espacio se despliegan con scripts propios porque `databricks.yml` lo edita otro flujo. Cuando se pueda, agregar el dashboard como recurso `dashboards:` del bundle apuntando a `dashboards/fmcg_sales_insights.lvdash.json` (para Genie, verificar si la versión del CLI ya lo soporta como recurso de bundle; no se probó).
- **Filtros de selección única:** "Todos" es un valor más del desplegable; no hay multi-selección (p. ej. Q1 y Q2 juntos). Pasar a multi-selección exige parámetros multivalor (`ARRAY_CONTAINS`).
- **Moneda:** todos los montos están en PEN porque Silver fija `currency = 'PEN'`; los datasets no agrupan por `currency`. Si llegara multimoneda, los contadores sumarían monedas distintas: habría que agregar un filtro de moneda o una tabla de tipos de cambio.
- **Pedidos:** el contador suma `orders` de `v_kpi_sales_monthly` (distintos por mes y compañía). Es exacto porque un pedido pertenece a un solo día; no lo sería si un pedido abarcara varios meses.
- **Calidad:** `v_kpi_data_quality` muestra la última corrida **por entidad**; si Silver se corre solo para una compañía, la otra desaparece de esa vista. El contador "filas marcadas/corregidas/rechazadas" suma reglas `info` (duplicados, normalizaciones), así que no es un número de "errores".
- **Genie es probabilístico:** las 4 respuestas probadas coinciden, pero otras preguntas pueden generar SQL distinto; conviene marcar como confiables (thumbs up / SQL de ejemplo) las que se usen en demos. Las conversaciones de prueba quedaron en el historial del espacio.
- **Permisos:** solo el creador tiene acceso. Compartir el dashboard publicado y el espacio Genie con el grupo `analysts` cuando exista (hoy los grupos están comentados en `infra/02`).
- Comentarios de columnas de las vistas: se completaron en Genie, no en UC. Para que también aparezcan en el Catalog Explorer habría que agregar `COMMENT` por columna en `src/gold/ddl.py` (vistas con lista de columnas) y recrearlas.

## 6. Capturas

Tomadas con el MCP de Playwright sobre la sesión del navegador del usuario que ya estaba iniciada en el workspace (no se inició sesión).

| Archivo | Contenido |
|---|---|
| `docs/img/dashboard-ventas.png` | página Ventas publicada, sin filtros |
| `docs/img/dashboard-calidad.png` | página Calidad de datos publicada |
| `docs/img/genie-ingresos-totales.png` | Genie: ingresos totales y líneas (37.450.346,40 PEN; 996.453) |
| `docs/img/genie-top5-2024.png` | Genie: top 5 productos 2024 con tabla y gráfico |
