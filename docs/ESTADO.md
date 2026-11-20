# Estado vivo del proyecto

Leyenda: ✅ verificado con ejecución real · 🟡 parcial · 🐞 roto · ⛔ sin implementar · ⏳ sin verificar.
Última actualización: 2026-11-19 (sesión 2: Silver y Gold corridos en el workspace).

## Estado por hito
| Hito | Estado | Evidencia |
|---|---|---|
| H0 Infra Azure + diseño | ✅ | RG, ADLS Gen2, Access Connector (+rol), workspace premium sin NAT creados y verificados por CLI; 0 NAT gateways en el RG gestionado |
| H1 Unity Catalog | ✅ | credencial `cred_fmcg_lakehouse`, 5 external locations, catálogo `fmcg_lakehouse`, 5 esquemas, 2 volumes, 3 tablas `ops.*`, 8 filas en `ops.source_registry` (verificado con consulta) |
| H2 Datagen + landing | ✅ | 1.606 CSV subidos = 1.606 del MANIFEST; 1.005.642 líneas de pedido |
| H3 Bronze (Auto Loader) | ✅ | Job desplegado y corrido en serverless: 8/8 tablas reconcilian con el MANIFEST (filas y archivos), 0 `_rescued_data`; 2.ª corrida = 0 archivos nuevos, 0 filas escritas (idempotente) |
| H4 Silver | ✅ | DDL ejecutado (19 sentencias); `silver_transform` modo `full` en serverless: orders 699.699 + 296.754, cuarentena 4.633, prices 24 en cuarentena; coincide con `expected.py`. 2.ª corrida `full` con los mismos conteos |
| H5 Gold | ✅ | `gold_build` en serverless, 7 tareas ok; ingresos iguales en Silver, `fact_sales` y `agg_sales_monthly`; 6 vistas KPI; KPIs recalculados en Python desde los CSV (`scripts/verify_gold_kpis.py`, 342 comparaciones OK, `docs/09` §8); 2.ª corrida idéntica |
| H6 Orquestación end-to-end + incremental | ✅ | job maestro `fmcg-medallion-pipeline` (Bronze → Silver → Gold → `reconcile` con `ops.pipeline_runs`; falla si no cuadra). Lote incremental real (25 filas de hija, subido a landing): Bronze 305.968, Silver 296.776, cuarentena 4.634 y 996.475 líneas en Gold, todo igual a lo calculado antes en Python; ingreso 37.450.922,98 PEN; corrida incremental repetida sin cambios. Detalle: `docs/10b-incremental-y-orquestacion.md` |
| H7 Dashboard + Genie | ✅ | dashboard AI/BI `FMCG Sales Insights` (12 datasets verificados, 2 páginas) y espacio Genie `FMCG Sales Genie` (4 preguntas contrastadas con SQL directo). Detalle y capturas: `docs/11-serving.md` |
| H8 README final + `.drawio` + GitHub | 🟡 | `.drawio` en `docs/`; README final y publicación en curso |

## Pendientes (orden sugerido)
1. ~~Moneda a USD~~ **Descartado**: D26 quedó en **PEN** (cierre sin reprocesos). USD queda como mejora futura.
2. **Registrar D9–D25** (propuestas del agente en `docs/05` y `docs/08`) en `02-decisiones.md` tras la revisión de Manu con los expertos. Manu: "en breve te lo digo". ⏳
3. Preguntas abiertas para expertos (detalle en `docs/05` §4–5 y `docs/08` §6): qué hace el staging cuando no hay lote nuevo; celdas vacías llegan como NULL; si se recrea el checkpoint hay que subir `--generation`; uso de los contenedores `bronze/silver/gold` (hoy las tablas son managed en `uc-managed`, las 3 external locations quedaron reservadas sin uso por decisión de Manu de crear 5); el volume externo cubre todo `landing`.
4. Ciclo de vida de landing a 90 días **aplicado** ✅ (`infra/05-landing-lifecycle.sh`, regla `landing-90d`, verificado con `--show`, 2026-11-19). Permisos/grupos (`de_engineers`, `analysts`) comentados en `infra/02` ⛔.

## Decisiones de Manu tomadas el 2026-11-19 (ver `02-decisiones.md`)
- Moneda PEN (revierte USD); precio no numérico va a cuarentena; `line_amount` se calcula en Silver; toda corrección o transformación va en Silver.

## Verificado vs asumido
- **Verificado:** todo H0–H3 (arriba), 159 pruebas pytest locales (`.venv/bin/pytest -q`, cubren datagen, Bronze, Silver, Gold, reconcile y verificador de KPI), `bundle validate -t dev` OK sobre el bundle completo (bronze_ingest, silver_transform, gold_build, medallion_pipeline; 2026-11-19), exportación del `.drawio` (XML válido, 27 nodos, 13 aristas).
- **Verificado también (H4–H7):** Silver y Gold corridos en serverless y reconciliados con lo calculado en Python; job maestro e incremental con lote real; dashboard y Genie contrastados con SQL directo.
- **Asumido / sin ejecutar:** rendimiento a escalas mayores que ~1 M de líneas; comportamiento con volúmenes de landing de decenas de miles de archivos.
- **No visto renderizado:** la web `docs/diseno.html` y el enlace del diseñador (solo se validó su lógica; la extensión de Chrome no estaba conectada).

## Trampas de entorno (costaron tiempo)
- `databricks bundle deploy` por defecto descarga Terraform y falla (*openpgp: key expired*, CLI v0.288): usar `DATABRICKS_BUNDLE_ENGINE=direct`.
- `databricks storage-credentials create` con `--json` exige el `name` **dentro** del JSON (sin argumento posicional).
- En serverless `__file__` no existe en `spark_python_task` (el runner hace `exec(compile(...))`): resolver la ruta con `inspect.currentframe().f_code.co_filename`.
- `F.try_to_date` no existe en el PySpark del entorno serverless `environment_version: "3"`: usar `F.call_function("try_to_date", ...)`.
- Las tablas Bronze tienen distinto número de columnas: no se pueden `UNION` entre ellas.
- Metastore UC regional **compartido** con otros trabajos: catálogos `fmcg`, `fmcg_dev`, `pede_dev` y sus locations no son nuestros; no tocarlos.
- Sin Java local: nada de Spark en el Mac; validar en el workspace.
- Subida a landing: `az storage blob upload-batch` imprime progreso con `\r`; verificar el conteo con `az storage blob list` aparte.
- El MCP de Playwright guarda `browser_evaluate --filename` en el cwd del proceso (`~`) como cadena JSON; hay que decodificarla.
- La extensión Claude-in-Chrome no estaba conectada: para el diseñador multicloud se usó el MCP de Playwright.

## Cómo retomar
```
cd /Users/macdenix/fmcg-medallion-lakehouse-azure
export DATABRICKS_HOST=https://<workspace-host> DATABRICKS_AUTH_TYPE=azure-cli DATABRICKS_BUNDLE_ENGINE=direct
.venv/bin/pytest -q                      # 159 pruebas
databricks bundle validate -t dev
python3 infra/run_sql.py <archivo.sql> --warehouse <warehouse-id> [--dry-run]
```
Login de Azure: lo hace Manu en su terminal (`az login`); nunca ejecutarlo desde la sesión.
