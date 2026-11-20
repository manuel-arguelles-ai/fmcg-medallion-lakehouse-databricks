# Estado vivo del proyecto

Leyenda: ✅ verificado con ejecución real · 🟡 parcial · 🐞 roto · ⛔ sin implementar · ⏳ sin verificar.
Última actualización: 2026-11-19 (cierre de sesión 1).

## Estado por hito
| Hito | Estado | Evidencia |
|---|---|---|
| H0 Infra Azure + diseño | ✅ | RG, ADLS Gen2, Access Connector (+rol), workspace premium sin NAT creados y verificados por CLI; 0 NAT gateways en el RG gestionado |
| H1 Unity Catalog | ✅ | credencial `cred_fmcg_lakehouse`, 5 external locations, catálogo `fmcg_lakehouse`, 5 esquemas, 2 volumes, 3 tablas `ops.*`, 8 filas en `ops.source_registry` (verificado con consulta) |
| H2 Datagen + landing | ✅ | 1.606 CSV subidos = 1.606 del MANIFEST; 1.005.642 líneas de pedido |
| H3 Bronze (Auto Loader) | ✅ | Job desplegado y corrido en serverless: 8/8 tablas reconcilian con el MANIFEST (filas y archivos), 0 `_rescued_data`; 2.ª corrida = 0 archivos nuevos, 0 filas escritas (idempotente) |
| H4 Silver | 🟡 | Código y 101 pruebas locales ✅; **nunca corrió en Spark/workspace** ⏳ |
| H5 Gold | ⛔ | solo diseño (`03-diseno-solucion.md` §5.3) |
| H6 Orquestación end-to-end + incremental | ⛔ | bundle solo tiene Bronze; `resources/silver_job.yml` sin enlazar |
| H7 Dashboard + Genie | ⛔ | — |
| H8 README final + `.drawio` + GitHub | ⛔ | el `.drawio` ya está en `docs/`; falta README final y publicar |

## Pendientes (orden sugerido)
1. **Moneda a USD** (decisión D26). Silver quedó con `rules.CURRENCY = "PEN"` (`src/silver/rules.py:20`). Los precios de datagen (p. ej. 17.87, 3.45) no traen moneda en el CSV, así que el cambio es la constante + `docs/08-silver-implementacion.md` (D17, §6 duda 4) + regenerar `src/silver/sql/01_silver_ddl.sql` si lo referencia. Bronze no cambia. Estado: ⏳ no aplicado.
2. **Enlazar y correr Silver** ⛔: agregar `include: [resources/*.yml]` y `src/silver/**` en `sync.include` de `databricks.yml`; ejecutar antes `src/silver/sql/01_silver_ddl.sql` (**NO EJECUTADO**, con `infra/run_sql.py`, pide OK de Manu); `bundle validate` → `deploy` → correr `silver_transform` en modo `full`.
3. **Validar Silver contra `src/silver/expected.py`** ⏳ (hija/orders: 296.754 en Silver y 4.633 en cuarentena; ver `docs/08` §5). Riesgos de Spark aún no probados: `try_element_at` vía `call_function`, patrón de fecha `MMMM d, yyyy`, comportamiento ANSI, `MERGE ... UPDATE SET *` desde vista temporal. (`try_to_date` vía `call_function` sí funciona: lo usa Bronce.)
4. **Idempotencia e incremental de Silver** ⏳ (dos corridas iguales; modo `incremental` desde `staging_*`).
5. **Registrar D9–D25** (propuestas del agente en `docs/05` y `docs/08`) en `02-decisiones.md` tras la revisión de Manu con los expertos. Manu: "en breve te lo digo". ⏳
6. Preguntas abiertas para expertos (detalle en `docs/05` §4–5 y `docs/08` §6): qué hace el staging cuando no hay lote nuevo; celdas vacías llegan como NULL; si se recrea el checkpoint hay que subir `--generation`; uso de los contenedores `bronze/silver/gold` (hoy las tablas son managed en `uc-managed`, las 3 external locations quedaron reservadas sin uso por decisión de Manu de crear 5); el volume externo cubre todo `landing`.
7. Ciclo de vida de landing a 90 días **no aplicado** (comando propuesto en `docs/05`) ⛔. Permisos/grupos (`de_engineers`, `analysts`) comentados en `infra/02` ⛔.
8. Gold (H5), orquestación (H6), dashboard y Genie (H7), README final y publicación a GitHub (H8).

## Decisiones de Manu tomadas el 2026-11-19 (ver `02-decisiones.md`)
- Moneda USD; precio no numérico va a cuarentena; `line_amount` se calcula en Silver; toda corrección o transformación va en Silver.

## Verificado vs asumido
- **Verificado:** todo H0–H3 (arriba), 101 pruebas pytest locales, `bundle validate`, exportación del `.drawio` (XML válido, 27 nodos, 13 aristas).
- **Asumido / sin ejecutar:** todo `src/silver/transform.py`; que los conteos esperados de Silver coincidan; rendimiento de Silver a ~1 M de líneas.
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
.venv/bin/pytest -q                      # 101 pruebas
databricks bundle validate -t dev
python3 infra/run_sql.py <archivo.sql> --warehouse <warehouse-id> [--dry-run]
```
Login de Azure: lo hace Manu en su terminal (`az login`); nunca ejecutarlo desde la sesión.
