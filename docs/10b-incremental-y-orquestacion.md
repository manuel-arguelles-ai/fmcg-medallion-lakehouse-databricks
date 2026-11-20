# Orquestación end-to-end y prueba incremental

## Job maestro
`resources/pipeline_job.yml` define `fmcg-medallion-pipeline`: Bronze → Silver → Gold → `reconcile`. Parámetros `catalog` y `mode` (`incremental` | `full`), `max_concurrent_runs: 1`. Si una etapa falla, las siguientes no corren.

`src/pipeline/reconcile.py` compara conteos landing → bronze → silver (+ cuarentena) → gold por compañía y fuente, y en pedidos también los montos (Silver, Gold, agregado). Escribe una fila por combinación en `ops.pipeline_runs` y hace fallar el job si algo no cuadra.

## Lote incremental de prueba
`src/datagen/increment.py` genera un archivo pequeño y siempre igual (25 filas de pedidos de la hija) con anomalías sembradas: cantidad nula, cliente inválido, producto sin precio y duplicados. Se niega a sobrescribir un archivo existente con otro contenido. Se subió a `landing` como archivo nuevo (`hija/orders/ingest_date=2026-01-02/`); no se tocó nada previo.

## Resultados medidos (19-nov-2026)
| Medida | Línea base | Con el lote | Esperado (Python) | Workspace |
|---|---|---|---|---|
| Pedidos hija en Bronze | 305.943 | 305.968 | 305.968 | 305.968 |
| Pedidos hija en Silver | 296.754 | 296.776 | 296.776 | 296.776 |
| Cuarentena de pedidos hija | 4.633 | 4.634 | 4.634 | 4.634 |
| `fact_sales` | 996.453 | 996.475 | 996.475 | 996.475 |
| Ingreso total (PEN) | 37.450.346,40 | 37.450.922,98 | 37.450.922,98 | 37.450.922,98 |

`scripts/verify_gold_kpis.py` recalcula todos los KPI desde los CSV y coincide con Gold (veredicto OK). Solo cambian nov-2025 y dic-2025 de la hija.

## Idempotencia
- Corrida incremental **sin** lote nuevo: Bronce no escribe filas y Silver/cuarentena/Gold quedan idénticos (incluida la marca `_silver_updated_at`). Esto requirió un fix: el `MERGE` incremental de Silver actualiza solo si la fila es estrictamente más reciente (`>`).
- Corrida incremental repetida **después** del lote (run `490051995343769`, SUCCESS en las 4 tareas): mismos conteos, 996.475 líneas y 37.450.922,98 PEN.
- Corrida con el lote: run `644643814040590`, SUCCESS en bronze, silver, gold y reconcile.

## Nueva línea base
996.475 líneas y 37.450.922,98 PEN. El lote de prueba queda en landing como parte del dataset.
