# FMCG Medallion Lakehouse — Azure Databricks

Solución end-to-end (Bronze → Silver → Gold → BI) sobre **Azure Data Lake Storage Gen2 + Azure Databricks + Unity Catalog**, pieza de portafolio de datos.

Inspirada en el repo de referencia `takadlag/databricks-medallion-lakehouse-fmcg` (commit `5e99b2b`), pero reconstruida desde cero: datos sintéticos propios, tablas "madre" explícitas, calidad de datos verificable, orquestación y documentación honesta.

> Estado: **fase 0 — análisis y descubrimiento**. Ver `docs/`.

## Documentos
- `docs/00-analisis-referencia.md` — qué es la referencia, qué le falta y qué construimos.
- `docs/01-inventario-azure.md` — recursos existentes en Azure/Databricks y brechas.
- `docs/02-decisiones.md` — registro de decisiones (incluye las de los expertos asesores por capa).
