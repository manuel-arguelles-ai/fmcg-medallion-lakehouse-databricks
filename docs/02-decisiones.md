# Registro de decisiones

| # | Fecha | Decisión | Origen |
|---|---|---|---|
| D1 | 2026-11-19 | Repo de la solución separado del clon de referencia; la referencia es solo consulta | Manu |
| D2 | 2026-11-19 | Todo corre en Azure (ADLS Gen2 + Databricks) en el tenant free trial existente | Manu |
| D3 | 2026-11-19 | Solución completa, no copia; el tratamiento por capa se decide con los expertos data engineers | Manu |
| D4 | 2026-11-19 | Repo local con git para trazabilidad; GitHub después | Manu |
| D5 | 2026-11-19 | Infra exclusiva y nueva: RG, ADLS Gen2, Access Connector y workspace Databricks premium propios; sin reutilizar componentes previos | Manu |
| D6 | 2026-11-19 | Workspace sin NAT gateway (nodos con IP pública, `enable-no-public-ip=false`) para evitar cargo fijo; preferir serverless | Manu |
| D7 | 2026-11-19 | Región `centralus`; el metastore UC regional es compartido y no se puede evitar sin cambiar de región | Manu / Nix |
| D8 | 2026-11-19 | Catálogo propio `fmcg_lakehouse` (los nombres `fmcg` y `fmcg_dev` están ocupados por otro trabajo) | Nix (propuesto) |

| D26 | 2026-11-19 | Moneda de la solución: **PEN** (Manu revierte USD el mismo día: se cierra con PEN para no reprocesar y ganar tiempo; USD queda como mejora futura) | Manu |
| D27 | 2026-11-19 | Precio no numérico → **cuarentena** en Silver (la alternativa "0 marcado" queda descartada por ahora) | Manu |
| D28 | 2026-11-19 | `line_amount` se calcula en **Silver**; criterio general: toda corrección o transformación va en Silver | Manu |
| D29 | 2026-11-19 | `databricks bundle deploy` con motor `direct` (el motor Terraform falla por clave GPG expirada en el CLI v0.288) | Nix |

> Propuestas D9–D15 (`docs/05-bronce-implementacion.md`) y D16–D25 (`docs/08-silver-implementacion.md`): **sin registrar**, pendientes de revisión de Manu con los expertos.

## Pendientes por capa (a decidir con los expertos)
- Landing/Bronze:
- Silver:
- Gold:
- Serving/BI:
