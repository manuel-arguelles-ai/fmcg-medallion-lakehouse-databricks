# Inventario Azure / Databricks (solo lectura, 2026-11-19)

Verificado con `az` y `databricks` CLI, sin crear ni modificar nada.

## Existe y es reutilizable
| Recurso | Detalle |
|---|---|
| Suscripción | "Azure subscription 1" (Enabled; la CLI no expone el tipo de oferta, confirmar que es Free Trial en el portal) |
| Región | centralus |
| Databricks workspace | SKU **premium**, sin VNet injection, aprovisionado OK |
| Unity Catalog | metastore asignado al workspace |
| ADLS Gen2 | 2 cuentas con HNS: una con contenedores bronze/silver/gold, otra con `fmcg` y `fmcg-dev` |
| Credencial de storage | 1 credencial UC con acceso a ambas cuentas + external locations ya creadas |
| Cómputo | 1 SQL Warehouse serverless (Small, detenido); varios clusters terminados |
| Otros | Key Vault, Azure SQL, Data Factory (no necesarios para esta solución) |

## Conflictos a resolver (el entorno NO está vacío)
- Ya existen los catálogos UC **`fmcg`** y **`fmcg_dev`**, con external locations y contenedores homónimos: pertenecen a otro trabajo. Nuestra solución NO debe usar esos nombres.
- El workspace y las cuentas de storage son compartidos con otros proyectos; hay que aislar con catálogo, contenedor y esquemas propios.
- No se tiene rol de datos (Blob Data *) sobre el contenedor `fmcg` vía Azure AD; sí hay acceso admin en Databricks.

## Brechas (qué falta para esta solución)
- Catálogo UC propio + external location para su contenedor (landing, bronze, silver, gold).
- Contenedor ADLS propio (o cuenta nueva) y permisos sobre él.
- Datos sintéticos cargados en landing.
- Cómputo para jobs (serverless o cluster pequeño), validar cuotas de la Free Trial.

> Toda creación/modificación en Azure o Databricks se propone con el comando exacto y se ejecuta solo con confirmación explícita.
