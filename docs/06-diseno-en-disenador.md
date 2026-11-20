# Diseño en el diseñador multicloud

Herramienta: https://analizodatos.com/ingenieria-de-datos-multicloud (modo *Diseñador*, plataforma **Databricks**, nube **Azure**).

**Enlace al diseño (el estado completo va en el hash de la URL):**

https://analizodatos.com/ingenieria-de-datos-multicloud/#m=disenador&p=databricks&n=azure&s=src.files%2Cing.batch%2Csto.bronze%2Csto.silver%2Csto.gold%2Cproc.spark%2Cproc.sql%2Csrv.dwh%2Ccon.bi%2Ctrx.orq%2Ctrx.gov%2Ctrx.obs%2Ctrx.cicd&vf=src.files%3Acsv&vc=databricks.ing.batch%3Aautoloader%2Cdatabricks.srv.dwh%3Aserverless%2Cdatabricks.con.bi%3Adashboards%7Cgenie%2Cdatabricks.trx.obs%3Asystables%7Cdq

## Mapeo del diseño a las capacidades de la herramienta

| Capa (diseño propio) | Capacidad | Componente (Databricks en Azure) | Variante elegida |
|---|---|---|---|
| Fuentes (madre + hija) | Archivos CSV / Excel | CSV en ADLS Gen2 | CSV |
| Landing → Bronze | Ingesta batch | Auto Loader (Lakeflow Connect no aplica: son archivos) | Auto Loader |
| Bronze | Almacén · Bronce | Delta Lake + Unity Catalog sobre ADLS Gen2 | — |
| Silver | Almacén · Plata | Delta Lake + Unity Catalog sobre ADLS Gen2 | — |
| Gold | Almacén · Oro | Delta Lake + Unity Catalog sobre ADLS Gen2 | — |
| Transformaciones PySpark | Procesamiento Spark | Spark en Databricks (serverless) | — |
| Modelo gold y vistas de KPI | Transformación SQL / ELT | Databricks SQL | Databricks SQL |
| Serving | Warehouse / serving | SQL Warehouse | Serverless |
| Consumo | BI / dashboards | AI/BI Dashboards + Genie | Dashboards y Genie |
| Orquestación | Orquestación | Lakeflow Jobs | — |
| Gobierno | Gobierno y linaje | Unity Catalog | — |
| Calidad y observabilidad (`ops.*`) | Observabilidad | System tables + Data quality monitoring | System tables y Data quality |
| Despliegue | CI/CD y despliegue | Declarative Automation Bundles (antes Asset Bundles) | — |

## Capacidades que NO se incluyeron (y por qué)
- **Ingesta CDC / streaming** y fuentes transaccionales, eventos, logs, APIs, documentos y salidas de modelos: la solución es batch desde archivos.
- **Pipelines batch + streaming (Lakeflow Spark Declarative Pipelines)**: decisión abierta con los expertos (Silver con *expectations* vs jobs propios). Si se adopta, se agrega `proc.pipe`.
- **ML e IA**: fuera de alcance.

## Verificación
El mapeo se validó con la propia lógica de la herramienta (ids de capacidades, variantes y compatibilidad). Resultado: sin advertencias bloqueantes; un aviso informativo: la documentación no declara estado para CSV + Auto Loader en batch.

Nombres de componentes tomados de la herramienta (v2, verificada contra documentación oficial el 2026-11-10); pueden cambiar de nombre con el tiempo.
