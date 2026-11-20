# Gold — requisitos (qué pide la referencia y qué suma la sala)

## 1. Lo que hace la referencia
`takadlag/databricks-medallion-lakehouse-fmcg` (commit `5e99b2b`) entrega una capa Gold orientada a un dashboard "Sales Insights": hecho de ventas mensual con dimensiones de cliente, producto y fecha mensual, y KPIs de ingresos, cantidad, top 10 de productos y mix por categoría. No incluye tablas madre explícitas, historial de precios ni preparación para ML (ver `00-analisis-referencia.md`).

## 2. Lo que suma la sala (5 respuestas sobre qué contiene Gold)
Síntesis de las respuestas recibidas:
1. Es la capa final, con los datos más limpios tras procesamientos, joins y configuraciones incrementales; en ella están las estructuras que consumen BI, ML y AI, para evitar transformaciones pesadas a futuro.
2. Datos listos para consumo final del negocio: reportes, dashboards (p. ej. Power BI), análisis ejecutivo o modelos de ML.
3. KPIs y tablas agregadas de negocio (hechos y dimensiones) que consumen los dashboards.
4. Aquí se definen los casos de negocio; es la data que consumen los analistas para sus dashboards.
5. Las tablas que resuelven una pregunta del negocio.

**Lo que la sala sumó y el proyecto no tenía:** preparar Gold también para ML/AI, no solo para dashboards.

## 3. Mapeo a la implementación
| Requisito | Objeto Gold | Consumidor |
|---|---|---|
| Hechos y dimensiones para BI (2, 3) | `dim_date`, `dim_customer`, `dim_product`, `dim_price`, `fact_sales` | BI |
| Tablas agregadas que resuelven una pregunta (3, 5) | `agg_sales_monthly`, vistas `v_kpi_*` | BI / Genie |
| Casos de negocio definidos (4) | KPIs de ingresos, top 10, mix por categoría, madre vs hija, calidad | analistas |
| Datos listos para ML/AI (1, 2 y lo que suma la sala) | `feature_sales_monthly` (rezagos 1–3, promedio móvil, etiqueta del mes siguiente) | ML / AI |
| Evitar transformaciones pesadas a futuro (1) | recarga idempotente y reconstruible; agregados y features precalculados | todos |
| Procesos incrementales (1) | Gold se reconstruye desde Silver tras cada corrida (`incremental`) | pipeline |

Implementación y riesgos: `09-gold-implementacion.md`.
