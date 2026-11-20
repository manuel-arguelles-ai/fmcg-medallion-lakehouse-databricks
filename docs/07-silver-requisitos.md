# Silver — requisitos (referencia + expertos + diseño)

Fuentes: (a) repo de referencia `/Users/macdenix/databricks-medallion-lakehouse-fmcg` (notebooks `silver/*` y `gold/1_full_load_fact`), (b) consenso de la sala de expertos del 2026-11-19 sobre Silver, (c) `03-diseno-solucion.md` §5.2.

## Principio
Silver = dato confiable: tipado, limpio, deduplicado, estandarizado y conformado entre madre e hija, listo para modelar en Gold. Lo irrecuperable no se pierde: va a cuarentena con motivo.

## Lo que ya hace la referencia (conservar)
- **customers**: dedup por `customer_id`; `trim` del nombre; `initcap`; typos de ciudad mapeados (Bengaluruu/Bengalore→Bengaluru, Hyderabadd/Hyderbad→Hyderabad, NewDelhi/NewDheli/NewDelhee→New Delhi); ciudades nulas corregidas por diccionario de negocio; `customer_id` a string; atributos conformados al modelo madre: `customer = nombre-ciudad` (o `-Unknown`), `market`, `platform`, `channel`.
- **products**: dedup por `product_id`; `initcap` de categoría; corrección del typo recurrente (`Protien`→`Protein`) en nombre y categoría; `division` por categoría; `variant` extraído entre paréntesis; `product_code = sha2(product_name)`; `product_id` no numérico → valor de reserva; renombre `product_name→product`.
- **prices**: `month` parseado desde 4 formatos; `gross_price` válido numérico, negativos → positivos, no numéricos → 0; join con products para traer `product_code`.
- **orders**: descartar `order_qty` nulo; `customer_id` no numérico → valor de reserva; quitar día de semana del texto de fecha y parsear 4 formatos; dedup por (order_id, fecha, cliente, producto, qty); `product_id` a string; join con products para `product_code`; MERGE incremental.

## Lo que suma la sala (no está en la referencia)
1. **Cálculos derivados** sobre los datos ya limpios.
2. **Normalización y tratamiento de nulos** (no solo descartar).
3. **Estandarización de mayúsculas/minúsculas y de monedas.**
4. **Reglas de calidad explícitas** (p. ej. "no admite negativos").
5. **Orquestación** (la sala nombra SSIS/Data Factory/Glue; en nuestra solución es Lakeflow Jobs, con Silver dependiente de Bronze).
6. Tipos de datos correctos (fechas, enteros, decimales).

## Conciliación con nuestro diseño (decisiones propuestas)
| Tema | Propuesta |
|---|---|
| "Quitar nulos" | Nulos en campos **obligatorios** → cuarentena (no se descartan en silencio). Nulos en atributos **opcionales** → valor por defecto explícito (`Unknown`) y flag `*_was_null`. |
| Negativos / no numéricos | Precio negativo: se corrige a positivo **y** se marca `price_was_negative=true`. No numérico: cuarentena o 0 marcado (`price_was_invalid=true`); decidir con expertos. |
| Dedup | Determinista: `row_number()` por clave de negocio, gana el mayor `_ingest_ts`. |
| Claves | Clave de negocio = clave natural + `company`. Huérfanos → miembro desconocido `-1` explícito (no `999999`). |
| Joins | `LEFT JOIN` a products/customers; no resueltos → cuarentena + conteo. |
| Typos y mapeos | En **tablas de referencia** (`silver.ref_city_mapping`, `silver.ref_category_division`, `silver.ref_typo_fixes`), no en código. |
| Moneda | Columna `currency` estandarizada ('INR') en precios y montos; una sola moneda de reporte. |
| Derivados permitidos en Silver | Partes de fecha (`order_year`, `order_quarter`, `order_month`, `order_month_start`, `is_weekend`), flags de calidad, texto normalizado, `line_amount_inr` (cantidad × precio vigente del mes; NULL + flag si no hay precio). A confirmar con expertos si `line_amount_inr` vive en Silver o solo en Gold. |
| Idempotencia | `MERGE` por clave de negocio; reprocesar el mismo lote no duplica ni cambia resultados. |
| Lectura de Bronze | Desde `bronze.staging_<company>_<source>` (solo lote nuevo) en incremental; desde la tabla Bronze completa en carga inicial/reproceso. |
| Calidad | `ops.dq_results` (por corrida y regla: evaluadas/fallidas) y `quarantine.<tabla>` (fila cruda + `rule_id` + `reason` + `_batch_id`). |
| Madre + hija | El mismo código procesa ambas compañías (parametrizado); la madre debe pasar sin cuarentena de datos "sembrados". |

## Fuera de alcance de Silver
Modelo dimensional, SCD2, claves sustituta, agregados y KPIs: eso es Gold.
