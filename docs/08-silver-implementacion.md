# Silver — implementación

Estado: **código escrito y probado localmente en su parte pura; nada ejecutado en Azure/Databricks ni en Spark** (regla de la sesión: solo lectura en la nube, y no hay Java local).
Fuente de verdad de requisitos: `07-silver-requisitos.md`. Diseño: `03-diseno-solucion.md` §5.2.

## 1. Qué se construyó

| Pieza | Archivo | Notas |
|---|---|---|
| Reglas (34) y funciones puras | `src/silver/rules.py` | catálogo `RULES` (id, descripción, severidad, acción) + parseo de fechas (5 formatos), precios, cantidades, ids, texto, dedup; `clean_customer/product/price/order` como oráculo testeable |
| Tablas de referencia | `src/silver/reference_data.py` | ciudades/typos, categoría→división, typos de nombre, atributos por compañía; derivadas de lo que genera datagen (un test verifica la cobertura) |
| DDL + siembra | `src/silver/ddl.py` → `src/silver/sql/01_silver_ddl.sql` (**NO EJECUTADO**) | `silver.*`, `quarantine.*`, `ops.dq_results`, `silver.ref_*`, siembra de `ref_*` (solo inserta lo que falta) y miembros `-1`; el `.sql` se genera del código y un test comprueba que coinciden |
| Transformaciones Spark | `src/silver/transform.py` | una función por entidad, ambas compañías con el mismo código; serverless (solo DataFrame/SQL, sin RDD/caché) |
| Entrypoint | `src/silver/run.py` | `--catalog --entity (customers\|products\|prices\|orders\|all) --mode (incremental\|full) [--company] [--skip-setup]` |
| Job | `resources/silver_job.yml` | `silver_transform`: customers ‖ products → prices → orders (orders depende de prices **y** customers); parámetros `catalog`, `mode`. **No enlazado** a `databricks.yml` |
| Conteos esperados | `src/silver/expected.py` | corre las reglas puras sobre los CSV de datagen para reconciliar el workspace |
| Pruebas | `tests/test_silver_*.py` | 70 pruebas nuevas (101 en total con las 31 de Bronze/datagen) |

## 2. Cómo se corre (pendiente de Manu, con confirmación)

1. Enlazar el job: en `databricks.yml` agregar `include: [resources/*.yml]` y `src/silver/**` en `sync.include` (si no, el código de Silver no sube al workspace). `bundle validate -t dev` sobre una copia con esos dos cambios dio OK (4 tareas, dependencias correctas).
2. Opcional: ejecutar `src/silver/sql/01_silver_ddl.sql` (el entrypoint hace el mismo `setup()` idempotente salvo `--skip-setup`).
3. Carga inicial / reproceso: `databricks bundle run silver_transform -t dev --params mode=full`. Corridas siguientes: `mode=incremental` (lee `bronze.staging_*`).
4. Reconciliar contra `cd src && python -m silver.expected ../data/generated` (tabla en §5).

### Flujo de cada entidad (`transform.py`)
1. Lee Bronze (`incremental` → `bronze.staging_<c>_<f>`, `full` → `bronze.<c>_<f>`), madre+hija unidas.
2. Normaliza claves; filas sin clave válida → cuarentena.
3. Dedup determinista: `row_number()` por clave de negocio, gana mayor `_ingest_ts` (desempate `_source_file` y `_batch_id` desc).
4. Reglas: cuarentena / corregir / marcar (banderas `*_was_*`, `*_unresolved`, `price_missing`).
5. Derivados (partes de fecha, `is_weekend`, `currency`, `line_amount`) y joins `LEFT` con `silver.customers/products/prices`.
6. `MERGE` idempotente (`WHEN MATCHED AND s._ingest_ts >= t._ingest_ts`), cuarentena (borra los lotes ya cuarentenados y reinserta) y `ops.dq_results`.

Claves de negocio (`company` + natural): customers `(company, customer_id)`, products `(company, product_id)`, prices `(company, product_id, price_month)`, orders `(company, order_id, line_no)`. Miembro desconocido `-1` por compañía en customers y products.

## 3. Cumplimiento contra `07-silver-requisitos.md`

Validación: **P** = pytest local (reglas puras), **S** = pendiente de correr en Spark/workspace, **R** = revisión de código/estática.

### Lo que hace la referencia (conservar)
| Requisito | Regla / archivo | Val. |
|---|---|---|
| customers: dedup por `customer_id` | CUS_08, `dedup()` | P+S |
| customers: trim + initcap del nombre | CUS_03, CUS_04 | P+S |
| customers: typos de ciudad por mapeo | CUS_05, `ref_city_mapping` | P+S |
| customers: ciudades nulas por diccionario | CUS_06 → `Unknown` + `city_was_null` (ver D23: no hay valor recuperable en datos sintéticos) | P+S |
| customers: atributos conformados (customer, market, channel) | `customer_label`, `market`, `channel` desde `ref_company`; `platform` no aplica (D25) | S |
| products: dedup por `product_id` | PRD_07 | P+S |
| products: initcap de categoría, `division` por categoría | PRD_05/PRD_06, `ref_category_division` (lookup, no initcap ciego: «Cuidado del Hogar» queda intacto) | P+S |
| products: typo recurrente en nombre | PRD_04, `ref_typo_fixes` (Energisante→Energizante) | P+S |
| products: `variant` entre paréntesis, `product_code = sha2(name)`, renombre `product` | `split_variant`, `F.sha2` | P+S |
| products: `product_id` no numérico | PRD_01 (irrecuperable → cuarentena) y PRD_02 (`PX-<n>` → `<n>`, D16) | P+S |
| prices: `month` desde formatos múltiples | PRC_02/PRC_03, `DATE_FORMATS` (misma tabla en Python y Spark) | P+S |
| prices: negativos a positivo; no numéricos | PRC_04 (abs + `price_was_negative`); PRC_05 (cuarentena, D18) | P+S |
| prices: join con products para `product_code` | LEFT JOIN `silver.products`; PRC_07 marca huérfanos | S |
| orders: descartar `order_qty` nulo | ORD_03 (cuarentena), ORD_04 (no entero positivo) | P+S |
| orders: `customer_id` no numérico → reserva | ORD_07: miembro `-1` + `customer_id_was_invalid` | P+S |
| orders: quitar día de semana y parsear 4 formatos | ORD_02/ORD_06, formato `weekday_long` | P+S (el parseo Spark de `MMMM d, yyyy`: S) |
| orders: dedup por clave | ORD_11 sobre `(company, order_id, line_no)` (D21) | P+S |
| orders: join con products para `product_code` | no se copia a orders (queda en `silver.products`); ORD_09 resuelve `product_id` | S |
| orders: MERGE incremental | `Run.merge` | S |

### Lo que suma la sala
| Requisito | Dónde | Val. |
|---|---|---|
| Cálculos derivados | `order_year/quarter/month/month_start`, `is_weekend`, `unit_price`, `line_amount` (D20) | P+S |
| Normalización y nulos (no solo descartar) | obligatorios → cuarentena; opcionales → `Unknown` + flag | P+S |
| Mayúsculas/minúsculas y monedas | CUS_04, PRD_05; columna `currency` (D17) | P+S |
| Reglas de calidad explícitas | `RULES` (34, id/severidad/acción) | P+R |
| Orquestación | `resources/silver_job.yml` | bundle validate OK en copia; no desplegado |
| Tipos correctos | DDL: `DATE`, `INT`, `DECIMAL(12,2)/(18,2)`, `BOOLEAN` | R |

### Conciliación con el diseño (tabla de `07`)
| Tema | Implementación | Val. |
|---|---|---|
| Nulos obligatorios → cuarentena; opcionales → `Unknown` + `*_was_null` | CUS_01/02, PRD_01/03, PRC_01/02, ORD_01-05 vs CUS_06, PRD_06 | P |
| Negativos / no numéricos | PRC_04 + PRC_05 | P |
| Dedup determinista | `dedup()` (Spark) / `dedup_latest()` (puro) | P (puro) + S |
| Claves natural + `company`; huérfano `-1` | `*_key = company:id`, filas `-1` sembradas | S |
| Joins LEFT; no resueltos | ORD_08/ORD_09/PRC_07 marcan y cuentan; D19 explica por qué no van a cuarentena | S |
| Tablas de referencia, no código | `silver.ref_*` leídas en cada corrida | S |
| Moneda | `currency` (`PEN`, D17) | P |
| Derivados permitidos | ver arriba | P+S |
| Idempotencia | MERGE por clave + cuarentena delete+insert por lote | **S** (la prueba real: correr 2 veces y comparar) |
| Lectura de Bronze | `mode` incremental/full | S |
| Calidad | `ops.dq_results`, `quarantine.<entidad>` | S |
| Madre + hija, madre sin cuarentena | mismo código; test: la madre limpia no dispara ninguna regla (ni corregir/marcar) | P |

### Anomalías sembradas por datagen → regla (pytest las compara con `MANIFEST.json` en el dataset pequeño)
| Anomalía (manifest) | Regla | Acción |
|---|---|---|
| `dup_rows` (customers/products/orders), `dup_cross_file` | CUS_08, PRD_07, ORD_11 | corregir (dedup) |
| `name_whitespace` / `name_case_mixed` | CUS_03 / CUS_04 | corregir |
| `city_typo` / `city_null` | CUS_05 / CUS_06 | corregir / marcar |
| `category_lowercase` / `name_typo_term` / `product_id_non_numeric` | PRD_05 / PRD_04 / PRD_02 | corregir |
| `date_format_*` en prices / orders | PRC_03 / ORD_06 | corregir |
| `negative_price` / `non_numeric_price` | PRC_04 / PRC_05 | corregir+marcar / cuarentena |
| `customer_id_invalid` | ORD_07 | corregir (`-1` + flag) |
| `order_qty_null` | ORD_03 | cuarentena |

## 4. Decisiones nuevas propuestas (para registrar en `02-decisiones.md`)

| # | Propuesta |
|---|---|
| D16 | `product_id` con prefijo `PX-<n>` se **recupera** a `<n>` (bandera `product_id_was_prefixed`), no se manda a un valor de reserva como la referencia: orders/prices ya usan el id numérico, así que el prefijo es ruido recuperable. Lo irrecuperable (nulo, `abc`) va a cuarentena |
| D17 | Moneda `PEN` (dominio sintético andino), columna `line_amount` + `currency`. `07` habla de `INR`/`line_amount_inr` heredado de la referencia (India); etiquetar soles como INR sería un error. La constante es `rules.CURRENCY`; renombrar la columna es trivial si Gold prefiere otro nombre |
| D18 | Precio no numérico o nulo → **cuarentena** (con 0 un monto saldría en 0 sin avisar). Alternativa implementada y conmutable: `rules.PRICE_INVALID_ACTION = MARCAR` (precio 0 + `price_was_invalid`) |
| D19 | `quarantine` = filas que **no** entran a Silver (la conciliación Bronze = Silver + cuarentena + duplicados descartados cuadra). Huérfanos con clave bien formada **sí** entran: `customer_id` inválido o no resuelto → `-1` + bandera (la venta sigue valiendo); producto no resuelto → `-1` y `line_amount` NULL + bandera. El diseño §5.2 decía «registrar en cuarentena» los huérfanos: aquí se registran en `ops.dq_results` y en las banderas. Clave de producto/pedido nula o malformada → cuarentena |
| D20 | `line_amount` vive en Silver (a confirmar con expertos). Se calcula al cargar con el precio vigente del mes; si cambian los precios, reprocesar orders en `full` |
| D21 | Clave de negocio de orders = `(company, order_id, line_no)` (la referencia usaba 5 campos, que dejaría pasar un duplicado con distinta cantidad). Dedup **antes** de evaluar reglas |
| D22 | Cuarentena: 1 fila por fila rechazada (`rule_id` = primera regla, `reason` = todos los motivos). Idempotencia: se borran los lotes ya cuarentenados y se reinsertan (no es atómico). `ops.dq_results.batch_id` = id de la corrida de Silver; `rows_evaluated` = población de la etapa donde se evalúa la regla (crudo, deduplicado o tras joins) |
| D23 | Ciudad nula → `Unknown` + `city_was_null`. En datagen la ciudad nula es nula en **todos** los snapshots del cliente: no existe «mapeo de ciudad por cliente» derivable; se evaluará si la hija entrega un diccionario real |
| D24 | Tareas por **entidad** (ambas compañías en una), no por compañía×entidad: el dedup y los joins son por `company`, y así orders espera a que *todas* sus dependencias terminen. Las tablas `ref_*` se siembran solo-insert para no pisar ediciones de expertos |
| D25 | `market` y `channel` salen de `silver.ref_company` (Perú; Retail tradicional / Bares deportivos). `platform` de la referencia no tiene equivalente en nuestros datos y se omite |

## 5. Valores esperados en el workspace (modo `full`, 1 M de líneas, semilla 42)

Calculados localmente con las reglas puras (`python -m silver.expected`). Si Spark no coincide, hay un bug en `transform.py` o en el paralelismo regla/Spark.

| Entidad · compañía | Bronze | Silver | Cuarentena | Dups descartados | Otras |
|---|---|---|---|---|---|
| customers · madre | 32.250 | 1.500 | 0 | 30.750 | 0 correcciones |
| customers · hija | 13.285 | 600 | 0 | 12.685 | CUS_03 48, CUS_04 48, CUS_05 42, CUS_06 18 (sobre filas ganadoras) |
| products · madre | 648 | 27 | 0 | 621 | 0 |
| products · hija | 408 | 16 | 0 | 392 | PRD_02 2, PRD_04 4, PRD_05 3 |
| prices · madre | 648 | 648 | 0 | 0 | 0 |
| prices · hija | 384 | 360 | 24 (PRC_05) | 0 | PRC_03 296, PRC_04 24 |
| orders · madre | 699.699 | 699.699 | 0 | 0 | 0 (todos con `line_amount`) |
| orders · hija | 305.943 | 296.754 | 4.633 (ORD_03) | 4.556 | ORD_06 301.387, ORD_07 4.456, ORD_10 18.995 (sin precio) |

Más Silver contiene las 2 filas `-1` de customers y 2 de products (cuentan aparte). Nota: la cuarentena puede traer la misma fila de negocio en lotes distintos (el duplicado reenviado al día siguiente se cuarentena de nuevo en su lote); el conteo de arriba es en `full`, donde ya está deduplicada. Los conteos «sobre filas ganadoras» de customers/products son distintos de los del manifest (que cuenta filas emitidas, copias incluidas); el test de pytest compara contra el manifest sobre filas emitidas.

## 6. Dudas para los expertos

1. **`line_amount` en Silver o solo en Gold** (D20): hoy en Silver con precio vigente del mes. ¿Se congela al cargar o Gold lo recalcula con `dim_price` con vigencia?
2. **Precio no numérico: cuarentena vs 0 marcado** (D18). Con cuarentena, 18.995 líneas de la hija quedan con `line_amount` NULL (los 24 producto-mes sin precio).
3. **Huérfanos: ¿`-1` con bandera (D19) o cuarentena** como dice el diseño? Afecta si la venta con cliente desconocido cuenta en los ingresos de Gold (hoy sí).
4. **Moneda**: ¿PEN (dato real) o INR por herencia de documentos? (D17)
5. **Ciudad nula**: ¿aceptan `Unknown` o hay una fuente para recuperarla? (D23)
6. **Formato `dd/MM/yyyy` vs `MM/dd/yyyy`**: se asumió día primero (así lo emite datagen); en datos reales sería ambiguo cuando día ≤ 12.
7. ¿SCD2 de clientes/productos desde Silver (CDF ya activo en esas dos tablas) o solo en Gold?
8. ¿Lakeflow Declarative Pipelines con *expectations* en lugar de este job? (abierto en §5.2 del diseño; este código es la variante «job + reglas propias»).

## 7. Qué quedó validado y qué no

**Validado localmente (pytest, 101 en total):**
- Las 34 reglas puras: casos borde (4 formatos de fecha + ISO y «Tuesday, July 01, 2025», precio negativo/no numérico/nulo, qty nula/0/negativa/no entera, `customer_id` inválido, `product_id` `PX-`/no numérico, dups con distinto `_ingest_ts` y empate).
- Cada anomalía sembrada por datagen es detectada por su regla, con conteo exacto igual al manifest; la madre limpia no dispara **ninguna** regla.
- Cobertura de las tablas de referencia contra datagen; DDL/siembra coherentes y `01_silver_ddl.sql` sincronizado con el generador; columnas crudas = registro de Bronze.
- Estática de `transform.py`: compila, usa exactamente las 34 reglas, sin RDD/`sparkContext`/`cache`/`persist`/overwrite; `run.py` (argumentos); orden de dependencias.
- `resources/silver_job.yml` es YAML válido y `databricks bundle validate -t dev` (sobre una copia temporal con el `include` y el `sync`) devolvió OK con las dependencias esperadas.

**Pendiente de correr en Spark / workspace (no afirmado):**
- Toda la ejecución de `transform.py` (no hay Java ni pyspark local). Puntos de riesgo a mirar primero:
  - `F.call_function("try_to_date"/"try_element_at", ...)` (pyspark ≥ 3.5) y `F.array_compact`/`F.get` (≥ 3.4) en el runtime serverless `environment_version 3`.
  - Patrón `MMMM d, yyyy` con `try_to_date` (nombres de mes en inglés dependen del locale de Spark) y la política de parser de fechas.
  - ANSI mode: todo cast está protegido por regex; confirmar que ningún dato real lo rompe.
  - `MERGE ... UPDATE SET * / INSERT *` desde una vista temporal con ventanas y `current_timestamp()` (el MERGE puede leer el origen dos veces; el dedup es determinista, pero verificar).
  - `DELETE` + `append` de cuarentena (no atómico) y reintentos.
  - Paridad Spark↔puro: las reglas están escritas dos veces (expresiones Spark y funciones Python); la reconciliación de §5 es la prueba.
  - Rendimiento a 1 M de líneas: cada etapa recalcula desde Bronze (sin caché, por serverless); si es lento, escribir una tabla intermedia.
- Idempotencia (2 corridas iguales) y modo incremental (segundo lote solo toca lo afectado).
- Ejecutar el DDL/siembra y que los miembros `-1` y `ref_*` queden como se espera.
- Enlazar el job en `databricks.yml` (lo hace la sesión principal) y desplegar.

## 8. Riesgos

- Staging conserva solo el último lote con datos (D9): si Silver falla y corre otro Bronze antes del reintento, ese lote intermedio se pierde para `incremental`; remedio: `mode=full` (MERGE, idempotente).
- `mode=full` no borra de Silver lo que ya no esté en Bronze (Bronze es append-only, así que no ocurre).
- `orders` depende de que `customers`, `products` y `prices` ya estén en Silver *en la misma corrida*; correr solo `orders` sobre dimensiones desactualizadas produce `-1`/`price_missing` espurios.
