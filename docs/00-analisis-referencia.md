# Análisis del repo de referencia

Repo: `takadlag/databricks-medallion-lakehouse-fmcg` @ `5e99b2b` (clon local de solo lectura, fuera de este repo).

## Qué es
Pipeline medallion para una empresa **hija** ("Sports Bar", India, precios en INR) que se integra por `MERGE` con tablas de una empresa **madre**:

```
S3 landing (customers, products, gross_price, orders)
 -> Bronze (Delta crudo + read_timestamp, file_name, file_size)
 -> Silver (dedup, tipos, typos, fechas multi-formato, joins)
 -> Gold sb_* (hija)
 -> MERGE a madre: dim_customers, dim_products, dim_gross_price, fact_orders
 -> Dashboard SQL "Sales Insights" + Genie
```

## Reglas de negocio por fuente
- **customers**: dedup por `customer_id`, trim + initcap, typos de ciudad (Bengaluru/Hyderabad/New Delhi), 4 ciudades nulas corregidas por diccionario, `customer = nombre-ciudad`, market=India, platform=Sports Bar, channel=Acquisition.
- **products**: `Protien`->`Protein`, `division` por categoría, `variant` entre paréntesis, `product_code = sha2(product_name)`, `product_id` no numérico -> 999999.
- **gross_price**: `month` en 4 formatos, negativos -> positivos, no numéricos -> 0; último precio no-cero por producto y año.
- **orders**: landing -> processed, carga full e incremental (staging), fechas en 4 formatos (incl. "Tuesday, July 01, 2025"), `customer_id` inválido -> 999999, agregación diaria -> mensual por (date, product_code, customer_code).
- **dim_date**: grano mensual 2024-01 a 2025-12.

## Brechas de la referencia (a cubrir)
1. No hay datos (bucket S3 propio del autor).
2. Las tablas madre (`dim_customers`, `dim_products`, `dim_gross_price`, `fact_orders`) se asumen existentes; nadie las crea.
3. El dashboard usa `total_amount_inr` (qty x precio); ese gold no existe en el repo.
4. README promete `bronze.py/silver.py/gold.py`, `requirements.txt`, GitHub Actions, Workflows, Terraform: nada existe.
5. Rutas fijas (`/Workspace/consolidated_pipeline/...`, `s3://sportsbar-final`, catálogo `fmcg`).
6. Sin pruebas de calidad reales.

## Problemas de diseño a corregir
- `dim_gross_price` se une solo por `product_code`: el año nuevo pisa al anterior (se pierde historial).
- `INNER JOIN` con products descarta pedidos huérfanos; `customer_id=999999` no existe en la dimensión.
- `product_id` inválido -> 999999 hace colisionar productos distintos.
- `dropDuplicates` sin criterio de "más reciente" (elige fila arbitraria).
- Incremental frágil: `dbutils.fs.mv` + tablas staging que se borran al final.
- Bronze: `overwrite` en dimensiones y `append` en pedidos, sin política explícita.

## Alcance de nuestra solución (completa)
1. Generador de datos sintéticos reproducible (semilla fija) con anomalías documentadas.
2. Medallion en ADLS Gen2 + Unity Catalog; tablas madre e hija; ingesta incremental (Auto Loader / COPY INTO).
3. Calidad de datos: reglas por capa + cuarentena/rechazos.
4. Gold en estrella + KPIs (ingresos, cantidad, top productos, mix por categoría).
5. Orquestación (Workflows / Asset Bundles), CI mínimo, README y diagramas veraces.
6. Dashboard equivalente, con capturas.
