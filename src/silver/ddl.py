"""DDL de Silver (tablas, cuarentena, ops.dq_results, ref_*) y su siembra. Fuente unica de columnas.

`sql/01_silver_ddl.sql` se GENERA desde aqui (un test verifica que coincide):
    cd src && python -m silver.ddl fmcg_lakehouse > silver/sql/01_silver_ddl.sql
`transform.setup()` ejecuta las mismas sentencias (todas idempotentes).
"""
from __future__ import annotations

from . import reference_data as ref

LINEAGE = [
    ("_source_file", "STRING", "archivo de landing del que viene la fila ganadora"),
    ("_batch_id", "STRING", "lote de Bronze"),
    ("_ingest_ts", "TIMESTAMP", "momento de ingesta en Bronze; decide el dedup"),
    ("_silver_updated_at", "TIMESTAMP", "ultima escritura en Silver"),
]

# entidad -> (clave de negocio, columnas (nombre, tipo, comentario))
SILVER = {
    "customers": (["company", "customer_id"], [
        ("company", "STRING", "madre | hija"),
        ("customer_id", "STRING", "clave natural; '-1' = miembro desconocido"),
        ("customer_key", "STRING", "clave de negocio conformada company:customer_id"),
        ("customer_name", "STRING", "trim + colapso de espacios + initcap"),
        ("city", "STRING", "ciudad canonica (ref_city_mapping) o 'Unknown'"),
        ("customer_label", "STRING", "nombre-ciudad (como la referencia)"),
        ("market", "STRING", "de ref_company"),
        ("channel", "STRING", "de ref_company"),
        ("name_was_normalized", "BOOLEAN", "CUS_03/CUS_04"),
        ("city_was_corrected", "BOOLEAN", "CUS_05"),
        ("city_was_null", "BOOLEAN", "CUS_06"),
        ("city_is_unmapped", "BOOLEAN", "CUS_07"),
        ("is_unknown_member", "BOOLEAN", "fila del miembro desconocido"),
    ]),
    "products": (["company", "product_id"], [
        ("company", "STRING", "madre | hija"),
        ("product_id", "STRING", "clave natural numerica sin prefijo; '-1' = desconocido"),
        ("product_key", "STRING", "clave de negocio conformada company:product_id"),
        ("product_id_raw", "STRING", "id tal cual llego de Bronze"),
        ("product_id_was_prefixed", "BOOLEAN", "PRD_02"),
        ("product_code", "STRING", "sha2(product_name, 256)"),
        ("product_name", "STRING", "nombre completo corregido (typos)"),
        ("product", "STRING", "nombre sin la variante entre parentesis"),
        ("variant", "STRING", "texto entre parentesis"),
        ("category", "STRING", "categoria canonica (ref_category_division)"),
        ("division", "STRING", "division por categoria o 'Unknown'"),
        ("name_was_corrected", "BOOLEAN", "PRD_04"),
        ("category_was_normalized", "BOOLEAN", "PRD_05"),
        ("division_was_unknown", "BOOLEAN", "PRD_06"),
        ("is_unknown_member", "BOOLEAN", "fila del miembro desconocido"),
    ]),
    "prices": (["company", "product_id", "price_month"], [
        ("company", "STRING", "madre | hija"),
        ("product_id", "STRING", "clave natural del producto"),
        ("product_key", "STRING", "company:product_id"),
        ("product_code", "STRING", "de silver.products (NULL si no resuelve)"),
        ("price_month", "DATE", "primer dia del mes de vigencia"),
        ("unit_price", "DECIMAL(12,2)", "precio unitario > 0 (negativos en valor absoluto)"),
        ("currency", "STRING", "moneda estandarizada"),
        ("price_was_negative", "BOOLEAN", "PRC_04"),
        ("price_was_invalid", "BOOLEAN", "PRC_05 si la accion fuera 'marcar' (precio 0)"),
        ("month_was_reformatted", "BOOLEAN", "PRC_03"),
        ("product_unresolved", "BOOLEAN", "PRC_07"),
    ]),
    "orders": (["company", "order_id", "line_no"], [
        ("company", "STRING", "madre | hija"),
        ("order_id", "STRING", "clave natural del pedido"),
        ("line_no", "INT", "numero de linea dentro del pedido"),
        ("order_date", "DATE", "fecha parseada de 5 formatos"),
        ("order_year", "INT", "parte de fecha"),
        ("order_quarter", "INT", "parte de fecha"),
        ("order_month", "INT", "parte de fecha"),
        ("order_month_start", "DATE", "primer dia del mes (une con prices.price_month)"),
        ("is_weekend", "BOOLEAN", "sabado o domingo"),
        ("customer_id", "STRING", "'-1' si invalido o no resuelto"),
        ("product_id", "STRING", "'-1' si no resuelto"),
        ("order_qty", "INT", "entero > 0"),
        ("unit_price", "DECIMAL(12,2)", "precio vigente del mes (NULL si no hay)"),
        ("line_amount", "DECIMAL(18,2)", "order_qty * unit_price; NULL + price_missing si no hay precio"),
        ("currency", "STRING", "moneda estandarizada"),
        ("date_was_reformatted", "BOOLEAN", "ORD_06"),
        ("customer_id_was_invalid", "BOOLEAN", "ORD_07"),
        ("customer_unresolved", "BOOLEAN", "ORD_08"),
        ("product_unresolved", "BOOLEAN", "ORD_09"),
        ("price_missing", "BOOLEAN", "ORD_10"),
    ]),
}
ENTITIES = list(SILVER)

# Columnas crudas de Bronze por entidad (un test las compara con el registro de Bronze).
RAW_COLUMNS = {
    "customers": ["customer_id", "customer_name", "city"],
    "products": ["product_id", "product_name", "category"],
    "prices": ["product_id", "price_month", "unit_price"],
    "orders": ["order_id", "line_no", "order_date", "customer_id", "product_id", "order_qty"],
}
QUARANTINE_META = [
    ("_company", "STRING"), ("_source_file", "STRING"), ("_ingest_ts", "TIMESTAMP"), ("_batch_id", "STRING"),
    ("rule_id", "STRING"), ("reason", "STRING"), ("quarantined_at", "TIMESTAMP"), ("silver_run_id", "STRING"),
]
DQ_COLUMNS = [
    ("batch_id", "STRING", "id de la corrida de Silver (no el de Bronze)"),
    ("entity", "STRING", "customers | products | prices | orders"),
    ("company", "STRING", "madre | hija"),
    ("rule_id", "STRING", "ver silver/rules.py"),
    ("rows_evaluated", "BIGINT", "poblacion de la etapa donde se evalua la regla"),
    ("rows_failed", "BIGINT", "filas que dispararon la regla"),
    ("action", "STRING", "corregir | marcar | cuarentena"),
    ("mode", "STRING", "incremental | full"),
    ("ts", "TIMESTAMP", "momento de la corrida"),
]
MERGE_KEY = {e: k for e, (k, _) in SILVER.items()}


def columns(entity: str) -> list[str]:
    """Orden fisico de la tabla Silver: negocio + linaje."""
    return [c for c, _, _ in SILVER[entity][1]] + [c for c, _, _ in LINEAGE]


def _q(v: str) -> str:
    return "'" + v.replace("'", "''") + "'"


def _cols_sql(cols) -> str:
    return ",\n  ".join(f"`{c[0]}` {c[1]}" + (f" COMMENT {_q(c[2])}" if len(c) > 2 else "") for c in cols)


def silver_table_ddl(catalog: str, entity: str) -> str:
    key, cols = SILVER[entity]
    props = "'delta.enableChangeDataFeed' = 'true'" if entity in ("customers", "products") else \
            "'delta.autoOptimize.optimizeWrite' = 'true'"
    return (f"CREATE TABLE IF NOT EXISTS {catalog}.silver.{entity} (\n  {_cols_sql(cols + LINEAGE)}\n)\n"
            f"COMMENT {_q('Silver: clave de negocio = ' + ', '.join(key) + ' (sin PK fisica; la garantiza el MERGE)')}\n"
            f"TBLPROPERTIES ({props})")


def quarantine_table_ddl(catalog: str, entity: str) -> str:
    cols = [(c, "STRING") for c in RAW_COLUMNS[entity]] + QUARANTINE_META
    return (f"CREATE TABLE IF NOT EXISTS {catalog}.quarantine.{entity} (\n  {_cols_sql(cols)}\n)\n"
            f"COMMENT {_q('Filas rechazadas de Silver: fila cruda de Bronze + rule_id + reason + _batch_id (1 fila por fila rechazada)')}")


def dq_table_ddl(catalog: str) -> str:
    return (f"CREATE TABLE IF NOT EXISTS {catalog}.ops.dq_results (\n  {_cols_sql(DQ_COLUMNS)}\n)\n"
            f"COMMENT {_q('Resultados de calidad por corrida, entidad, compania y regla')}")


REF_DDL = {
    "ref_city_mapping": "CREATE TABLE IF NOT EXISTS {c}.silver.ref_city_mapping (\n"
                        "  raw_city_key STRING COMMENT 'lower(trim(city)) como llega',\n"
                        "  city STRING COMMENT 'ciudad canonica',\n  updated_at TIMESTAMP)",
    "ref_category_division": "CREATE TABLE IF NOT EXISTS {c}.silver.ref_category_division (\n"
                             "  category_key STRING COMMENT 'lower(trim(category))',\n"
                             "  category STRING COMMENT 'categoria canonica',\n"
                             "  division STRING, updated_at TIMESTAMP)",
    "ref_typo_fixes": "CREATE TABLE IF NOT EXISTS {c}.silver.ref_typo_fixes (\n"
                      "  entity STRING, column_name STRING, wrong_term STRING, correct_term STRING,\n"
                      "  updated_at TIMESTAMP)",
    "ref_company": "CREATE TABLE IF NOT EXISTS {c}.silver.ref_company (\n"
                   "  company STRING, source_system STRING, market STRING, channel STRING,\n"
                   "  updated_at TIMESTAMP)",
}

REF_KEYS = {
    "ref_city_mapping": ["raw_city_key"], "ref_category_division": ["category_key"],
    "ref_typo_fixes": ["entity", "column_name", "wrong_term"], "ref_company": ["company"],
}


def ref_rows() -> dict[str, tuple[list[str], list[tuple]]]:
    return {
        "ref_city_mapping": (["raw_city_key", "city"], sorted(ref.CITY_MAPPING.items())),
        "ref_category_division": (["category_key", "category", "division"],
                                  [(k, c, d) for k, (c, d) in sorted(ref.CATEGORY_DIVISION.items())]),
        "ref_typo_fixes": (["entity", "column_name", "wrong_term", "correct_term"], list(ref.TYPO_FIXES)),
        "ref_company": (["company", "source_system", "market", "channel"], list(ref.COMPANY_ATTRS)),
    }


def ref_seed_sql(catalog: str, table: str) -> str:
    """MERGE que SOLO inserta lo que falta (no pisa ediciones de los expertos)."""
    cols, rows = ref_rows()[table]
    values = ",\n".join("  (" + ", ".join(_q(v) for v in r) + ")" for r in rows)
    on = " AND ".join(f"t.{k} = s.{k}" for k in REF_KEYS[table])
    ins_cols = ", ".join(cols + ["updated_at"])
    ins_vals = ", ".join([f"s.{c}" for c in cols] + ["current_timestamp()"])
    return (f"MERGE INTO {catalog}.silver.{table} AS t\nUSING (SELECT * FROM VALUES\n{values}\n"
            f"  AS v({', '.join(cols)})) AS s\nON {on}\n"
            f"WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_vals})")


def unknown_member_sql(catalog: str, entity: str) -> str:
    """MERGE del miembro desconocido ('-1') por compania; existe en la dimension para los huerfanos."""
    over = {
        "customers": {"customer_id": "'-1'", "customer_key": "concat(c.company, ':-1')",
                      "customer_name": "'Unknown'", "city": "'Unknown'", "customer_label": "'Unknown'",
                      "market": "'Unknown'", "channel": "'Unknown'"},
        "products": {"product_id": "'-1'", "product_key": "concat(c.company, ':-1')",
                     "product_id_raw": "'-1'", "product_name": "'Unknown'", "product": "'Unknown'",
                     "category": "'Unknown'", "division": "'Unknown'"},
    }[entity]
    sel = []
    for name, typ, _ in SILVER[entity][1]:
        if name == "company":
            expr = "c.company"
        elif name == "is_unknown_member":
            expr = "true"
        elif name in over:
            expr = over[name]
        elif typ == "BOOLEAN":
            expr = "false"
        else:
            expr = "CAST(NULL AS " + typ + ")"
        sel.append(f"    {expr} AS {name}")
    sel += ["    'seed' AS _source_file", "    'seed' AS _batch_id",
            "    current_timestamp() AS _ingest_ts", "    current_timestamp() AS _silver_updated_at"]
    key = MERGE_KEY[entity]
    on = " AND ".join(f"t.{k} = s.{k}" for k in key)
    return (f"MERGE INTO {catalog}.silver.{entity} AS t\nUSING (\n  SELECT\n" + ",\n".join(sel) +
            "\n  FROM (VALUES ('madre'), ('hija')) AS c(company)\n) AS s\n"
            f"ON {on}\nWHEN NOT MATCHED THEN INSERT *")


def setup_statements(catalog: str) -> list[str]:
    """Todo lo idempotente que Silver necesita antes de correr, en orden."""
    out = [silver_table_ddl(catalog, e) for e in ENTITIES]
    out += [quarantine_table_ddl(catalog, e) for e in ENTITIES]
    out.append(dq_table_ddl(catalog))
    out += [d.format(c=catalog) for d in REF_DDL.values()]
    out += [ref_seed_sql(catalog, t) for t in REF_DDL]
    out += [unknown_member_sql(catalog, e) for e in ("customers", "products")]
    return out


HEADER = (
    "-- NO EJECUTADO — requiere confirmacion de Manu (crea tablas en Unity Catalog y siembra datos).\n"
    "-- GENERADO desde src/silver/ddl.py; no editar a mano:\n"
    "--   cd src && python -m silver.ddl fmcg_lakehouse > silver/sql/01_silver_ddl.sql\n"
    "-- Requiere los esquemas silver, quarantine y ops (infra/02-unity-catalog.sql). Todo es idempotente.\n"
    "-- Orden: tablas Silver, cuarentena, ops.dq_results, ref_*, siembra de ref_* y miembros -1.\n"
)


def render_sql(catalog: str) -> str:
    return HEADER + "\n" + ";\n\n".join(setup_statements(catalog)) + ";\n"


if __name__ == "__main__":
    import sys
    print(render_sql(sys.argv[1] if len(sys.argv) > 1 else "fmcg_lakehouse"), end="")
