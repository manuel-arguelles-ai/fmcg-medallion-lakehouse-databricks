"""DDL de Gold (estrella + agregado + features ML + vistas de KPI). Fuente unica de columnas.

`sql/01_gold_ddl.sql` se GENERA desde aqui (un test verifica que coincide):
    cd src && python -m gold.ddl fmcg_lakehouse > gold/sql/01_gold_ddl.sql
`transform.setup()` ejecuta las mismas sentencias (todas idempotentes: CREATE TABLE IF NOT EXISTS y
CREATE OR REPLACE VIEW). Las tablas se recargan completas desde Silver con INSERT OVERWRITE.
Moneda: Gold NO fija ninguna; los montos llevan al lado la columna `currency` que viene de Silver.
"""
from __future__ import annotations

from silver import rules as silver_rules

from . import model

AUDIT = [("_silver_updated_at", "TIMESTAMP", "ultima escritura de la fila origen en Silver"),
         ("_gold_loaded_at", "TIMESTAMP", "momento de la recarga de Gold")]

# tabla -> (clave de negocio (unica), columnas (nombre, tipo, comentario), CLUSTER BY)
GOLD = {
    "dim_date": (["date_sk"], [
        ("date_sk", "INT", "yyyymmdd; -1 = desconocido"),
        ("date", "DATE", "dia calendario"),
        ("year", "INT", ""), ("quarter", "INT", "1..4"), ("quarter_label", "STRING", "Q1..Q4"),
        ("month", "INT", "1..12"), ("month_name", "STRING", "nombre del mes en espanol"),
        ("year_month", "STRING", "YYYY-MM"), ("month_start", "DATE", "primer dia del mes"),
        ("month_end", "DATE", "ultimo dia del mes"), ("day", "INT", "dia del mes"),
        ("day_of_week", "INT", "ISO: 1 = lunes ... 7 = domingo"), ("day_name", "STRING", "en espanol"),
        ("is_weekend", "BOOLEAN", "sabado o domingo"), ("week_of_year", "INT", "semana ISO"),
        ("day_of_year", "INT", "1..366"), ("is_month_end", "BOOLEAN", "ultimo dia del mes"),
        ("is_unknown_member", "BOOLEAN", "fila del miembro desconocido"),
    ], None),
    "dim_customer": (["company", "customer_id"], [
        ("customer_sk", "BIGINT", "clave sustituta = xxhash64(customer_key), estable entre recargas; -1 = desconocido"),
        ("company", "STRING", "madre | hija ('Unknown' en el miembro -1)"),
        ("customer_id", "STRING", "clave natural"),
        ("customer_key", "STRING", "clave de negocio company:customer_id"),
        ("customer_name", "STRING", "nombre normalizado en Silver"),
        ("city", "STRING", "ciudad canonica o 'Unknown'"),
        ("customer_label", "STRING", "nombre-ciudad"),
        ("market", "STRING", "de silver.ref_company"),
        ("channel", "STRING", "de silver.ref_company"),
        ("name_was_normalized", "BOOLEAN", "Silver CUS_03/CUS_04"),
        ("city_was_corrected", "BOOLEAN", "Silver CUS_05"),
        ("city_was_null", "BOOLEAN", "Silver CUS_06"),
        ("city_is_unmapped", "BOOLEAN", "Silver CUS_07"),
        ("is_unknown_member", "BOOLEAN", "fila del miembro desconocido"),
    ], None),
    "dim_product": (["company", "product_id"], [
        ("product_sk", "BIGINT", "clave sustituta = xxhash64(product_key); -1 = desconocido"),
        ("company", "STRING", "madre | hija ('Unknown' en el miembro -1)"),
        ("product_id", "STRING", "clave natural"),
        ("product_key", "STRING", "clave de negocio company:product_id"),
        ("product_code", "STRING", "sha2(product_name) de Silver"),
        ("product_name", "STRING", "nombre completo"),
        ("product", "STRING", "nombre sin variante"),
        ("variant", "STRING", "variante (texto entre parentesis)"),
        ("category", "STRING", "categoria canonica"),
        ("division", "STRING", "division por categoria o 'Unknown'"),
        ("product_id_was_prefixed", "BOOLEAN", "Silver PRD_02"),
        ("name_was_corrected", "BOOLEAN", "Silver PRD_04"),
        ("category_was_normalized", "BOOLEAN", "Silver PRD_05"),
        ("division_was_unknown", "BOOLEAN", "Silver PRD_06"),
        ("is_unknown_member", "BOOLEAN", "fila del miembro desconocido"),
    ], None),
    "dim_price": (["company", "product_id", "valid_from"], [
        ("price_sk", "BIGINT", "clave sustituta = xxhash64(product_key|valid_from); -1 = sin precio"),
        ("product_sk", "BIGINT", "dim_product (-1 si el producto no resuelve)"),
        ("company", "STRING", "madre | hija"),
        ("product_id", "STRING", "clave natural del producto"),
        ("product_key", "STRING", "company:product_id"),
        ("valid_from", "DATE", "inicio de vigencia (primer dia del mes del precio)"),
        ("valid_to", "DATE", "fin de vigencia (ultimo dia del mismo mes; no se estira a meses sin precio)"),
        ("unit_price", "DECIMAL(12,2)", "precio unitario vigente"),
        ("currency", "STRING", "moneda de Silver (no se fija en Gold)"),
        ("prev_unit_price", "DECIMAL(12,2)", "precio del mes calendario anterior (NULL si no hubo)"),
        ("price_change_pct", "DECIMAL(12,4)", "unit_price / prev_unit_price - 1"),
        ("price_was_negative", "BOOLEAN", "Silver PRC_04"),
        ("price_was_invalid", "BOOLEAN", "Silver PRC_05 (si la accion fuera marcar)"),
        ("month_was_reformatted", "BOOLEAN", "Silver PRC_03"),
        ("product_unresolved", "BOOLEAN", "Silver PRC_07"),
        ("is_unknown_member", "BOOLEAN", "fila del miembro desconocido"),
    ], None),
    "fact_sales": (["company", "order_id", "line_no"], [
        ("company", "STRING", "madre | hija (dimension degenerada)"),
        ("order_id", "STRING", "dimension degenerada"),
        ("line_no", "INT", "dimension degenerada"),
        ("date_sk", "INT", "dim_date (yyyymmdd)"),
        ("customer_sk", "BIGINT", "dim_customer (-1 = desconocido)"),
        ("product_sk", "BIGINT", "dim_product (-1 = desconocido)"),
        ("price_sk", "BIGINT", "dim_price vigente en order_date (-1 = sin precio)"),
        ("order_date", "DATE", "fecha del pedido"),
        ("order_month_start", "DATE", "primer dia del mes del pedido"),
        ("order_qty", "INT", "cantidad"),
        ("unit_price", "DECIMAL(12,2)", "precio vigente (de Silver)"),
        ("line_amount", "DECIMAL(18,2)", "order_qty * unit_price, calculado en Silver (D28); NULL si no hay precio"),
        ("currency", "STRING", "moneda de line_amount (de Silver)"),
        ("price_missing", "BOOLEAN", "Silver ORD_10"),
        ("customer_id_was_invalid", "BOOLEAN", "Silver ORD_07"),
        ("customer_unresolved", "BOOLEAN", "Silver ORD_08"),
        ("product_unresolved", "BOOLEAN", "Silver ORD_09"),
        ("date_was_reformatted", "BOOLEAN", "Silver ORD_06"),
    ], ["order_month_start", "product_sk"]),
    "agg_sales_monthly": (["month_start", "company", "customer_sk", "product_sk", "currency"], [
        ("month_start", "DATE", "mes (primer dia)"),
        ("year", "INT", ""), ("quarter", "INT", ""), ("month", "INT", ""),
        ("company", "STRING", "madre | hija"),
        ("customer_sk", "BIGINT", "dim_customer"),
        ("product_sk", "BIGINT", "dim_product"),
        ("currency", "STRING", "moneda de revenue"),
        ("order_lines", "BIGINT", "lineas de pedido"),
        ("orders", "BIGINT", "pedidos distintos (NO aditivo entre productos/clientes)"),
        ("total_qty", "BIGINT", "suma de order_qty"),
        ("revenue", "DECIMAL(28,2)", "suma de line_amount (lineas sin precio no suman)"),
        ("lines_without_price", "BIGINT", "lineas con price_missing"),
    ], ["month_start", "product_sk"]),
    "feature_sales_monthly": (["month_start", "company", "customer_sk", "product_sk", "currency"], [
        ("month_start", "DATE", "mes de la observacion"),
        ("company", "STRING", "madre | hija"),
        ("customer_sk", "BIGINT", "dim_customer"),
        ("product_sk", "BIGINT", "dim_product"),
        ("currency", "STRING", "moneda de los montos"),
        ("revenue", "DECIMAL(28,2)", "ingreso del mes"),
        ("total_qty", "BIGINT", "cantidad del mes"),
        ("order_lines", "BIGINT", "lineas del mes"),
        ("revenue_lag_1", "DECIMAL(28,2)", "ingreso de hace 1 mes calendario (0 si no compro)"),
        ("revenue_lag_2", "DECIMAL(28,2)", "hace 2 meses"),
        ("revenue_lag_3", "DECIMAL(28,2)", "hace 3 meses"),
        ("qty_lag_1", "BIGINT", "cantidad de hace 1 mes (0 si no compro)"),
        ("qty_lag_2", "BIGINT", "hace 2 meses"),
        ("qty_lag_3", "BIGINT", "hace 3 meses"),
        ("revenue_avg_3m", "DECIMAL(28,2)", "promedio de los 3 meses previos (sin el actual)"),
        ("months_since_prev_purchase", "INT", "meses desde la compra anterior (NULL si es la primera)"),
        ("purchase_month_seq", "INT", "n-esimo mes con compras del par cliente-producto"),
        ("target_revenue_next_month", "DECIMAL(28,2)", "ETIQUETA: ingreso del mes siguiente; NULL si aun no se conoce"),
    ], ["month_start", "customer_sk"]),
}
TABLES = list(GOLD)
DIMENSIONS = ["dim_date", "dim_customer", "dim_product", "dim_price"]
GOLD_RUNS = [
    ("run_id", "STRING", "id de la corrida de Gold"),
    ("task", "STRING", "tabla/tarea"),
    ("target_table", "STRING", "tabla escrita"),
    ("rows_written", "BIGINT", "filas tras el INSERT OVERWRITE"),
    ("checks", "STRING", "reconciliaciones hechas (texto)"),
    ("started_at", "TIMESTAMP", ""), ("finished_at", "TIMESTAMP", ""),
    ("status", "STRING", "ok | failed"), ("error", "STRING", ""),
]


def columns(table: str) -> list[str]:
    cols = [c for c, _, _ in GOLD[table][1]]
    return cols + [c for c, _, _ in AUDIT]


def types(table: str) -> dict[str, str]:
    return {c: t for c, t, _ in GOLD[table][1] + AUDIT}


def _q(v: str) -> str:
    return "'" + v.replace("'", "''") + "'"


def _cols_sql(cols) -> str:
    return ",\n  ".join(f"`{c}` {t}" + (f" COMMENT {_q(m)}" if m else "") for c, t, m in cols)


COMMENTS = {
    "dim_date": "Calendario diario (anios completos que cubren los pedidos) + miembro -1",
    "dim_customer": "Clientes SCD1 (estado actual de Silver) + miembro -1",
    "dim_product": "Productos SCD1 (estado actual de Silver) + miembro -1",
    "dim_price": "Precio por producto con vigencia mensual (valid_from/valid_to) + miembro -1",
    "fact_sales": "Hecho de ventas, grano linea de pedido; monto = line_amount de Silver",
    "agg_sales_monthly": "Agregado mes x producto x cliente derivado de fact_sales (reconstruible)",
    "feature_sales_monthly": "Features ML mes x cliente x producto con lags de calendario y etiqueta del mes siguiente",
}


def table_ddl(catalog: str, table: str) -> str:
    key, cols, cluster = GOLD[table]
    cl = f"\nCLUSTER BY ({', '.join(cluster)})" if cluster else ""
    return (f"CREATE TABLE IF NOT EXISTS {catalog}.gold.{table} (\n  {_cols_sql(cols + AUDIT)}\n){cl}\n"
            f"COMMENT {_q(COMMENTS[table] + '. Clave unica: ' + ', '.join(key) + '. Recarga completa (INSERT OVERWRITE)')}")


def gold_runs_ddl(catalog: str) -> str:
    return (f"CREATE TABLE IF NOT EXISTS {catalog}.ops.gold_runs (\n  {_cols_sql(GOLD_RUNS)}\n)\n"
            f"COMMENT {_q('Una fila por tarea de Gold: filas escritas y reconciliaciones')}")


# ------------------------------------------------------------------ miembros desconocidos
UNKNOWN_OVERRIDES = {
    "dim_customer": {"customer_sk": str(model.UNKNOWN_SK), "company": "'Unknown'", "customer_id": "'-1'",
                     "customer_key": "'Unknown:-1'", "customer_name": "'Unknown'", "city": "'Unknown'",
                     "customer_label": "'Unknown'", "market": "'Unknown'", "channel": "'Unknown'"},
    "dim_product": {"product_sk": str(model.UNKNOWN_SK), "company": "'Unknown'", "product_id": "'-1'",
                    "product_key": "'Unknown:-1'", "product_name": "'Unknown'", "product": "'Unknown'",
                    "category": "'Unknown'", "division": "'Unknown'"},
    "dim_price": {"price_sk": str(model.UNKNOWN_SK), "product_sk": str(model.UNKNOWN_SK),
                  "company": "'Unknown'", "product_id": "'-1'", "product_key": "'Unknown:-1'"},
}


def unknown_member_select(table: str) -> str:
    """SELECT de una fila con el miembro -1 (se une a cada recarga de la dimension)."""
    over = UNKNOWN_OVERRIDES[table]
    sel = []
    for name, typ, _ in GOLD[table][1] + AUDIT:
        if name == "is_unknown_member":
            expr = "true"
        elif name in over:
            expr = over[name]
        elif typ == "BOOLEAN":
            expr = "false"
        elif name == "_gold_loaded_at":
            expr = "current_timestamp()"
        else:
            expr = f"CAST(NULL AS {typ})"
        sel.append(f"{expr} AS `{name}`")
    return "SELECT " + ", ".join(sel)


# ------------------------------------------------------------------ vistas de KPI
def _rules_values() -> str:
    rows = ",\n    ".join(f"({_q(r.rule_id)}, {_q(r.entity)}, {_q(r.description)}, {_q(r.severity)})"
                          for r in silver_rules.RULES)
    return f"VALUES\n    {rows}\n  AS r(rule_id, entity, description, severity)"


def views(catalog: str) -> dict[str, str]:
    g, o, qn = f"{catalog}.gold", f"{catalog}.ops", f"{catalog}.quarantine"
    quarantine_union = "\n  UNION ALL\n".join(
        f"  SELECT '{e}' AS entity, _company AS company, rule_id, quarantined_at FROM {qn}.{e}"
        for e in ("customers", "products", "prices", "orders"))
    return {
        "v_kpi_sales_monthly": f"""SELECT d.year, d.quarter, d.month, d.month_start, d.year_month,
  f.company, f.currency,
  SUM(f.line_amount) AS revenue,
  SUM(f.order_qty) AS total_qty,
  COUNT(DISTINCT f.order_id) AS orders,
  COUNT(*) AS order_lines,
  COUNT_IF(f.price_missing) AS lines_without_price,
  SUM(f.line_amount) / NULLIF(COUNT(DISTINCT f.order_id), 0) AS avg_ticket
FROM {g}.fact_sales f
JOIN {g}.dim_date d ON d.date_sk = f.date_sk
GROUP BY d.year, d.quarter, d.month, d.month_start, d.year_month, f.company, f.currency""",

        "v_kpi_top10_products": f"""WITH by_period AS (
  SELECT CAST(a.year AS STRING) AS period, a.product_sk, a.currency,
         SUM(a.revenue) AS revenue, SUM(a.total_qty) AS total_qty
  FROM {g}.agg_sales_monthly a GROUP BY a.year, a.product_sk, a.currency
  UNION ALL
  SELECT 'Total' AS period, a.product_sk, a.currency, SUM(a.revenue), SUM(a.total_qty)
  FROM {g}.agg_sales_monthly a GROUP BY a.product_sk, a.currency
), ranked AS (
  SELECT b.*, RANK() OVER (PARTITION BY b.period, b.currency ORDER BY b.revenue DESC NULLS LAST) AS revenue_rank
  FROM by_period b
)
SELECT r.period, r.revenue_rank, p.company, p.product_name, p.product, p.variant, p.category, p.division,
       r.currency, r.revenue, r.total_qty, r.product_sk
FROM ranked r JOIN {g}.dim_product p ON p.product_sk = r.product_sk
WHERE r.revenue_rank <= 10""",

        "v_kpi_category_mix": f"""SELECT a.year, a.month_start, a.company, p.division, p.category, a.currency,
  SUM(a.revenue) AS revenue,
  SUM(a.total_qty) AS total_qty,
  SUM(a.revenue) / NULLIF(SUM(SUM(a.revenue)) OVER (PARTITION BY a.month_start, a.company, a.currency), 0)
    AS revenue_share
FROM {g}.agg_sales_monthly a
JOIN {g}.dim_product p ON p.product_sk = a.product_sk
GROUP BY a.year, a.month_start, a.company, p.division, p.category, a.currency""",

        "v_kpi_company_comparison": f"""WITH m AS (
  SELECT f.order_month_start AS month_start, f.company, f.currency,
         SUM(f.line_amount) AS revenue, SUM(f.order_qty) AS total_qty,
         COUNT(DISTINCT f.order_id) AS orders,
         COUNT(DISTINCT CASE WHEN f.customer_sk <> -1 THEN f.customer_sk END) AS active_customers,
         COUNT_IF(f.customer_sk = -1) AS lines_unknown_customer
  FROM {g}.fact_sales f GROUP BY f.order_month_start, f.company, f.currency
)
SELECT YEAR(m.month_start) AS year, m.*,
  m.revenue / NULLIF(m.orders, 0) AS avg_ticket,
  m.revenue / NULLIF(SUM(m.revenue) OVER (PARTITION BY m.month_start, m.currency), 0) AS revenue_share_month,
  m.revenue / NULLIF(LAG(m.revenue) OVER (PARTITION BY m.company, m.currency ORDER BY m.month_start), 0) - 1
    AS revenue_mom_pct
FROM m""",

        "v_kpi_data_quality": f"""WITH latest AS (
  SELECT entity, MAX(ts) AS ts FROM {o}.dq_results GROUP BY entity
), rules AS (
  SELECT * FROM {_rules_values()}
)
SELECT q.entity, q.company, q.rule_id, r.description, r.severity, q.action,
  q.rows_evaluated, q.rows_failed,
  q.rows_failed / NULLIF(q.rows_evaluated, 0) AS fail_rate,
  q.mode, q.batch_id, q.ts
FROM {o}.dq_results q
JOIN latest l ON l.entity = q.entity AND l.ts = q.ts
LEFT JOIN rules r ON r.rule_id = q.rule_id""",

        "v_kpi_quarantine_summary": f"""SELECT entity, company, rule_id, COUNT(*) AS rows_quarantined,
  MAX(quarantined_at) AS last_quarantined_at
FROM (
{quarantine_union}
) x
GROUP BY entity, company, rule_id""",
    }


VIEW_COMMENTS = {
    "v_kpi_sales_monthly": "KPI: ingresos, cantidad, pedidos y ticket por mes y compania",
    "v_kpi_top10_products": "KPI: top 10 productos por ingreso, por anio y Total (RANK, empates comparten puesto)",
    "v_kpi_category_mix": "KPI: mix por division/categoria y participacion en el mes por compania",
    "v_kpi_company_comparison": "KPI: comparativo madre vs hija por mes (participacion y variacion mensual)",
    "v_kpi_data_quality": "Calidad: ultima corrida de Silver por entidad (ops.dq_results) con descripcion de la regla",
    "v_kpi_quarantine_summary": "Calidad: filas en cuarentena por entidad, compania y regla",
}


def view_ddl(catalog: str, name: str) -> str:
    return (f"CREATE OR REPLACE VIEW {catalog}.gold.{name}\nCOMMENT {_q(VIEW_COMMENTS[name])}\nAS\n"
            f"{views(catalog)[name]}")


def setup_statements(catalog: str) -> list[str]:
    """Todo lo idempotente que Gold necesita antes de correr, en orden (tablas antes que vistas)."""
    out = [table_ddl(catalog, t) for t in TABLES]
    out.append(gold_runs_ddl(catalog))
    out += [view_ddl(catalog, v) for v in VIEW_COMMENTS]
    return out


HEADER = (
    "-- NO EJECUTADO — requiere confirmacion de Manu (crea tablas y vistas en Unity Catalog).\n"
    "-- GENERADO desde src/gold/ddl.py; no editar a mano:\n"
    "--   cd src && python -m gold.ddl fmcg_lakehouse > gold/sql/01_gold_ddl.sql\n"
    "-- Requiere el esquema gold (infra/02-unity-catalog.sql) y el DDL de Silver ya aplicado\n"
    "-- (las vistas de calidad leen ops.dq_results y quarantine.*). Todo es idempotente.\n"
    "-- Orden: tablas gold.*, ops.gold_runs, vistas de KPI.\n"
)


def render_sql(catalog: str) -> str:
    return HEADER + "\n" + ";\n\n".join(setup_statements(catalog)) + ";\n"


if __name__ == "__main__":
    import sys
    print(render_sql(sys.argv[1] if len(sys.argv) > 1 else "fmcg_lakehouse"), end="")
