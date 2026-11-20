"""Genera dashboards/fmcg_sales_insights.lvdash.json (Lakeview serializado).

Uso: python3 dashboards/build_fmcg_sales_insights.py dashboards/fmcg_sales_insights.lvdash.json
El JSON es la fuente que despliega infra/03-deploy-dashboard.py; este script solo lo regenera."""
import json, sys

C = "fmcg_lakehouse"
PARAMS = [("p_anio", "Año"), ("p_trimestre", "Trimestre"), ("p_mes", "Mes"), ("p_compania", "Compañía")]


def ds(name, display, sql, params=False):
    d = {"name": name, "displayName": display, "queryLines": [l + "\n" for l in sql.strip().split("\n")]}
    if params:
        d["parameters"] = [{
            "displayName": k, "keyword": k, "dataType": "STRING",
            "defaultSelection": {"values": {"dataType": "STRING", "values": [{"value": "Todos"}]}},
        } for k, _ in PARAMS]
    return d


WHERE = """(:p_anio = 'Todos' OR CAST(year AS STRING) = :p_anio)
  AND (:p_trimestre = 'Todos' OR CAST(quarter AS STRING) = :p_trimestre)
  AND (:p_mes = 'Todos' OR CAST(month AS STRING) = :p_mes)
  AND (:p_compania = 'Todos' OR company = :p_compania)"""


def filtered(sql):
    return f"SELECT * FROM (\n{sql.strip()}\n) WHERE {WHERE}"


def values_ds(name, display, expr, order):
    return ds(name, display, f"""
SELECT valor FROM (
  SELECT 'Todos' AS valor, 0 AS orden
  UNION ALL
  SELECT DISTINCT CAST({expr} AS STRING), {order} FROM {C}.gold.v_kpi_sales_monthly
) ORDER BY orden, valor""")


DATASETS = [
    ds("ds_ventas_mes", "Ventas por mes y compañía", filtered(f"""
SELECT year, quarter, month, month_start, year_month, company, currency,
  revenue, total_qty, orders, order_lines, lines_without_price
FROM {C}.gold.v_kpi_sales_monthly"""), params=True),
    ds("ds_categoria", "Mix por categoría", filtered(f"""
SELECT a.year, a.quarter, a.month, a.month_start, a.company, a.currency,
  p.division, p.category, SUM(a.revenue) AS revenue, SUM(a.total_qty) AS total_qty
FROM {C}.gold.agg_sales_monthly a
JOIN {C}.gold.dim_product p ON p.product_sk = a.product_sk
GROUP BY ALL"""), params=True),
    ds("ds_top10", "Top 10 productos (parametrizado)", f"""
WITH f AS (
  SELECT p.company, p.product_name, p.category, a.currency,
    SUM(a.revenue) AS revenue, SUM(a.total_qty) AS total_qty
  FROM {C}.gold.agg_sales_monthly a
  JOIN {C}.gold.dim_product p ON p.product_sk = a.product_sk
  WHERE (:p_anio = 'Todos' OR CAST(a.year AS STRING) = :p_anio)
    AND (:p_trimestre = 'Todos' OR CAST(a.quarter AS STRING) = :p_trimestre)
    AND (:p_mes = 'Todos' OR CAST(a.month AS STRING) = :p_mes)
    AND (:p_compania = 'Todos' OR a.company = :p_compania)
  GROUP BY p.company, p.product_name, p.category, a.currency
)
SELECT RANK() OVER (ORDER BY revenue DESC NULLS LAST) AS revenue_rank,
  CONCAT(product_name, ' (', company, ')') AS product_label, *
FROM f
QUALIFY revenue_rank <= 10""", params=True),
    ds("ds_comparativo", "Comparativo madre vs hija", filtered(f"""
SELECT year, QUARTER(month_start) AS quarter, MONTH(month_start) AS month, month_start,
  company, currency, revenue, total_qty, orders, active_customers, lines_unknown_customer,
  avg_ticket, revenue_share_month, revenue_mom_pct
FROM {C}.gold.v_kpi_company_comparison"""), params=True),
    values_ds("ds_f_anio", "Filtro: años", "year", "year"),
    values_ds("ds_f_trimestre", "Filtro: trimestres", "quarter", "quarter"),
    values_ds("ds_f_mes", "Filtro: meses", "month", "month"),
    values_ds("ds_f_compania", "Filtro: compañías", "company", "1"),
    ds("ds_calidad", "Reglas de calidad con filas fallidas (última corrida Silver)", f"""
SELECT entity, company, rule_id, description, severity, action,
  rows_evaluated, rows_failed, fail_rate, mode, batch_id, ts
FROM {C}.gold.v_kpi_data_quality
WHERE rows_failed > 0"""),
    ds("ds_cuarentena", "Cuarentena por entidad y regla", f"""
SELECT entity, company, rule_id, rows_quarantined, last_quarantined_at
FROM {C}.gold.v_kpi_quarantine_summary"""),
]

# datasets que reciben los filtros por campo
PARAM_DS = ["ds_ventas_mes", "ds_categoria", "ds_comparativo", "ds_top10"]


def fld(name, expr):
    return {"name": name, "expression": expr}


def q(name, dsname, fields, disagg=False):
    return {"name": name, "query": {"datasetName": dsname, "fields": fields, "disaggregated": disagg}}


def frame(title, desc=None):
    f = {"showTitle": True, "title": title}
    if desc:
        f["showDescription"] = True
        f["description"] = desc
    return f


def pos(x, y, w, h):
    return {"x": x, "y": y, "width": w, "height": h}


def filter_widget(name, values_ds_name, title, param, x):
    """Parametro por consulta: el campo `valor` de un dataset de valores llena el desplegable y la
    seleccion se pasa al mismo parametro en todos los datasets de ventas (no hay filtros por campo)."""
    fq = f"{name}_valores"
    queries = [q(fq, values_ds_name, [fld("valor", "`valor`")])]
    fields = [{"fieldName": "valor", "displayName": title, "queryName": fq}]
    for d in PARAM_DS:
        pq = f"{name}_param_{d}"
        queries.append({"name": pq, "query": {"datasetName": d,
                                              "parameters": [{"name": param, "keyword": param}],
                                              "disaggregated": False}})
        fields.append({"parameterName": param, "queryName": pq})
    return {"widget": {"name": name, "queries": queries,
                       "spec": {"version": 2, "widgetType": "filter-single-select",
                                "encodings": {"fields": fields}, "frame": frame(title)}},
            "position": pos(x, 0, 1, 2)}


def text(name, md, p):
    return {"widget": {"name": name, "multilineTextboxSpec": {"lines": [md]}}, "position": p}


def counter(name, dsname, expr, fname, title, p, fmt=None):
    enc = {"value": {"fieldName": fname, "displayName": title}}
    if fmt:
        enc["value"]["format"] = fmt
    return {"widget": {"name": name, "queries": [q("main_query", dsname, [fld(fname, expr)])],
                       "spec": {"version": 2, "widgetType": "counter", "encodings": enc,
                                "frame": frame(title)}},
            "position": p}


NUM = {"type": "number-plain", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 2}}
PCT = {"type": "number-percent", "decimalPlaces": {"type": "max", "places": 1}}

page_ventas = [
    filter_widget("f_anio", "ds_f_anio", "Año", "p_anio", 0),
    filter_widget("f_trimestre", "ds_f_trimestre", "Trimestre", "p_trimestre", 1),
    filter_widget("f_mes", "ds_f_mes", "Mes", "p_mes", 2),
    filter_widget("f_compania", "ds_f_compania", "Compañía", "p_compania", 3),
    text("t_moneda", "**Montos en PEN** (moneda de Silver). Fuente: `fmcg_lakehouse.gold`.", pos(4, 0, 2, 2)),
    counter("c_ingresos", "ds_ventas_mes", "SUM(`revenue`)", "sum(revenue)", "Ingresos (PEN)", pos(0, 2, 2, 3), NUM),
    counter("c_cantidad", "ds_ventas_mes", "SUM(`total_qty`)", "sum(total_qty)", "Cantidad vendida", pos(2, 2, 2, 3), NUM),
    counter("c_pedidos", "ds_ventas_mes", "SUM(`orders`)", "sum(orders)", "Pedidos", pos(4, 2, 2, 3), NUM),
    {"widget": {"name": "l_tendencia",
                "queries": [q("main_query", "ds_ventas_mes", [
                    fld("month_start", "`month_start`"), fld("company", "`company`"),
                    fld("sum(revenue)", "SUM(`revenue`)")])],
                "spec": {"version": 3, "widgetType": "line",
                         "encodings": {"x": {"fieldName": "month_start", "scale": {"type": "temporal"}, "displayName": "Mes"},
                                       "y": {"fieldName": "sum(revenue)", "scale": {"type": "quantitative"}, "displayName": "Ingresos (PEN)"},
                                       "color": {"fieldName": "company", "scale": {"type": "categorical"}, "displayName": "Compañía"}},
                         "frame": frame("Tendencia mensual de ingresos: madre vs hija")}},
     "position": pos(0, 5, 6, 6)},
    {"widget": {"name": "b_top10",
                "queries": [q("main_query", "ds_top10", [
                    fld("product_label", "`product_label`"), fld("sum(revenue)", "SUM(`revenue`)")])],
                "spec": {"version": 3, "widgetType": "bar",
                         "encodings": {"x": {"fieldName": "sum(revenue)", "scale": {"type": "quantitative"}, "displayName": "Ingresos (PEN)"},
                                       "y": {"fieldName": "product_label", "displayName": "Producto",
                                             "scale": {"type": "categorical", "sort": {"by": "x-reversed"}}}},
                         "frame": frame("Top 10 productos por ingresos",
                                        "Empates comparten puesto (RANK)")}},
     "position": pos(0, 11, 3, 8)},
    {"widget": {"name": "p_categoria",
                "queries": [q("main_query", "ds_categoria", [
                    fld("category", "`category`"), fld("sum(revenue)", "SUM(`revenue`)")])],
                "spec": {"version": 3, "widgetType": "pie",
                         "encodings": {"angle": {"fieldName": "sum(revenue)", "scale": {"type": "quantitative"}, "displayName": "Ingresos (PEN)"},
                                       "color": {"fieldName": "category", "scale": {"type": "categorical"}, "displayName": "Categoría"}},
                         "frame": frame("Mix de ingresos por categoría")}},
     "position": pos(3, 11, 3, 8)},
    {"widget": {"name": "b_comparativo",
                "queries": [q("main_query", "ds_comparativo", [
                    fld("company", "`company`"), fld("sum(revenue)", "SUM(`revenue`)")])],
                "spec": {"version": 3, "widgetType": "bar",
                         "encodings": {"x": {"fieldName": "company", "scale": {"type": "categorical"}, "displayName": "Compañía"},
                                       "y": {"fieldName": "sum(revenue)", "scale": {"type": "quantitative"}, "displayName": "Ingresos (PEN)"},
                                       "color": {"fieldName": "company", "scale": {"type": "categorical"}, "displayName": "Compañía"}},
                         "frame": frame("Comparativo madre vs hija: ingresos")}},
     "position": pos(0, 19, 2, 6)},
    {"widget": {"name": "l_participacion",
                "queries": [q("main_query", "ds_comparativo", [
                    fld("month_start", "`month_start`"), fld("company", "`company`"),
                    fld("avg(revenue_share_month)", "AVG(`revenue_share_month`)")])],
                "spec": {"version": 3, "widgetType": "bar",
                         "encodings": {"x": {"fieldName": "month_start", "scale": {"type": "temporal"}, "displayName": "Mes"},
                                       "y": {"fieldName": "avg(revenue_share_month)", "scale": {"type": "quantitative"},
                                             "displayName": "Participación en el mes", "format": PCT},
                                       "color": {"fieldName": "company", "scale": {"type": "categorical"}, "displayName": "Compañía"}},
                         "mark": {"layout": "stack"},
                         "frame": frame("Participación mensual en ingresos (madre vs hija)")}},
     "position": pos(2, 19, 4, 6)},
    # una fila por mes x compania en ds_comparativo (moneda unica): AVG devuelve el valor de la vista
    {"widget": {"name": "l_variacion_mensual",
                "queries": [q("main_query", "ds_comparativo", [
                    fld("month_start", "`month_start`"), fld("company", "`company`"),
                    fld("avg(revenue_mom_pct)", "AVG(`revenue_mom_pct`)")])],
                "spec": {"version": 3, "widgetType": "line",
                         "encodings": {"x": {"fieldName": "month_start", "scale": {"type": "temporal"}, "displayName": "Mes"},
                                       "y": {"fieldName": "avg(revenue_mom_pct)", "scale": {"type": "quantitative"},
                                             "displayName": "Variación vs mes anterior", "format": PCT},
                                       "color": {"fieldName": "company", "scale": {"type": "categorical"}, "displayName": "Compañía"}},
                         "frame": frame("Variación mensual de ingresos (madre vs hija)",
                                        "revenue_mom_pct de v_kpi_company_comparison: ingresos del mes / mes anterior - 1")}},
     "position": pos(0, 25, 6, 6)},
]


def table(name, dsname, cols, title, p):
    return {"widget": {"name": name,
                       "queries": [q("main_query", dsname, [fld(c, f"`{c}`") for c, _ in cols], disagg=True)],
                       "spec": {"version": 2, "widgetType": "table",
                                "encodings": {"columns": [{"fieldName": c, "displayName": d} for c, d in cols]},
                                "frame": frame(title)}},
            "position": p}


page_calidad = [
    text("t_calidad", "Reglas de calidad de **Silver** con filas fallidas en la última corrida por entidad "
         "(`gold.v_kpi_data_quality`) y filas enviadas a cuarentena (`gold.v_kpi_quarantine_summary`).", pos(0, 0, 6, 2)),
    counter("c_cuarentena", "ds_cuarentena", "SUM(`rows_quarantined`)", "sum(rows_quarantined)",
            "Filas en cuarentena", pos(0, 2, 2, 3), NUM),
    counter("c_reglas", "ds_calidad", "COUNT(DISTINCT `rule_id`)", "count_distinct(rule_id)",
            "Reglas con filas fallidas", pos(2, 2, 2, 3)),
    counter("c_fallidas", "ds_calidad", "SUM(`rows_failed`)", "sum(rows_failed)",
            "Filas marcadas/corregidas/rechazadas", pos(4, 2, 2, 3), NUM),
    {"widget": {"name": "b_cuarentena",
                "queries": [q("main_query", "ds_cuarentena", [
                    fld("rule_id", "`rule_id`"), fld("entity", "`entity`"),
                    fld("sum(rows_quarantined)", "SUM(`rows_quarantined`)")])],
                "spec": {"version": 3, "widgetType": "bar",
                         "encodings": {"x": {"fieldName": "rule_id", "scale": {"type": "categorical"}, "displayName": "Regla"},
                                       "y": {"fieldName": "sum(rows_quarantined)", "scale": {"type": "quantitative"}, "displayName": "Filas en cuarentena"},
                                       "color": {"fieldName": "entity", "scale": {"type": "categorical"}, "displayName": "Entidad"}},
                         "frame": frame("Cuarentena por regla")}},
     "position": pos(0, 5, 2, 6)},
    table("tb_cuarentena", "ds_cuarentena",
          [("entity", "Entidad"), ("company", "Compañía"), ("rule_id", "Regla"),
           ("rows_quarantined", "Filas"), ("last_quarantined_at", "Última cuarentena")],
          "Detalle de cuarentena", pos(2, 5, 4, 6)),
    table("tb_calidad", "ds_calidad",
          [("entity", "Entidad"), ("company", "Compañía"), ("rule_id", "Regla"), ("description", "Descripción"),
           ("severity", "Severidad"), ("action", "Acción"), ("rows_evaluated", "Evaluadas"),
           ("rows_failed", "Fallidas"), ("fail_rate", "Tasa de fallo"), ("ts", "Corrida")],
          "Reglas con filas fallidas (última corrida de Silver por entidad)", pos(0, 11, 6, 10)),
]

dash = {
    "datasets": DATASETS,
    "pages": [
        {"name": "ventas", "displayName": "Ventas", "layout": page_ventas, "pageType": "PAGE_TYPE_CANVAS"},
        {"name": "calidad", "displayName": "Calidad de datos", "layout": page_calidad, "pageType": "PAGE_TYPE_CANVAS"},
    ],
}
out = sys.argv[1]
with open(out, "w", encoding="utf-8") as fh:
    json.dump(dash, fh, ensure_ascii=False, indent=2)
    fh.write("\n")
print("ok", out)
