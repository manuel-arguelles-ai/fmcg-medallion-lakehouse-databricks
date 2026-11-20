"""Reconciliacion entre capas (landing -> bronze -> silver -> gold) del pipeline maestro. Serverless.

Por compania x entidad mide conteos en cada capa, los escribe en ops.pipeline_runs (una fila por
compania x entidad y corrida) y FALLA si alguna identidad no cuadra (docs/03 §6). Identidades:

  L1  archivos en landing        = archivos distintos en Bronze + archivos rechazados (ops.landing_rejects)
  L2  filas de landing (aceptados) = filas en Bronze
  B1  filas Bronze sin clave valida = filas en cuarentena por regla de clave (CUS_01, PRD_01, PRC_01/02, ORD_01)
  B2  claves distintas en Bronze = claves distintas en Silver U cuarentena = claves en Bronze U Silver U cuarentena
      (cada clave de negocio tiene su fila ganadora en Silver o en cuarentena, y nada aparece de la nada)
  G1  filas Silver (sin miembro -1) = filas Gold de la compania (dim_customer/dim_product/dim_price/fact_sales)
  G2  orders: suma line_amount Silver = suma fact_sales = suma revenue de agg_sales_monthly

Las claves se normalizan con las MISMAS expresiones que usa Silver (silver.transform), no con una copia.
B1/B2 detectan, por ejemplo, un lote de Bronze que Silver nunca proceso (riesgo D9 del staging).

Uso (spark_python_task): reconcile.py --catalog fmcg_lakehouse --mode incremental --run-id {{job.run_id}}
"""
from __future__ import annotations

import argparse
import datetime as dt
import inspect
import os
import sys
import uuid

try:
    _THIS = os.path.abspath(__file__)
except NameError:  # serverless: exec(compile(...)) sin __file__
    _THIS = os.path.abspath(inspect.currentframe().f_code.co_filename)
_SRC = os.path.dirname(os.path.dirname(_THIS))
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

COMPANIES = ("madre", "hija")
ENTITIES = ("customers", "products", "prices", "orders")
GOLD_TABLE = {"customers": "dim_customer", "products": "dim_product", "prices": "dim_price", "orders": "fact_sales"}
KEYLESS_RULES = {"customers": ["CUS_01"], "products": ["PRD_01"], "prices": ["PRC_01", "PRC_02"], "orders": ["ORD_01"]}

# (columna, tipo, comentario) de ops.pipeline_runs
COLUMNS = [
    ("run_id", "STRING", "id de la reconciliacion (pipeline_<job_run_id> o aleatorio)"),
    ("job_run_id", "STRING", "run del job maestro"),
    ("mode", "STRING", "incremental | full (modo con que corrio Silver)"),
    ("company", "STRING", "madre | hija"),
    ("entity", "STRING", "customers | products | prices | orders"),
    ("landing_files", "BIGINT", "CSV en landing/<compania>/<entidad>"),
    ("rejected_files", "BIGINT", "de esos, rechazados por la pre-validacion de Bronze"),
    ("landing_rows", "BIGINT", "filas de datos en los CSV aceptados"),
    ("bronze_files", "BIGINT", "archivos distintos en la tabla Bronze"),
    ("bronze_rows", "BIGINT", "filas en la tabla Bronze"),
    ("bronze_keyless", "BIGINT", "filas Bronze sin clave de negocio valida"),
    ("bronze_keys", "BIGINT", "claves de negocio distintas en Bronze"),
    ("silver_rows", "BIGINT", "filas Silver (sin miembro -1)"),
    ("quarantine_rows", "BIGINT", "filas en cuarentena"),
    ("quarantine_keyless", "BIGINT", "filas en cuarentena por regla de clave"),
    ("silver_quarantine_keys", "BIGINT", "claves distintas en Silver U cuarentena"),
    ("all_keys", "BIGINT", "claves distintas en Bronze U Silver U cuarentena"),
    ("gold_rows", "BIGINT", "filas Gold de la compania (dimension o fact_sales)"),
    ("silver_amount", "DECIMAL(38,2)", "orders: suma line_amount en Silver"),
    ("gold_amount", "DECIMAL(38,2)", "orders: suma line_amount en fact_sales"),
    ("agg_amount", "DECIMAL(38,2)", "orders: suma revenue en agg_sales_monthly"),
    ("status", "STRING", "ok | failed"),
    ("failed_checks", "STRING", "identidades que no cuadran (vacio si ok)"),
    ("ts", "TIMESTAMP", "momento de la reconciliacion"),
]
METRICS = [c for c, _, _ in COLUMNS[5:21]]


def table_ddl(catalog: str) -> str:
    cols = ",\n  ".join(f"{c} {t} COMMENT '{m}'" for c, t, m in COLUMNS)
    return (f"CREATE TABLE IF NOT EXISTS {catalog}.ops.pipeline_runs (\n  {cols}\n)\n"
            "COMMENT 'Reconciliacion landing -> bronze -> silver -> gold por corrida del pipeline maestro'")


def evaluate(entity: str, m: dict) -> list[str]:
    """Identidades que fallan para una fila de metricas (puro, testeable sin Spark)."""
    fails = []

    def chk(name, ok, detail):
        if not ok:
            fails.append(f"{name}: {detail}")

    chk("L1", m["landing_files"] == m["bronze_files"] + m["rejected_files"],
        f"landing_files={m['landing_files']} != bronze_files={m['bronze_files']} + rejected={m['rejected_files']}")
    chk("L2", m["landing_rows"] == m["bronze_rows"],
        f"landing_rows={m['landing_rows']} != bronze_rows={m['bronze_rows']}")
    chk("B1", m["bronze_keyless"] == m["quarantine_keyless"],
        f"bronze_keyless={m['bronze_keyless']} != quarantine_keyless={m['quarantine_keyless']}")
    chk("B2", m["bronze_keys"] == m["silver_quarantine_keys"] == m["all_keys"],
        f"bronze_keys={m['bronze_keys']} silver_quarantine_keys={m['silver_quarantine_keys']} all_keys={m['all_keys']}")
    chk("G1", m["silver_rows"] == m["gold_rows"], f"silver_rows={m['silver_rows']} != gold_rows={m['gold_rows']}")
    if entity == "orders":
        chk("G2", m["silver_amount"] == m["gold_amount"] == m["agg_amount"],
            f"silver={m['silver_amount']} fact={m['gold_amount']} agg={m['agg_amount']}")
    return fails


# ------------------------------------------------------------------ Spark
def _raw_keys(entity):
    """Clave de negocio (sin company) desde columnas crudas, con las expresiones de Silver."""
    from pyspark.sql import functions as F

    from silver import transform as T
    if entity == "customers":
        return [T._id("customer_id")]
    if entity == "products":
        return [T._product_id("product_id")]
    if entity == "prices":
        return [T._product_id("product_id"), F.trunc(T._date("price_month")[0], "month")]
    ls = F.trim("line_no")
    return [T._id("order_id"), F.when(ls.rlike(r"^\d{1,9}$"), ls.cast("int"))]


def _keyed(df, keys):
    """(company, k0, k1...) solo de filas con todas las partes de la clave no nulas."""
    from functools import reduce

    from pyspark.sql import functions as F
    out = df.select("company", *[k.alias(f"k{i}") for i, k in enumerate(keys)])
    return out.filter(reduce(lambda a, b: a & b, [F.col(f"k{i}").isNotNull() for i in range(len(keys))]))


def _distinct_keys(df, n):
    from pyspark.sql import functions as F
    return {r["company"]: r["n"] for r in
            df.groupBy("company").agg(F.countDistinct(*[f"k{i}" for i in range(n)]).alias("n")).collect()}


def measure(spark, catalog: str, entity: str) -> dict:
    """{company: metricas} de una entidad."""
    from pyspark.sql import functions as F

    from bronze import schemas, validation
    from bronze.registry import get_spec
    from silver import ddl as sddl

    out = {c: {k: 0 for k in METRICS} for c in COMPANIES}
    rejected = {}
    for r in spark.table(f"{catalog}.ops.landing_rejects").where(F.col("source") == entity) \
            .select("company", "file_path").distinct().collect():
        rejected.setdefault(r["company"], set()).add(validation.normalize_path(r["file_path"]))

    # landing: listado del volume + lectura CSV con las mismas opciones que Auto Loader
    for c in COMPANIES:
        spec = get_spec(c, entity)
        files = {validation.normalize_path(p) for p, _ in validation.list_files(spec.landing_path(catalog))}
        rej = rejected.get(c, set()) & files
        out[c]["landing_files"], out[c]["rejected_files"] = len(files), len(rej)
        accepted = sorted(files - rej)
        if accepted:
            df = (spark.read.format("csv").option("header", "true").option("delimiter", spec.delimiter)
                  .option("quote", '"').option("escape", '"').schema(schemas.ddl_schema_string(spec))
                  .load(accepted))
            out[c]["landing_rows"] = df.count()

    keys = _raw_keys(entity)
    n = len(keys)
    bronze = None
    for c in COMPANIES:
        b = spark.table(f"{catalog}.bronze.{c}_{entity}").withColumn("company", F.lit(c))
        bronze = b if bronze is None else bronze.unionByName(b)
    for r in bronze.groupBy("company").agg(F.count(F.lit(1)).alias("rows"),
                                           F.countDistinct("_source_file").alias("files")).collect():
        out[r["company"]]["bronze_rows"], out[r["company"]]["bronze_files"] = r["rows"], r["files"]
    b_keys = _keyed(bronze, keys)
    for c, v in _distinct_keys(b_keys, n).items():
        out[c]["bronze_keys"] = v
    for r in bronze.groupBy("company").count().collect():
        out[r["company"]]["bronze_keyless"] = r["count"]
    for r in b_keys.groupBy("company").count().collect():
        out[r["company"]]["bronze_keyless"] -= r["count"]

    q = spark.table(f"{catalog}.quarantine.{entity}").withColumn("company", F.col("_company"))
    is_kl = F.col("rule_id").isin(KEYLESS_RULES[entity])
    for r in q.groupBy("company").agg(F.count(F.lit(1)).alias("n"),
                                      F.sum(F.when(is_kl, 1).otherwise(0)).alias("kl")).collect():
        out[r["company"]]["quarantine_rows"], out[r["company"]]["quarantine_keyless"] = r["n"], int(r["kl"] or 0)
    q_keys = _keyed(q.filter(~is_kl), keys)

    s = spark.table(f"{catalog}.silver.{entity}")
    if "is_unknown_member" in s.columns:
        s = s.filter(~F.coalesce(F.col("is_unknown_member"), F.lit(False)))
    for r in s.groupBy("company").agg(F.count(F.lit(1)).alias("n")).collect():
        out[r["company"]]["silver_rows"] = r["n"]
    s_keys = s.select("company", *[F.col(k).alias(f"k{i}") for i, k in enumerate(sddl.MERGE_KEY[entity][1:])])
    for c, v in _distinct_keys(s_keys.unionByName(q_keys), n).items():
        out[c]["silver_quarantine_keys"] = v
    for c, v in _distinct_keys(s_keys.unionByName(q_keys).unionByName(b_keys), n).items():
        out[c]["all_keys"] = v

    g = spark.table(f"{catalog}.gold.{GOLD_TABLE[entity]}")
    for r in g.groupBy("company").agg(F.count(F.lit(1)).alias("n")).collect():
        if r["company"] in out:   # el miembro -1 tiene company 'Unknown'
            out[r["company"]]["gold_rows"] = r["n"]
    if entity == "orders":
        for c in COMPANIES:
            out[c]["silver_amount"] = out[c]["gold_amount"] = out[c]["agg_amount"] = None
        for col, df, amount in (("silver_amount", s, "line_amount"), ("gold_amount", g, "line_amount"),
                                ("agg_amount", spark.table(f"{catalog}.gold.agg_sales_monthly"), "revenue")):
            for r in df.groupBy("company").agg(F.sum(amount).alias("a")).collect():
                if r["company"] in out:
                    out[r["company"]][col] = r["a"]
    else:
        for c in COMPANIES:
            out[c]["silver_amount"] = out[c]["gold_amount"] = out[c]["agg_amount"] = None
    return out


def run(spark, catalog: str, mode: str, job_run_id: str | None = None) -> list[dict]:
    spark.sql(table_ddl(catalog))
    run_id = f"pipeline_{job_run_id}" if job_run_id else \
        f"pipeline_{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:6]}"
    ts = dt.datetime.now(dt.timezone.utc)
    rows, failed = [], []
    for entity in ENTITIES:
        for company, m in measure(spark, catalog, entity).items():
            fails = evaluate(entity, m)
            failed += [f"{company}/{entity} {f}" for f in fails]
            rows.append({"run_id": run_id, "job_run_id": job_run_id, "mode": mode, "company": company,
                         "entity": entity, **m, "status": "failed" if fails else "ok",
                         "failed_checks": "; ".join(fails) or None, "ts": ts})
            print(f"[{run_id}] {company}/{entity} {'FAIL ' + '; '.join(fails) if fails else 'ok'} {m}")
    schema = ", ".join(f"{c} {t}" for c, t, _ in COLUMNS)
    spark.createDataFrame([tuple(r[c] for c, _, _ in COLUMNS) for r in rows], schema) \
        .write.mode("append").saveAsTable(f"{catalog}.ops.pipeline_runs")
    if failed:
        raise RuntimeError("reconciliacion entre capas fallida: " + " | ".join(failed))
    return rows


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Reconciliacion landing -> bronze -> silver -> gold")
    p.add_argument("--catalog", default="fmcg_lakehouse")
    p.add_argument("--mode", default="incremental", choices=["incremental", "full"])
    p.add_argument("--run-id", help="run del job maestro ({{job.run_id}})")
    a, _ = p.parse_known_args(argv)
    return a


def main(argv=None):
    from pyspark.sql import SparkSession
    a = parse_args(argv)
    run(SparkSession.builder.getOrCreate(), a.catalog, a.mode, a.run_id)


if __name__ == "__main__":
    main()
