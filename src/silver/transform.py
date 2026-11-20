"""Bronze -> Silver (Spark, compatible con serverless: solo API DataFrame/SQL, sin APIs de bajo nivel ni caches).

Una funcion por entidad (customers, products, prices, orders); procesan madre e hija con el mismo codigo.
Flujo comun de cada entidad:
  1. leer Bronze (`incremental` = bronze.staging_<c>_<f>, `full` = tabla Bronze completa)
  2. normalizar claves -> filas sin clave valida a cuarentena
  3. dedup determinista por clave de negocio (gana mayor _ingest_ts)
  4. reglas: cuarentena (no entra a Silver) / corregir / marcar (banderas)
  5. MERGE idempotente a silver.<entidad>; cuarentena y ops.dq_results
Las reglas son las de `rules.py` (mismos ids, formatos y regex). Serverless usa ANSI: todo cast
va protegido por regex y las fechas con try_to_date, para que un dato malo nunca tumbe el job.
NO VALIDADO EN SPARK todavia (no hay Java local): ver docs/08-silver-implementacion.md.
"""
from __future__ import annotations

import datetime as dt
import uuid
from functools import reduce
from itertools import chain

from silver import ddl, rules

try:  # pyspark solo existe en el workspace; localmente se importa el modulo solo para inspeccion
    from pyspark.sql import Window
    from pyspark.sql import functions as F
except ImportError:  # pragma: no cover
    F = Window = None

COMPANIES = ("madre", "hija")
DQ_SCHEMA = ("batch_id STRING, entity STRING, company STRING, rule_id STRING, rows_evaluated BIGINT, "
             "rows_failed BIGINT, action STRING, mode STRING, ts TIMESTAMP")


# ------------------------------------------------------------------ expresiones de columna
def _col(c):
    return F.col(c) if isinstance(c, str) else c


def _b(c):
    """Condicion sin NULL: NULL cuenta como falso."""
    return F.coalesce(c, F.lit(False))


def _txt(c):
    """trim + colapso de espacios; vacio -> NULL (rules.collapse_ws)."""
    x = F.trim(F.regexp_replace(_col(c), r"\s+", " "))
    return F.when(x != "", x)


def _id(c):
    s = F.trim(_col(c))
    return F.when(s.rlike(rules.ID_RE), s)


def _product_id(c):
    s = F.trim(_col(c))
    return F.when(s.rlike(rules.PRODUCT_ID_RE), F.regexp_extract(s, rules.PRODUCT_ID_RE, 1))


def _date(c):
    """(fecha, clave_de_formato) con los formatos de rules.DATE_FORMATS; imposible/no soportado -> NULL."""
    s = F.trim(_col(c))
    parsed, keys = [], []
    for f in rules.DATE_FORMATS:
        txt = F.regexp_replace(s, r"^[A-Za-z]+,\s*", "") if f.strip_weekday else s
        parsed.append(F.when(s.rlike(f.regex), F.call_function("try_to_date", txt, F.lit(f.spark_pattern))))
        keys.append(F.when(s.rlike(f.regex), F.lit(f.key)))
    return F.coalesce(*parsed), F.coalesce(*keys)


def _lookup(d: dict, key):
    """d[key] sin error si la clave no existe (ANSI hace fallar map[key]); clave NULL -> NULL."""
    m = F.create_map(*chain.from_iterable((F.lit(k), F.lit(v)) for k, v in d.items()))
    return F.call_function("try_element_at", m, key)


# ------------------------------------------------------------------ infraestructura comun
def new_run_id(entity: str) -> str:
    return f"silver_{entity}_{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:6]}"


def setup(spark, catalog: str) -> None:
    """Crea (si faltan) tablas Silver/cuarentena/dq/ref_*, siembra ref_* y los miembros -1. Idempotente."""
    for stmt in ddl.setup_statements(catalog):
        spark.sql(stmt)


def load_refs(spark, catalog: str) -> dict:
    """Lee las tablas de referencia (pequenas) al driver."""
    def rows(t):
        return spark.table(f"{catalog}.silver.{t}").collect()
    refs = {
        "city": {r["raw_city_key"]: r["city"] for r in rows("ref_city_mapping")},
        "category": {r["category_key"]: (r["category"], r["division"]) for r in rows("ref_category_division")},
        "typos": [(r["wrong_term"], r["correct_term"]) for r in rows("ref_typo_fixes")
                  if r["entity"] == "products" and r["column_name"] == "product_name"],
        "company": {r["company"]: (r["market"], r["channel"]) for r in rows("ref_company")},
    }
    if not refs["city"] or not refs["category"]:
        raise RuntimeError("ref_city_mapping/ref_category_division vacias: ejecutar setup() primero")
    return refs


def read_bronze(spark, catalog, entity, mode, companies):
    prefix = "staging_" if mode == "incremental" else ""
    dfs = [spark.table(f"{catalog}.bronze.{prefix}{c}_{entity}") for c in companies]
    return reduce(lambda a, b: a.unionByName(b), dfs)


def dedup(df, keys):
    """row_number por clave de negocio; gana mayor _ingest_ts (desempate fijo por archivo y lote)."""
    w = Window.partitionBy(*keys).orderBy(F.col("_ingest_ts").desc(), F.col("_source_file").desc(),
                                          F.col("_batch_id").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")


def tag(df, checks):
    """Agrega _q_ids/_q_reasons (arrays) con las reglas de cuarentena que dispara cada fila."""
    if not checks:
        empty = F.array().cast("array<string>")
        return df.withColumn("_q_ids", empty).withColumn("_q_reasons", empty)
    ids = F.array_compact(F.array(*[F.when(_b(c), F.lit(r)) for r, c, _ in checks]))
    why = F.array_compact(F.array(*[F.when(_b(c), F.lit(m)) for _, c, m in checks]))
    return df.withColumn("_q_ids", ids).withColumn("_q_reasons", why)


def untag(df):
    return df.drop("_q_ids", "_q_reasons")


class Run:
    """Contexto de una corrida de una entidad: acumula ops.dq_results y escribe Silver/cuarentena."""

    def __init__(self, spark, catalog, entity, mode, companies, run_id=None):
        self.spark, self.catalog, self.entity, self.mode = spark, catalog, entity, mode
        self.companies = list(companies)
        self.run_id = run_id or new_run_id(entity)
        self.ts = dt.datetime.now(dt.timezone.utc)
        self.dq: list[tuple] = []
        self.totals = {"rows_in": 0, "quarantined": 0, "deduped": 0}

    def _record(self, company, rule_id, evaluated, failed):
        self.dq.append((self.run_id, self.entity, company, rule_id, int(evaluated or 0), int(failed or 0),
                        rules.get_rule(rule_id).action, self.mode, self.ts))

    def stage(self, df, conds: dict):
        """Cuenta evaluadas/fallidas por compania y regla en UNA pasada. Devuelve {company: fila}."""
        aggs = [F.count(F.lit(1)).alias("_n")]
        aggs += [F.sum(F.when(_b(c), 1).otherwise(0)).alias(r) for r, c in conds.items()]
        if "_q_ids" in df.columns:
            aggs.append(F.sum(F.when(F.size("_q_ids") > 0, 1).otherwise(0)).alias("_rejected"))
        out = {}
        for row in df.groupBy("company").agg(*aggs).collect():
            d = row.asDict()
            out[d["company"]] = d
            for r in conds:
                self._record(d["company"], r, d["_n"], d[r])
        return out

    def dups(self, rule_id, keyed, keys):
        """Regla de duplicado: evaluadas = filas con clave valida; fallidas = copias descartadas."""
        agg = keyed.groupBy("company").agg(F.count(F.lit(1)).alias("n"), F.countDistinct(*keys).alias("d"))
        for row in agg.collect():
            self._record(row["company"], rule_id, row["n"], row["n"] - row["d"])
            self.totals["deduped"] += row["d"]

    def write_quarantine(self, raw, *rejected):
        """Idempotente: borra lo ya cuarentenado de los mismos lotes (o companias en full) y re-inserta."""
        cols = ddl.RAW_COLUMNS[self.entity] + ["_company", "_source_file", "_ingest_ts", "_batch_id"]
        parts = [d.select(*cols, F.get("_q_ids", 0).alias("rule_id"),
                          F.concat_ws("; ", "_q_reasons").alias("reason"),
                          F.current_timestamp().alias("quarantined_at"),
                          F.lit(self.run_id).alias("silver_run_id")) for d in rejected]
        q = reduce(lambda a, b: a.unionByName(b), parts)
        table = f"{self.catalog}.quarantine.{self.entity}"
        if self.mode == "full":
            comps = ", ".join(f"'{c}'" for c in self.companies)
            self.spark.sql(f"DELETE FROM {table} WHERE _company IN ({comps})")
        else:
            batches = [r[0] for r in raw.select("_batch_id").distinct().collect() if r[0]]
            if batches:
                ids = ", ".join("'" + b.replace("'", "''") + "'" for b in batches)
                self.spark.sql(f"DELETE FROM {table} WHERE _batch_id IN ({ids})")
        q.write.mode("append").saveAsTable(table)

    def merge(self, valid):
        """MERGE idempotente por clave de negocio; solo pisa si la fila nueva es igual o mas reciente."""
        view = f"_silver_src_{self.entity}"
        valid.select(*ddl.columns(self.entity)).createOrReplaceTempView(view)
        on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in ddl.MERGE_KEY[self.entity])
        self.spark.sql(
            f"MERGE INTO {self.catalog}.silver.{self.entity} AS t USING {view} AS s ON {on} "
            "WHEN MATCHED AND s._ingest_ts >= t._ingest_ts THEN UPDATE SET * "
            "WHEN NOT MATCHED THEN INSERT *")

    def finish(self) -> dict:
        if self.dq:
            self.spark.createDataFrame(self.dq, DQ_SCHEMA).write.mode("append").saveAsTable(
                f"{self.catalog}.ops.dq_results")
        out = {"entity": self.entity, "mode": self.mode, "run_id": self.run_id, **self.totals}
        print(f"[{self.run_id}] {out}")
        return out

    def count_in(self, s1: dict):
        """s1 = stage() sobre la entrada con claves taggeadas."""
        self.totals["rows_in"] = sum(d["_n"] for d in s1.values())
        self.totals["quarantined"] += sum(d.get("_rejected") or 0 for d in s1.values())


def _lineage():
    return [F.col("_source_file"), F.col("_batch_id"), F.col("_ingest_ts"),
            F.current_timestamp().alias("_silver_updated_at")]


def _split_keys(run: Run, n, key_checks, keys):
    """Etapa 1: filas sin clave valida -> cuarentena; el resto se deduplica. Devuelve (keyless, deduped)."""
    t = tag(n, key_checks)
    run.count_in(run.stage(t, {r: c for r, c, _ in key_checks}))
    keyless = t.filter(F.size("_q_ids") > 0)
    keyed = untag(t.filter(F.size("_q_ids") == 0))
    return keyless, keyed, dedup(keyed, keys)


def _empty(entity, mode):
    print(f"[silver.{entity}] sin filas en Bronze ({mode}); nada que hacer")
    return {"entity": entity, "mode": mode, "rows_in": 0, "quarantined": 0, "deduped": 0}


# ------------------------------------------------------------------ customers
def customers(spark, catalog, mode="incremental", companies=COMPANIES, run_id=None) -> dict:
    run = Run(spark, catalog, "customers", mode, companies, run_id)
    refs = load_refs(spark, catalog)
    raw = read_bronze(spark, catalog, "customers", mode, companies)
    n = raw.withColumn("company", F.col("_company")).withColumn("k_id", _id("customer_id"))
    keyless, keyed, dd = _split_keys(
        run, n, [("CUS_01", F.col("k_id").isNull(), "customer_id nulo o no numerico")],
        ["company", "k_id"])
    run.dups("CUS_08", keyed, ["company", "k_id"])

    dd = (dd.withColumn("c_name0", _txt("customer_name"))
          .withColumn("c_name", F.initcap("c_name0"))
          .withColumn("c_city0", _txt("city"))
          .withColumn("c_mapped", _lookup(refs["city"], F.lower(F.col("c_city0"))))
          .withColumn("c_city", F.coalesce("c_mapped", F.initcap("c_city0"), F.lit(rules.UNKNOWN_TEXT))))
    t = tag(dd, [("CUS_02", F.col("c_name0").isNull(), "customer_name vacio")])
    conds = {
        "CUS_02": F.col("c_name0").isNull(),
        "CUS_03": F.col("c_name0").isNotNull() & (F.col("customer_name") != F.col("c_name0")),
        "CUS_04": F.col("c_name") != F.col("c_name0"),
        "CUS_05": F.col("c_mapped").isNotNull() & (F.col("c_mapped") != F.col("c_city0")),
        "CUS_06": F.col("c_city0").isNull(),
        "CUS_07": F.col("c_city0").isNotNull() & F.col("c_mapped").isNull(),
    }
    s2 = run.stage(t, conds)
    run.totals["quarantined"] += sum(d["_rejected"] or 0 for d in s2.values())

    market = {c: m for c, (m, _) in refs["company"].items()}
    channel = {c: ch for c, (_, ch) in refs["company"].items()}
    valid = untag(t.filter(F.size("_q_ids") == 0)).select(
        "company", F.col("k_id").alias("customer_id"),
        F.concat_ws(":", "company", "k_id").alias("customer_key"),
        F.col("c_name").alias("customer_name"), F.col("c_city").alias("city"),
        F.concat_ws("-", "c_name", "c_city").alias("customer_label"),
        F.coalesce(_lookup(market, F.col("company")), F.lit(rules.UNKNOWN_TEXT)).alias("market"),
        F.coalesce(_lookup(channel, F.col("company")), F.lit(rules.UNKNOWN_TEXT)).alias("channel"),
        ((F.col("customer_name") != F.col("c_name0")) | (F.col("c_name") != F.col("c_name0"))).alias("name_was_normalized"),
        _b(conds["CUS_05"]).alias("city_was_corrected"), _b(conds["CUS_06"]).alias("city_was_null"),
        _b(conds["CUS_07"]).alias("city_is_unmapped"), F.lit(False).alias("is_unknown_member"),
        *_lineage())
    run.write_quarantine(raw, keyless, t.filter(F.size("_q_ids") > 0))
    run.merge(valid)
    return run.finish()


# ------------------------------------------------------------------ products
def products(spark, catalog, mode="incremental", companies=COMPANIES, run_id=None) -> dict:
    run = Run(spark, catalog, "products", mode, companies, run_id)
    refs = load_refs(spark, catalog)
    raw = read_bronze(spark, catalog, "products", mode, companies)
    n = raw.withColumn("company", F.col("_company")).withColumn("k_id", _product_id("product_id"))
    keyless, keyed, dd = _split_keys(
        run, n, [("PRD_01", F.col("k_id").isNull(), "product_id nulo o no reconocible")], ["company", "k_id"])
    run.dups("PRD_07", keyed, ["company", "k_id"])

    name = _txt("product_name")
    for wrong, right in refs["typos"]:
        name = F.regexp_replace(name, rf"\b{wrong}\b", right)
    canon = {k: c for k, (c, _) in refs["category"].items()}
    divis = {k: d for k, (_, d) in refs["category"].items()}
    dd = (dd.withColumn("p_name0", _txt("product_name")).withColumn("p_name", name)
          .withColumn("p_cat0", _txt("category"))
          .withColumn("p_canon", _lookup(canon, F.lower(F.col("p_cat0"))))
          .withColumn("p_division", F.coalesce(_lookup(divis, F.lower(F.col("p_cat0"))), F.lit(rules.UNKNOWN_TEXT)))
          .withColumn("p_category", F.coalesce("p_canon", F.initcap("p_cat0"), F.lit(rules.UNKNOWN_TEXT)))
          .withColumn("p_prefixed", F.col("k_id").isNotNull() & F.upper(F.trim("product_id")).startswith("PX-")))
    t = tag(dd, [("PRD_03", F.col("p_name0").isNull(), "product_name vacio")])
    conds = {
        "PRD_03": F.col("p_name0").isNull(),
        "PRD_02": F.col("p_prefixed"),
        "PRD_04": F.col("p_name0").isNotNull() & (F.col("p_name") != F.col("p_name0")),
        "PRD_05": F.col("p_canon").isNotNull() & (F.col("p_canon") != F.col("p_cat0")),
        "PRD_06": F.col("p_canon").isNull(),
    }
    s2 = run.stage(t, conds)
    run.totals["quarantined"] += sum(d["_rejected"] or 0 for d in s2.values())

    base = F.trim(F.regexp_replace("p_name", r"\s*\([^)]*\)\s*$", ""))
    variant = F.trim(F.regexp_extract("p_name", r"\(([^)]*)\)", 1))
    valid = untag(t.filter(F.size("_q_ids") == 0)).select(
        "company", F.col("k_id").alias("product_id"),
        F.concat_ws(":", "company", "k_id").alias("product_key"),
        F.col("product_id").alias("product_id_raw"), F.col("p_prefixed").alias("product_id_was_prefixed"),
        F.sha2("p_name", 256).alias("product_code"), F.col("p_name").alias("product_name"),
        F.when(base != "", base).otherwise(F.col("p_name")).alias("product"),
        F.when(variant != "", variant).alias("variant"),
        F.col("p_category").alias("category"), F.col("p_division").alias("division"),
        _b(conds["PRD_04"]).alias("name_was_corrected"), _b(conds["PRD_05"]).alias("category_was_normalized"),
        _b(conds["PRD_06"]).alias("division_was_unknown"), F.lit(False).alias("is_unknown_member"),
        *_lineage())
    run.write_quarantine(raw, keyless, t.filter(F.size("_q_ids") > 0))
    run.merge(valid)
    return run.finish()


# ------------------------------------------------------------------ prices
def prices(spark, catalog, mode="incremental", companies=COMPANIES, run_id=None) -> dict:
    run = Run(spark, catalog, "prices", mode, companies, run_id)
    raw = read_bronze(spark, catalog, "prices", mode, companies)
    d, fmt = _date("price_month")
    n = (raw.withColumn("company", F.col("_company")).withColumn("k_id", _product_id("product_id"))
         .withColumn("k_date", d).withColumn("k_fmt", fmt)
         .withColumn("k_month", F.trunc("k_date", "month")))
    keys = ["company", "k_id", "k_month"]
    keyless, keyed, dd = _split_keys(run, n, [
        ("PRC_01", F.col("k_id").isNull(), "product_id nulo o no reconocible"),
        ("PRC_02", F.col("k_month").isNull(), "price_month nulo o formato no soportado")], keys)
    run.dups("PRC_06", keyed, keys)

    s = F.trim("unit_price")
    dd = dd.withColumn("k_price", F.when(s.rlike(rules.PRICE_RE), s.cast("decimal(12,2)")))
    invalid = F.col("k_price").isNull()
    as_quarantine = rules.get_rule("PRC_05").action == rules.CUARENTENA
    checks = [("PRC_05", invalid, "unit_price nulo o no numerico")] if as_quarantine else []
    t = tag(dd, checks)
    conds = {"PRC_05": invalid, "PRC_04": F.col("k_price") < 0, "PRC_03": F.col("k_fmt") != rules.ISO_KEY}
    s2 = run.stage(t, conds)
    run.totals["quarantined"] += sum(d_["_rejected"] or 0 for d_ in s2.values())

    ok = untag(t.filter(F.size("_q_ids") == 0))
    prod = spark.table(f"{catalog}.silver.products").filter(F.col("product_id") != rules.UNKNOWN_ID).select(
        F.col("company").alias("j_company"), F.col("product_id").alias("j_id"), F.col("product_code").alias("j_code"))
    j = ok.join(F.broadcast(prod), (ok["company"] == prod["j_company"]) & (ok["k_id"] == prod["j_id"]), "left")
    unresolved = F.col("j_id").isNull()
    run.stage(j, {"PRC_07": unresolved})
    valid = j.select(
        "company", F.col("k_id").alias("product_id"), F.concat_ws(":", "company", "k_id").alias("product_key"),
        F.col("j_code").alias("product_code"), F.col("k_month").alias("price_month"),
        F.when(invalid, F.lit(0).cast("decimal(12,2)")).otherwise(F.abs("k_price")).alias("unit_price"),
        F.lit(rules.CURRENCY).alias("currency"), _b(F.col("k_price") < 0).alias("price_was_negative"),
        invalid.alias("price_was_invalid"), (F.col("k_fmt") != rules.ISO_KEY).alias("month_was_reformatted"),
        unresolved.alias("product_unresolved"), *_lineage())
    run.write_quarantine(raw, keyless, *([t.filter(F.size("_q_ids") > 0)] if as_quarantine else []))
    run.merge(valid)
    return run.finish()


# ------------------------------------------------------------------ orders
def orders(spark, catalog, mode="incremental", companies=COMPANIES, run_id=None) -> dict:
    """Requiere silver.customers, silver.products y silver.prices ya cargadas (ver orden del job)."""
    run = Run(spark, catalog, "orders", mode, companies, run_id)
    raw = read_bronze(spark, catalog, "orders", mode, companies)
    d, fmt = _date("order_date")
    ls, qs = F.trim("line_no"), F.trim("order_qty")
    n = (raw.withColumn("company", F.col("_company")).withColumn("k_oid", _id("order_id"))
         .withColumn("k_line", F.when(ls.rlike(r"^\d{1,9}$"), ls.cast("int"))))
    keys = ["company", "k_oid", "k_line"]
    keyless, keyed, dd = _split_keys(
        run, n, [("ORD_01", F.col("k_oid").isNull() | F.col("k_line").isNull(), "order_id/line_no nulo o no numerico")], keys)
    run.dups("ORD_11", keyed, keys)

    dd = (dd.withColumn("k_date", d).withColumn("k_fmt", fmt)
          .withColumn("k_qty", F.when(qs.rlike(rules.QTY_RE), qs.cast("int")))
          .withColumn("k_prod", _product_id("product_id")).withColumn("k_cust", _id("customer_id")))
    qty_null = F.coalesce(qs, F.lit("")) == ""
    row_checks = [
        ("ORD_02", F.col("k_date").isNull(), "order_date nulo o formato no soportado"),
        ("ORD_03", qty_null, "order_qty nulo"),
        ("ORD_04", ~qty_null & ~_b(F.col("k_qty") > 0), "order_qty no es entero positivo"),
        ("ORD_05", F.col("k_prod").isNull(), "product_id nulo o no reconocible"),
    ]
    t = tag(dd, row_checks)
    s2 = run.stage(t, {r: c for r, c, _ in row_checks})
    run.totals["quarantined"] += sum(d_["_rejected"] or 0 for d_ in s2.values())

    ok = untag(t.filter(F.size("_q_ids") == 0)).withColumn("k_month", F.trunc("k_date", "month"))
    cust = spark.table(f"{catalog}.silver.customers").select(
        F.col("company").alias("c_company"), F.col("customer_id").alias("c_id"))
    prod = spark.table(f"{catalog}.silver.products").filter(F.col("product_id") != rules.UNKNOWN_ID).select(
        F.col("company").alias("p_company"), F.col("product_id").alias("p_id"))
    price = spark.table(f"{catalog}.silver.prices").select(
        F.col("company").alias("r_company"), F.col("product_id").alias("r_id"),
        F.col("price_month").alias("r_month"), F.col("unit_price").alias("r_unit"))
    j = (ok.join(F.broadcast(cust), (ok["company"] == cust["c_company"]) & (ok["k_cust"] == cust["c_id"]), "left")
         .join(F.broadcast(prod), (F.col("company") == prod["p_company"]) & (F.col("k_prod") == prod["p_id"]), "left"))
    j = j.join(F.broadcast(price), (F.col("company") == price["r_company"]) & (F.col("p_id") == price["r_id"])
               & (F.col("k_month") == price["r_month"]), "left")
    cust_invalid = F.col("k_cust").isNull()
    cust_unres = F.col("k_cust").isNotNull() & F.col("c_id").isNull()
    prod_unres = F.col("p_id").isNull()
    price_missing = F.col("p_id").isNotNull() & F.col("r_unit").isNull()
    run.stage(j, {"ORD_06": F.col("k_fmt") != rules.ISO_KEY, "ORD_07": cust_invalid, "ORD_08": cust_unres,
                  "ORD_09": prod_unres, "ORD_10": price_missing})
    valid = j.select(
        "company", F.col("k_oid").alias("order_id"), F.col("k_line").alias("line_no"),
        F.col("k_date").alias("order_date"), F.year("k_date").alias("order_year"),
        F.quarter("k_date").alias("order_quarter"), F.month("k_date").alias("order_month"),
        F.col("k_month").alias("order_month_start"), F.dayofweek("k_date").isin(1, 7).alias("is_weekend"),
        F.coalesce("c_id", F.lit(rules.UNKNOWN_ID)).alias("customer_id"),
        F.coalesce("p_id", F.lit(rules.UNKNOWN_ID)).alias("product_id"),
        F.col("k_qty").alias("order_qty"), F.col("r_unit").alias("unit_price"),
        (F.col("k_qty") * F.col("r_unit")).cast("decimal(18,2)").alias("line_amount"),
        F.lit(rules.CURRENCY).alias("currency"), (F.col("k_fmt") != rules.ISO_KEY).alias("date_was_reformatted"),
        cust_invalid.alias("customer_id_was_invalid"), cust_unres.alias("customer_unresolved"),
        prod_unres.alias("product_unresolved"), price_missing.alias("price_missing"), *_lineage())
    run.write_quarantine(raw, keyless, t.filter(F.size("_q_ids") > 0))
    run.merge(valid)
    return run.finish()


ENTITY_FUNCS = {"customers": customers, "products": products, "prices": prices, "orders": orders}
# Orden de dependencias: products antes de prices y orders; customers y prices antes de orders.
ORDER = ["customers", "products", "prices", "orders"]
