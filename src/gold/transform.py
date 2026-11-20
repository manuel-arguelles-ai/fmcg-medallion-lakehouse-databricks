"""Silver -> Gold (Spark, compatible con serverless: solo API DataFrame/SQL, sin APIs de bajo nivel ni caches).

Una funcion por tabla. Cada una RECONSTRUYE su tabla completa desde Silver (o desde Gold aguas arriba)
con INSERT OVERWRITE: correrla dos veces deja la tabla igual (idempotente) y borrar Gold entero se
repara con una corrida (reconstruible). Conserva el DDL (comentarios, CLUSTER BY) porque no recrea la tabla.

Claves sustitutas: xxhash64 de la clave de negocio -> estables entre recargas (la misma fila de Silver
siempre recibe la misma clave), sin secuencias ni identidades. Se verifica unicidad en cada recarga.
Miembro desconocido: -1 en todas las dimensiones. SCD1 (estado actual de Silver); SCD2 queda abierto.
NO VALIDADO EN SPARK todavia (no hay Java local): ver docs/09-gold-implementacion.md.
"""
from __future__ import annotations

import datetime as dt
import uuid

from gold import ddl, model

try:  # pyspark solo existe en el workspace; localmente se importa el modulo solo para inspeccion
    from pyspark.sql import Window
    from pyspark.sql import functions as F
except ImportError:  # pragma: no cover
    F = Window = None


def new_run_id() -> str:
    return f"gold_{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:6]}"


def setup(spark, catalog: str) -> None:
    """Crea (si faltan) las tablas gold.*, ops.gold_runs y (re)crea las vistas de KPI. Idempotente."""
    for stmt in ddl.setup_statements(catalog):
        spark.sql(stmt)


# ------------------------------------------------------------------ utilidades
def _b(c):
    return F.coalesce(c, F.lit(False))


def _month_idx(c):
    """model.month_index en Spark: meses consecutivos difieren en 1."""
    return F.year(c) * 12 + F.month(c) - 1


def _date_sk(c):
    """model.date_sk en Spark: yyyymmdd; NULL -> -1."""
    return F.coalesce(F.year(c) * 10000 + F.month(c) * 100 + F.dayofmonth(c), F.lit(model.UNKNOWN_SK))


def _conform(df, table):
    """Selecciona y castea a los tipos del DDL, en el orden fisico de la tabla."""
    missing = [c for c in ddl.columns(table) if c not in df.columns]
    if missing:
        raise RuntimeError(f"gold.{table}: faltan columnas {missing}")
    return df.select(*[F.col(c).cast(t).alias(c) for c, t in ddl.types(table).items()])


def _audit(silver_ts="_silver_updated_at"):
    src = F.col(silver_ts) if silver_ts else F.lit(None).cast("timestamp")
    return [src.alias("_silver_updated_at"), F.current_timestamp().alias("_gold_loaded_at")]


def _passthrough(table):
    """Columnas de la dimension que se copian tal cual de Silver (todas menos la clave sustituta,
    is_unknown_member y auditoria)."""
    skip = {GOLD_SK[table], "is_unknown_member", "_silver_updated_at", "_gold_loaded_at"}
    return [F.col(c) for c in ddl.columns(table) if c not in skip]


GOLD_SK = {"dim_customer": "customer_sk", "dim_product": "product_sk"}


def _unknown(spark, table):
    return spark.sql(ddl.unknown_member_select(table))


def _assert_unique(df, keys, what):
    dup = df.groupBy(*keys).count().filter(F.col("count") > 1).limit(5).collect()
    if dup:
        raise RuntimeError(f"{what}: claves duplicadas {keys}: {[r.asDict() for r in dup]}")


class Task:
    """Una recarga de una tabla: INSERT OVERWRITE + registro en ops.gold_runs (ok o failed)."""

    def __init__(self, spark, catalog, table, run_id=None):
        self.spark, self.catalog, self.table = spark, catalog, table
        self.run_id = run_id or new_run_id()
        self.started = dt.datetime.now(dt.timezone.utc)
        self.checks: list[str] = []
        self.rows = None

    @property
    def fqn(self):
        return f"{self.catalog}.gold.{self.table}"

    def overwrite(self, df) -> int:
        view = f"_gold_src_{self.table}"
        _conform(df, self.table).createOrReplaceTempView(view)
        self.spark.sql(f"INSERT OVERWRITE TABLE {self.fqn} SELECT * FROM {view}")
        self.rows = self.spark.table(self.fqn).count()
        return self.rows

    def check(self, name, ok, detail=""):
        self.checks.append(f"{name}={'ok' if ok else 'FAIL'}{(' ' + detail) if detail else ''}")
        if not ok:
            raise RuntimeError(f"{self.fqn}: reconciliacion '{name}' fallo {detail}")

    def log(self, status, error=None):
        row = [(self.run_id, self.table, self.fqn, self.rows, "; ".join(self.checks), self.started,
                dt.datetime.now(dt.timezone.utc), status, (str(error)[:1000] if error else None))]
        schema = ", ".join(f"{c} {t}" for c, t, _ in ddl.GOLD_RUNS)
        self.spark.createDataFrame(row, schema).write.mode("append").saveAsTable(f"{self.catalog}.ops.gold_runs")
        print(f"[{self.run_id}] gold.{self.table} status={status} rows={self.rows} checks={self.checks}")
        return {"table": self.table, "run_id": self.run_id, "rows": self.rows, "status": status,
                "checks": list(self.checks)}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc is not None:
            self.log("failed", exc)
        return False   # la excepcion sigue: la tarea del job falla


# ------------------------------------------------------------------ dimensiones
def dim_date(spark, catalog, run_id=None) -> dict:
    """Calendario diario de anios completos que cubren pedidos y precios. Filas generadas con model.date_rows."""
    with Task(spark, catalog, "dim_date", run_id) as t:
        b = spark.sql(f"SELECT MIN(d) AS mn, MAX(d) AS mx FROM ("
                      f"SELECT order_date AS d FROM {catalog}.silver.orders UNION ALL "
                      f"SELECT price_month FROM {catalog}.silver.prices)").first()
        if b["mn"] is None:
            raise RuntimeError("silver.orders/silver.prices vacias: correr Silver antes que Gold")
        start, end = model.calendar_bounds(b["mn"], b["mx"])
        cols = [(c, ty) for c, ty, _ in ddl.GOLD["dim_date"][1]]
        rows = [tuple(r[c] for c, _ in cols) for r in model.date_rows(start, end)]
        df = spark.createDataFrame(rows, ", ".join(f"`{c}` {ty}" for c, ty in cols)).select("*", *_audit(None))
        t.overwrite(df)
        t.check("rows_eq_calendar", t.rows == (end - start).days + 2, f"{start}..{end} +1 desconocido")
        return t.log("ok")


def dim_customer(spark, catalog, run_id=None) -> dict:
    with Task(spark, catalog, "dim_customer", run_id) as t:
        s = spark.table(f"{catalog}.silver.customers").filter(
            ~_b(F.col("is_unknown_member")) & (F.col("customer_id") != "-1"))
        df = s.select(F.xxhash64("customer_key").alias("customer_sk"), *_passthrough("dim_customer"),
                      F.lit(False).alias("is_unknown_member"), *_audit())
        df = df.unionByName(_unknown(spark, "dim_customer"))
        _assert_unique(df, ["customer_sk"], "dim_customer")
        _assert_unique(df, ["company", "customer_id"], "dim_customer")
        t.overwrite(df)
        n_src = s.count()
        t.check("rows_eq_silver_plus_unknown", t.rows == n_src + 1, f"silver={n_src}")
        return t.log("ok")


def dim_product(spark, catalog, run_id=None) -> dict:
    with Task(spark, catalog, "dim_product", run_id) as t:
        s = spark.table(f"{catalog}.silver.products").filter(
            ~_b(F.col("is_unknown_member")) & (F.col("product_id") != "-1"))
        df = s.select(F.xxhash64("product_key").alias("product_sk"), *_passthrough("dim_product"),
                      F.lit(False).alias("is_unknown_member"), *_audit())
        df = df.unionByName(_unknown(spark, "dim_product"))
        _assert_unique(df, ["product_sk"], "dim_product")
        _assert_unique(df, ["company", "product_id"], "dim_product")
        t.overwrite(df)
        n_src = s.count()
        t.check("rows_eq_silver_plus_unknown", t.rows == n_src + 1, f"silver={n_src}")
        return t.log("ok")


def dim_price(spark, catalog, run_id=None) -> dict:
    """Requiere gold.dim_product recargada (toma product_sk de ahi)."""
    with Task(spark, catalog, "dim_price", run_id) as t:
        s = spark.table(f"{catalog}.silver.prices").withColumn("_m", _month_idx(F.col("price_month")))
        prod = spark.table(f"{catalog}.gold.dim_product").filter(~F.col("is_unknown_member")).select(
            "company", "product_id", "product_sk")
        # precio del mes calendario anterior (rangeBetween sobre el indice de mes: un hueco da NULL)
        w = Window.partitionBy("company", "product_id").orderBy("_m").rangeBetween(-1, -1)
        prev = F.max("unit_price").over(w)
        j = s.join(F.broadcast(prod), ["company", "product_id"], "left")
        df = j.select(
            F.xxhash64(F.concat_ws("|", "product_key", F.col("price_month").cast("string"))).alias("price_sk"),
            F.coalesce("product_sk", F.lit(model.UNKNOWN_SK)).alias("product_sk"),
            "company", "product_id", "product_key",
            F.trunc("price_month", "month").alias("valid_from"), F.last_day("price_month").alias("valid_to"),
            "unit_price", "currency", prev.alias("prev_unit_price"),
            F.when(prev != 0, F.round(F.col("unit_price") / prev - 1, 4)).alias("price_change_pct"),
            "price_was_negative", "price_was_invalid", "month_was_reformatted", "product_unresolved",
            F.lit(False).alias("is_unknown_member"), *_audit())
        df = df.unionByName(_unknown(spark, "dim_price"))
        _assert_unique(df, ["price_sk"], "dim_price")
        _assert_unique(df, ["company", "product_id", "valid_from"], "dim_price")
        t.overwrite(df)
        n_src = s.count()
        t.check("rows_eq_silver_plus_unknown", t.rows == n_src + 1, f"silver={n_src}")
        return t.log("ok")


# ------------------------------------------------------------------ hechos
def fact_sales(spark, catalog, run_id=None) -> dict:
    """Linea de pedido. Monto = line_amount de Silver (D28), no se recalcula. Requiere las 4 dimensiones."""
    with Task(spark, catalog, "fact_sales", run_id) as t:
        o = spark.table(f"{catalog}.silver.orders")
        g = f"{catalog}.gold"
        cust = spark.table(f"{g}.dim_customer").filter(~F.col("is_unknown_member")).select(
            "company", "customer_id", "customer_sk")
        prod = spark.table(f"{g}.dim_product").filter(~F.col("is_unknown_member")).select(
            "company", "product_id", "product_sk")
        price = spark.table(f"{g}.dim_price").filter(~F.col("is_unknown_member")).select(
            F.col("company").alias("r_company"), F.col("product_id").alias("r_product_id"),
            "valid_from", "valid_to", "price_sk")
        j = (o.join(F.broadcast(cust), ["company", "customer_id"], "left")
              .join(F.broadcast(prod), ["company", "product_id"], "left"))
        j = j.join(F.broadcast(price), (F.col("company") == F.col("r_company"))
                   & (F.col("product_id") == F.col("r_product_id"))
                   & F.col("order_date").between(F.col("valid_from"), F.col("valid_to")), "left")
        unk = F.lit(model.UNKNOWN_SK)
        df = j.select(
            "company", "order_id", "line_no", _date_sk(F.col("order_date")).alias("date_sk"),
            F.coalesce("customer_sk", unk).alias("customer_sk"), F.coalesce("product_sk", unk).alias("product_sk"),
            F.coalesce("price_sk", unk).alias("price_sk"), "order_date", "order_month_start", "order_qty",
            "unit_price", "line_amount", "currency", "price_missing", "customer_id_was_invalid",
            "customer_unresolved", "product_unresolved", "date_was_reformatted", *_audit())
        t.overwrite(df)
        n_src = o.count()
        t.check("rows_eq_silver", t.rows == n_src, f"silver={n_src}")
        # informativo: lineas con monto pero sin precio vigente en dim_price (deberia ser 0)
        incoherent = spark.table(t.fqn).filter(F.col("line_amount").isNotNull() & (F.col("price_sk") == -1)).count()
        t.checks.append(f"amount_without_price_sk={incoherent}")
        return t.log("ok")


def agg_sales_monthly(spark, catalog, run_id=None) -> dict:
    """Mes x compania x cliente x producto x moneda, derivado de gold.fact_sales."""
    with Task(spark, catalog, "agg_sales_monthly", run_id) as t:
        f = spark.table(f"{catalog}.gold.fact_sales")
        df = (f.groupBy(F.col("order_month_start").alias("month_start"), "company", "customer_sk", "product_sk",
                        "currency")
              .agg(F.count(F.lit(1)).alias("order_lines"), F.countDistinct("order_id").alias("orders"),
                   F.sum("order_qty").alias("total_qty"), F.sum("line_amount").alias("revenue"),
                   F.sum(F.when(_b(F.col("price_missing")), 1).otherwise(0)).alias("lines_without_price"),
                   F.max("_silver_updated_at").alias("_silver_updated_at"))
              .withColumn("year", F.year("month_start")).withColumn("quarter", F.quarter("month_start"))
              .withColumn("month", F.month("month_start"))
              .withColumn("_gold_loaded_at", F.current_timestamp()))
        t.overwrite(df)
        a = spark.table(t.fqn).agg(F.sum("order_lines").alias("l"), F.sum("total_qty").alias("q"),
                                   F.sum("revenue").alias("r")).first()
        b = f.agg(F.count(F.lit(1)).alias("l"), F.sum("order_qty").alias("q"), F.sum("line_amount").alias("r")).first()
        t.check("totals_eq_fact", (a["l"], a["q"], a["r"]) == (b["l"], b["q"], b["r"]),
                f"lineas={a['l']} qty={a['q']} revenue={a['r']}")
        return t.log("ok")


def feature_sales_monthly(spark, catalog, run_id=None) -> dict:
    """Features ML (model.monthly_features en Spark): lags de calendario via rangeBetween sobre el indice de mes."""
    with Task(spark, catalog, "feature_sales_monthly", run_id) as t:
        a = spark.table(f"{catalog}.gold.agg_sales_monthly").withColumn("_m", _month_idx(F.col("month_start")))
        max_m = a.agg(F.max("_m")).first()[0]
        if max_m is None:
            raise RuntimeError("gold.agg_sales_monthly vacia")
        keys = ["company", "customer_sk", "product_sk", "currency"]
        wr = Window.partitionBy(*keys).orderBy("_m")
        zero = F.lit(0).cast("decimal(28,2)")

        def at(col, k, default):
            return F.coalesce(F.sum(col).over(wr.rangeBetween(k, k)), default)

        lags = {}
        for k in model.FEATURE_LAGS:
            lags[f"revenue_lag_{k}"] = at("revenue", -k, zero)
            lags[f"qty_lag_{k}"] = at("total_qty", -k, F.lit(0).cast("bigint"))
        df = a.select("*", *[c.alias(n) for n, c in lags.items()],
                      (F.col("_m") - F.lag("_m", 1).over(wr)).alias("months_since_prev_purchase"),
                      F.row_number().over(wr).alias("purchase_month_seq"),
                      F.when(F.col("_m") + 1 <= F.lit(max_m), at("revenue", 1, zero)).alias("target_revenue_next_month"))
        df = df.withColumn("revenue_avg_3m", F.round(
            sum((F.col(f"revenue_lag_{k}") for k in model.FEATURE_LAGS), F.lit(0)) / len(model.FEATURE_LAGS), 2))
        df = df.withColumn("_gold_loaded_at", F.current_timestamp())   # _silver_updated_at viene del agregado
        t.overwrite(df)
        n_src = a.count()
        t.check("rows_eq_agg", t.rows == n_src, f"agg={n_src}")
        return t.log("ok")


TASK_FUNCS = {
    "dim_date": dim_date, "dim_customer": dim_customer, "dim_product": dim_product, "dim_price": dim_price,
    "fact_sales": fact_sales, "agg_sales_monthly": agg_sales_monthly, "feature_sales_monthly": feature_sales_monthly,
}
# Dependencias dentro de Gold (el job las replica; todas dependen ademas de la tarea `setup`).
DEPENDS = {
    "dim_date": [], "dim_customer": [], "dim_product": [], "dim_price": ["dim_product"],
    "fact_sales": ["dim_date", "dim_customer", "dim_product", "dim_price"],
    "agg_sales_monthly": ["fact_sales"], "feature_sales_monthly": ["agg_sales_monthly"],
}
ORDER = ["dim_date", "dim_customer", "dim_product", "dim_price", "fact_sales", "agg_sales_monthly",
         "feature_sales_monthly"]
