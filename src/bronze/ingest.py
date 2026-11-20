"""Ingesta Bronze de UNA fuente (compania x fuente) con Auto Loader. Pensado para serverless.

Uso (spark_python_task): ingest.py --catalog fmcg_lakehouse --company madre --source orders

Flujo: 1) pre-validacion de archivos nuevos (-> ops.landing_rejects)  2) Auto Loader availableNow con
esquema declarado STRING  3) append a bronze.<compania>_<fuente>  4) tabla de paso con SOLO el lote
nuevo  5) registro en ops.bronze_runs. No mueve archivos de landing; el checkpoint es la verdad.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import uuid

# Permite `import bronze.*` al correr como script suelto (spark_python_task) o como modulo.
try:
    _SRC = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except NameError:  # ejecucion sin __file__: se asume cwd = raiz del bundle
    _SRC = os.path.join(os.getcwd(), "src")
sys.path.insert(0, _SRC)

from bronze import schemas, validation  # noqa: E402
from bronze.registry import SourceSpec, get_spec, ops_ddl  # noqa: E402

RUNS_SCHEMA = (
    "batch_id STRING, company STRING, source STRING, target_table STRING, load_type STRING, "
    "started_at TIMESTAMP, finished_at TIMESTAMP, status STRING, files_new INT, files_rejected INT, "
    "rows_read BIGINT, rows_written BIGINT, rows_rescued BIGINT, rows_rejected BIGINT, error STRING"
)
REJECTS_SCHEMA = ("rejected_at TIMESTAMP, batch_id STRING, company STRING, source STRING, "
                  "file_path STRING, file_size BIGINT, rule_id STRING, reason STRING")


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingesta Bronze (Auto Loader)")
    p.add_argument("--catalog", default="fmcg_lakehouse")
    p.add_argument("--company", required=True, choices=["madre", "hija"])
    p.add_argument("--source", required=True, choices=["customers", "products", "prices", "orders"])
    p.add_argument("--generation", default="1",
                   help="generacion del checkpoint; subirla SOLO si se recrea el checkpoint (txnAppId)")
    a, _ = p.parse_known_args(argv)
    return a


def new_batch_id(spec: SourceSpec, now: dt.datetime) -> str:
    """Id unico de corrida: <compania>_<fuente>_<UTC>_<sufijo>."""
    return f"{spec.company}_{spec.source}_{now:%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:6]}"


def paths(spec: SourceSpec, catalog: str) -> dict:
    """Rutas y nombres derivados del registro."""
    ckpt = f"/Volumes/{catalog}/ops/checkpoints/bronze/{spec.company}_{spec.source}"
    return {
        "landing": spec.landing_path(catalog),
        "checkpoint": f"{ckpt}/stream",
        "schema_location": f"{ckpt}/schema",
        "bronze": f"{catalog}.bronze.{spec.table}",
        "staging": f"{catalog}.bronze.{spec.staging_table}",
    }


def run(spark, catalog: str, company: str, source: str, generation: str = "1") -> dict:
    from pyspark.sql import functions as F

    spec = get_spec(company, source)
    p = paths(spec, catalog)
    started = dt.datetime.now(dt.timezone.utc)
    batch_id = new_batch_id(spec, started)
    stats = dict(files_new=0, files_rejected=0, rows_read=0, rows_written=0, rows_rescued=0, rows_rejected=0)
    status, error = "SUCCEEDED", None
    try:
        for ddl in ops_ddl(catalog):
            spark.sql(ddl)
        spark.sql(schemas.table_ddl(spec, catalog))
        spark.sql(schemas.table_ddl(spec, catalog, staging=True))

        # 1) pre-validacion de lo que aun no se cargo ni se rechazo
        known = {validation.normalize_path(r[0]) for r in spark.sql(
            f"SELECT DISTINCT _source_file FROM {p['bronze']}").collect()}
        known |= {validation.normalize_path(r[0]) for r in spark.sql(
            f"SELECT DISTINCT file_path FROM {catalog}.ops.landing_rejects "
            f"WHERE company = '{company}' AND source = '{source}'").collect()}
        accepted, rejects = validation.validate_landing(p["landing"], spec, known)
        stats["files_new"], stats["files_rejected"] = len(accepted), len({r.file_path for r in rejects})
        if rejects:
            spark.createDataFrame(
                [(started, batch_id, company, source, r.file_path, r.file_size, r.rule_id, r.reason)
                 for r in rejects], REJECTS_SCHEMA
            ).write.mode("append").saveAsTable(f"{catalog}.ops.landing_rejects")
        rejected_paths = sorted({r.file_path for r in rejects})

        # 2) Auto Loader: esquema declarado (todo STRING), sin inferSchema, lo que no encaja -> _rescued_data
        stream = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", p["schema_location"])
            .option("header", "true").option("delimiter", spec.delimiter)
            .option("quote", '"').option("escape", '"')
            .option("rescuedDataColumn", schemas.RESCUED)
            .schema(schemas.ddl_schema_string(spec))
            .load(p["landing"])
        )
        folder_date = F.regexp_extract(F.col("_metadata.file_path"), r"ingest_date=(\d{4}-\d{2}-\d{2})", 1)
        enriched = (
            stream
            .withColumn("_company", F.lit(spec.company))
            .withColumn("_source_system", F.lit(spec.source_system))
            .withColumn("_source", F.lit(spec.source))
            .withColumn("_source_file", F.regexp_replace(F.col("_metadata.file_path"), "^dbfs:", ""))
            .withColumn("_file_size", F.col("_metadata.file_size"))
            .withColumn("_file_modification_time", F.col("_metadata.file_modification_time"))
            .withColumn("_ingest_ts", F.lit(started))
            .withColumn("_ingest_date", F.try_to_date(folder_date, F.lit("yyyy-MM-dd")))
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_load_type", F.lit(spec.load_type))
            .select(*schemas.table_columns(spec))
        )

        def write_batch(batch_df, micro_batch_id):
            out = batch_df
            if rejected_paths:  # archivos rechazados: consumidos por el checkpoint pero sin entrar a Bronze
                out = out.filter(~F.col("_source_file").isin(rejected_paths))
            (out.write.format("delta").mode("append")
                .option("txnAppId", f"bronze_{spec.table}_g{generation}")
                .option("txnVersion", micro_batch_id)
                .saveAsTable(p["bronze"]))

        query = (enriched.writeStream.foreachBatch(write_batch)
                 .option("checkpointLocation", p["checkpoint"])
                 .trigger(availableNow=True).start())
        query.awaitTermination()
        stats["rows_read"] = sum(int(x.get("numInputRows", 0)) for x in query.recentProgress)

        # 3) conteos del lote desde la propia tabla Bronze
        row = (spark.table(p["bronze"]).where(F.col("_batch_id") == batch_id)
               .agg(F.count(F.lit(1)).alias("n"),
                    F.count(F.when(F.col(schemas.RESCUED).isNotNull(), 1)).alias("r")).first())
        stats["rows_written"], stats["rows_rescued"] = int(row["n"]), int(row["r"])
        stats["rows_rejected"] = max(0, stats["rows_read"] - stats["rows_written"])

        # 4) tabla de paso: SOLO el lote nuevo (si no hubo filas nuevas se conserva el ultimo lote con datos)
        if stats["rows_written"] > 0:
            (spark.table(p["bronze"]).where(F.col("_batch_id") == batch_id)
             .write.format("delta").mode("overwrite").saveAsTable(p["staging"]))
    except Exception as e:  # se registra y se vuelve a lanzar
        status, error = "FAILED", str(e)[:2000]
        raise
    finally:
        spark.createDataFrame(
            [(batch_id, company, source, p["bronze"], spec.load_type, started,
              dt.datetime.now(dt.timezone.utc), status, stats["files_new"], stats["files_rejected"],
              stats["rows_read"], stats["rows_written"], stats["rows_rescued"], stats["rows_rejected"], error)],
            RUNS_SCHEMA).write.mode("append").saveAsTable(f"{catalog}.ops.bronze_runs")
    print(f"[{batch_id}] {status} {stats}")
    return {"batch_id": batch_id, "status": status, **stats}


def main(argv=None):
    from pyspark.sql import SparkSession
    a = parse_args(argv)
    run(SparkSession.builder.getOrCreate(), a.catalog, a.company, a.source, a.generation)


if __name__ == "__main__":
    main()
