"""Entrypoint de Gold para UNA tarea (tabla), pensado para serverless.

Uso (spark_python_task):
  run.py --catalog fmcg_lakehouse --task fact_sales [--skip-setup]
`--task setup` solo crea tablas/vistas (idempotente). `--task all` hace setup y recarga las 7 tablas
en el orden de dependencias. Cada tabla se reconstruye completa desde Silver (INSERT OVERWRITE).
No escribe nada fuera de gold.* y ops.gold_runs.
"""
from __future__ import annotations

import argparse
import os
import sys

# Permite `import gold.*` al correr como script suelto (spark_python_task) o como modulo.
import inspect

try:
    _THIS = os.path.abspath(__file__)
except NameError:  # el runner de serverless hace exec(compile(src, filename)): __file__ no existe,
    _THIS = os.path.abspath(inspect.currentframe().f_code.co_filename)  # pero el filename si
_SRC = os.path.dirname(os.path.dirname(_THIS))
sys.path.insert(0, _SRC)

from gold import transform  # noqa: E402


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Transformacion Silver -> Gold")
    p.add_argument("--catalog", default="fmcg_lakehouse")
    p.add_argument("--task", required=True, choices=["setup", *transform.ORDER, "all"])
    p.add_argument("--run-id", help="id comun para ops.gold_runs (el job pasa {{job.run_id}})")
    p.add_argument("--skip-setup", action="store_true", help="no ejecutar DDL/vistas (el job lo hace en la tarea setup)")
    a, _ = p.parse_known_args(argv)
    return a


def main(argv=None):
    from pyspark.sql import SparkSession
    a = parse_args(argv)
    spark = SparkSession.builder.getOrCreate()
    if a.task == "setup" or not a.skip_setup:
        transform.setup(spark, a.catalog)
    if a.task == "setup":
        return
    run_id = f"gold_job_{a.run_id}" if a.run_id else transform.new_run_id()
    for task in (transform.ORDER if a.task == "all" else [a.task]):
        transform.TASK_FUNCS[task](spark, a.catalog, run_id)


if __name__ == "__main__":
    main()
