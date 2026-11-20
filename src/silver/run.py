"""Entrypoint de Silver para UNA entidad (ambas companias), pensado para serverless.

Uso (spark_python_task):
  run.py --catalog fmcg_lakehouse --entity orders --mode incremental [--company hija] [--skip-setup]
`--entity all` corre las 4 en el orden de dependencias (customers, products, prices, orders).
No escribe nada fuera de silver.*, quarantine.*, ops.dq_results y silver.ref_* (siembra idempotente).
"""
from __future__ import annotations

import argparse
import os
import sys

# Permite `import silver.*` al correr como script suelto (spark_python_task) o como modulo.
try:
    _SRC = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except NameError:  # ejecucion sin __file__: se asume cwd = raiz del bundle
    _SRC = os.path.join(os.getcwd(), "src")
sys.path.insert(0, _SRC)

from silver import transform  # noqa: E402


def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Transformacion Bronze -> Silver")
    p.add_argument("--catalog", default="fmcg_lakehouse")
    p.add_argument("--entity", required=True, choices=[*transform.ORDER, "all"])
    p.add_argument("--mode", default="incremental", choices=["incremental", "full"],
                   help="incremental = bronze.staging_*; full = tabla Bronze completa (carga inicial/reproceso)")
    p.add_argument("--company", choices=["madre", "hija"], help="opcional; por defecto ambas")
    p.add_argument("--skip-setup", action="store_true", help="no ejecutar DDL/siembra idempotentes")
    a, _ = p.parse_known_args(argv)
    return a


def main(argv=None):
    from pyspark.sql import SparkSession
    a = parse_args(argv)
    spark = SparkSession.builder.getOrCreate()
    companies = (a.company,) if a.company else transform.COMPANIES
    if not a.skip_setup:
        transform.setup(spark, a.catalog)
    for entity in (transform.ORDER if a.entity == "all" else [a.entity]):
        transform.ENTITY_FUNCS[entity](spark, a.catalog, a.mode, companies)


if __name__ == "__main__":
    main()
