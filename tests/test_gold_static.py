"""Chequeos estaticos de la parte Spark de Gold (sin Spark): sintaxis, serverless, tareas y argumentos."""
import re
from pathlib import Path

import pytest

from gold import ddl, run, transform

SRC = Path(__file__).resolve().parents[1] / "src" / "gold"
TRANSFORM = (SRC / "transform.py").read_text(encoding="utf-8")
RUN = (SRC / "run.py").read_text(encoding="utf-8")


def test_python_files_compile():
    for f in SRC.glob("*.py"):
        compile(f.read_text(encoding="utf-8"), str(f), "exec")


def test_transform_is_serverless_friendly():
    for banned in ("sparkContext", ".rdd", ".cache(", ".persist(", "createGlobalTempView", "inferSchema",
                   "toPandas", "monotonically_increasing_id"):
        assert banned not in TRANSFORM, banned
    # recarga completa sin recrear la tabla (conserva comentarios y CLUSTER BY del DDL)
    assert "INSERT OVERWRITE TABLE" in TRANSFORM
    assert "CREATE OR REPLACE TABLE" not in TRANSFORM and '.mode("overwrite")' not in TRANSFORM
    # no escribe en Silver/Bronze
    assert not re.search(r"(INSERT|MERGE|DELETE)[^\n]*\.(silver|bronze)\.", TRANSFORM)


def test_runner_resolves_path_without_dunder_file_dependency():
    assert "inspect.currentframe().f_code.co_filename" in RUN and "except NameError" in RUN


def test_one_function_per_table_and_order_respects_dependencies():
    assert set(transform.TASK_FUNCS) == set(ddl.TABLES) == set(transform.ORDER) == set(transform.DEPENDS)
    for t in ddl.TABLES:
        assert re.search(rf"^def {t}\(spark, catalog, run_id=None\)", TRANSFORM, re.M), t
    pos = {t: i for i, t in enumerate(transform.ORDER)}
    for t, deps in transform.DEPENDS.items():
        assert all(pos[d] < pos[t] for d in deps), t


def test_every_table_reconciles_and_logs():
    for t in ddl.TABLES:
        body = TRANSFORM.split(f"def {t}(", 1)[1].split("\ndef ", 1)[0]
        assert "t.overwrite(" in body and "t.check(" in body and 't.log("ok")' in body, t


def test_dimensions_get_unknown_member_and_uniqueness_check():
    for t in ("dim_customer", "dim_product", "dim_price"):
        body = TRANSFORM.split(f"def {t}(", 1)[1].split("\ndef ", 1)[0]
        assert f'_unknown(spark, "{t}")' in body and "_assert_unique" in body, t


def test_run_args_defaults_and_choices():
    a = run.parse_args(["--task", "fact_sales"])
    assert (a.catalog, a.task, a.skip_setup, a.run_id) == ("fmcg_lakehouse", "fact_sales", False, None)
    a = run.parse_args(["--task", "all", "--catalog", "x", "--skip-setup", "--run-id", "42"])
    assert (a.task, a.catalog, a.skip_setup, a.run_id) == ("all", "x", True, "42")
    assert run.parse_args(["--task", "setup"]).task == "setup"
    with pytest.raises(SystemExit):
        run.parse_args(["--task", "nope"])
