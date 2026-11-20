"""Chequeos estaticos de la parte Spark de Silver (sin Spark): sintaxis, reglas y parametros."""
import re
from pathlib import Path

import pytest

from silver import ddl, rules, run

SRC = Path(__file__).resolve().parents[1] / "src" / "silver"
TRANSFORM = (SRC / "transform.py").read_text(encoding="utf-8")


def test_python_files_compile():
    for f in SRC.glob("*.py"):
        compile(f.read_text(encoding="utf-8"), str(f), "exec")


def test_every_rule_is_used_by_transform_and_every_reference_exists():
    used = set(re.findall(r'"([A-Z]{3}_\d\d)"', TRANSFORM))
    assert used == {r.rule_id for r in rules.RULES}


def test_transform_is_serverless_friendly():
    for banned in ("sparkContext", ".rdd", ".cache(", ".persist(", "createGlobalTempView", "inferSchema"):
        assert banned not in TRANSFORM
    assert ".write.mode(\"overwrite\")" not in TRANSFORM   # solo MERGE/append; nada de overwrite en Silver


def test_transform_functions_exist_per_entity():
    for e in ddl.ENTITIES:
        assert re.search(rf"^def {e}\(spark, catalog, mode=", TRANSFORM, re.M)
    assert "ORDER = [" in TRANSFORM


def test_run_args_defaults_and_choices():
    a = run.parse_args(["--entity", "orders"])
    assert (a.catalog, a.mode, a.company, a.skip_setup) == ("fmcg_lakehouse", "incremental", None, False)
    a = run.parse_args(["--entity", "all", "--mode", "full", "--company", "hija", "--catalog", "x"])
    assert (a.entity, a.mode, a.company, a.catalog) == ("all", "full", "hija", "x")
    with pytest.raises(SystemExit):
        run.parse_args(["--entity", "nope"])


def test_dependency_order():
    from silver import transform
    o = transform.ORDER
    assert o.index("products") < o.index("prices") < o.index("orders") and o.index("customers") < o.index("orders")
