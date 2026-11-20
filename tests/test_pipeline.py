"""Orquestacion (job maestro), reconciliacion entre capas (parte pura), MERGE incremental y lote incremental."""
import shutil
from pathlib import Path

import pytest
import yaml

from datagen import catalog_data as cd
from datagen.increment import build_lot, write_lot
from pipeline import reconcile
from silver import transform
from silver.expected import expected

ROOT = Path(__file__).resolve().parents[1]
JOB = yaml.safe_load((ROOT / "resources" / "pipeline_job.yml").read_text(encoding="utf-8"))["resources"]["jobs"]


# ---------------------------------------------------------------- job maestro
def test_master_job_chains_bronze_silver_gold_then_reconcile():
    job = JOB["medallion_pipeline"]
    assert job["name"] == "fmcg-medallion-pipeline" and job["max_concurrent_runs"] == 1
    assert {p["name"]: p["default"] for p in job["parameters"]} == {"catalog": "${var.catalog}", "mode": "incremental"}
    tasks = {t["task_key"]: t for t in job["tasks"]}
    assert list(tasks) == ["bronze", "silver", "gold", "reconcile"]
    deps = {k: [d["task_key"] for d in t.get("depends_on", [])] for k, t in tasks.items()}
    assert deps == {"bronze": [], "silver": ["bronze"], "gold": ["silver"], "reconcile": ["gold"]}
    assert tasks["bronze"]["run_job_task"]["job_id"] == "${resources.jobs.bronze_ingest.id}"
    assert tasks["silver"]["run_job_task"]["job_id"] == "${resources.jobs.silver_transform.id}"
    assert tasks["silver"]["run_job_task"]["job_parameters"]["mode"] == "{{job.parameters.mode}}"
    assert tasks["gold"]["run_job_task"]["job_id"] == "${resources.jobs.gold_build.id}"
    params = tasks["reconcile"]["spark_python_task"]["parameters"]
    assert params[params.index("--mode") + 1] == "{{job.parameters.mode}}"
    assert (ROOT / "resources" / tasks["reconcile"]["spark_python_task"]["python_file"]).resolve() == \
        (ROOT / "src" / "pipeline" / "reconcile.py")


def test_bundle_includes_master_job_and_syncs_its_code():
    b = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
    assert "resources/pipeline_job.yml" in b["include"]
    assert "src/pipeline/**" in b["sync"]["include"]
    bronze = b["resources"]["jobs"]["bronze_ingest"]
    assert [p["name"] for p in bronze["parameters"]] == ["catalog"]   # el maestro le pasa el catalogo
    assert all("{{job.parameters.catalog}}" in t["spark_python_task"]["parameters"] for t in bronze["tasks"])


# ---------------------------------------------------------------- reconciliacion (puro)
def _ok_metrics(entity="orders"):
    m = dict(landing_files=10, rejected_files=1, landing_rows=100, bronze_files=9, bronze_rows=100,
             bronze_keyless=2, bronze_keys=90, silver_rows=85, quarantine_rows=7, quarantine_keyless=2,
             silver_quarantine_keys=90, all_keys=90, gold_rows=85,
             silver_amount=None, gold_amount=None, agg_amount=None)
    if entity == "orders":
        m.update(silver_amount=12.5, gold_amount=12.5, agg_amount=12.5)
    return m


def test_evaluate_ok():
    for e in reconcile.ENTITIES:
        assert reconcile.evaluate(e, _ok_metrics(e)) == []


@pytest.mark.parametrize("change,check", [
    ({"bronze_files": 8}, "L1"), ({"landing_rows": 99}, "L2"), ({"quarantine_keyless": 1}, "B1"),
    ({"silver_quarantine_keys": 89}, "B2"), ({"all_keys": 91}, "B2"), ({"gold_rows": 84}, "G1"),
    ({"agg_amount": 12.4}, "G2"),
])
def test_evaluate_detects_each_broken_identity(change, check):
    fails = reconcile.evaluate("orders", {**_ok_metrics(), **change})
    assert len(fails) == 1 and fails[0].startswith(check)


def test_amount_check_only_for_orders():
    assert reconcile.evaluate("customers", {**_ok_metrics("customers"), "agg_amount": 1}) == []


def test_pipeline_runs_ddl_is_idempotent_and_matches_metrics():
    ddl = reconcile.table_ddl("cat")
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS cat.ops.pipeline_runs")
    assert set(reconcile.METRICS) == set(_ok_metrics())
    for c, _, comment in reconcile.COLUMNS:
        assert f"{c} " in ddl and "'" not in comment


def test_reconcile_args():
    a = reconcile.parse_args(["--mode", "full", "--run-id", "7"])
    assert (a.catalog, a.mode, a.run_id) == ("fmcg_lakehouse", "full", "7")


# ---------------------------------------------------------------- MERGE incremental
def test_merge_condition_strict_only_in_incremental():
    assert transform.merge_condition("incremental") == "s._ingest_ts > t._ingest_ts"
    assert transform.merge_condition("full") == "s._ingest_ts >= t._ingest_ts"


# ---------------------------------------------------------------- lote incremental
@pytest.fixture()
def gen_copy(small_gen, tmp_path):
    src, _ = small_gen
    dst = tmp_path / "gen"
    shutil.copytree(src, dst)
    return dst


def test_lot_is_deterministic_and_seeds_anomalies(gen_copy):
    a = build_lot(gen_copy)
    assert a == build_lot(gen_copy)
    ingest, rows, desc = a
    assert desc["anomalies"]["order_qty_null"] == 1 and desc["anomalies"]["customer_id_invalid"] == 1
    assert desc["anomalies"]["dup_rows"] == 1 and desc["anomalies"]["dup_existing"] == 1
    assert all(len(v) == len(cd.COLUMNS["orders"]) for v, _ in rows)
    lo, hi = desc["order_id_range"]
    new_ids = {int(v[0]) for v, t in rows if "dup_existing" not in t}
    assert new_ids == set(range(lo, hi + 1))


def test_lot_effect_matches_pure_rules(gen_copy):
    before = expected(gen_copy)[("orders", "hija")]
    desc = write_lot(gen_copy)
    after = expected(gen_copy)[("orders", "hija")]
    n = desc["rows"]
    assert after["bronze"] == before["bronze"] + n
    # nuevas validas = filas - 1 qty nula - 1 duplicado interno - 1 linea ya existente
    assert after["silver"] == before["silver"] + n - 3
    assert after["quarantine_rules"]["ORD_03"] == before["quarantine_rules"].get("ORD_03", 0) + 1
    assert write_lot(gen_copy) == desc   # reescribir el mismo lote es un no-op (mismo contenido)


def test_lot_refuses_to_overwrite_different_content(gen_copy):
    desc = write_lot(gen_copy)
    (gen_copy / desc["path"]).write_text("otro", encoding="utf-8")
    with pytest.raises(RuntimeError, match="inmutable"):
        write_lot(gen_copy)


def test_master_job_has_paused_schedule_and_retries():
    """ORQ-13: disparador declarado y pausado en dev. ORQ-14: reintento en cada tarea que ejecuta codigo
    (las run_job_task no admiten max_retries: reintentan las tareas de los jobs hijos)."""
    job = JOB["medallion_pipeline"]
    assert job["schedule"]["pause_status"] == "PAUSED" and job["schedule"]["quartz_cron_expression"]
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))["resources"]["jobs"]
    jobs = {**bundle, **JOB}
    for f in ("silver_job.yml", "gold_job.yml"):
        jobs.update(yaml.safe_load((ROOT / "resources" / f).read_text(encoding="utf-8"))["resources"]["jobs"])
    for name, j in jobs.items():
        for t in j["tasks"]:
            if "run_job_task" in t:
                assert "max_retries" not in t, (name, t["task_key"])
            else:
                assert t["max_retries"] == 1 and t["min_retry_interval_millis"] >= 60000, (name, t["task_key"])


def test_bundle_declares_the_versioned_dashboard():
    """SRV-01: el dashboard del repo esta declarado como recurso del bundle (enlazado al existente)."""
    b = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
    assert "resources/dashboard.yml" in b["include"]
    d = yaml.safe_load((ROOT / "resources" / "dashboard.yml").read_text(encoding="utf-8"))
    dash = d["resources"]["dashboards"]["fmcg_sales_insights"]
    assert (ROOT / "resources" / dash["file_path"]).resolve() == ROOT / "dashboards" / "fmcg_sales_insights.lvdash.json"
    assert dash["embed_credentials"] is True and dash["warehouse_id"]
