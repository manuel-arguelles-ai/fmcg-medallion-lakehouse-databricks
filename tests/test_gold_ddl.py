"""DDL de Gold: sincronia con el SQL versionado, coherencia con Silver, moneda y vistas."""
import re
from pathlib import Path

import yaml

import importlib.util

from gold import ddl, transform
from silver import ddl as sddl

ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("run_sql", ROOT / "infra" / "run_sql.py")
_run_sql = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_run_sql)
split_statements = _run_sql.split_statements
SQL_FILE = ROOT / "src" / "gold" / "sql" / "01_gold_ddl.sql"
JOB_FILE = ROOT / "resources" / "gold_job.yml"


def test_sql_file_in_sync_with_generator():
    text = SQL_FILE.read_text(encoding="utf-8")
    assert text == ddl.render_sql("fmcg_lakehouse")
    assert text.startswith("-- NO EJECUTADO")


def test_sql_splits_into_the_expected_statements():
    """infra/run_sql.py separa por ';': ningun literal/comentario debe romper una sentencia."""
    stmts = split_statements(SQL_FILE.read_text(encoding="utf-8"))
    assert stmts == [s.strip() for s in ddl.setup_statements("fmcg_lakehouse")]
    assert len(stmts) == len(ddl.TABLES) + 1 + len(ddl.VIEW_COMMENTS)


def test_statements_are_idempotent_and_only_touch_gold_or_ops_gold_runs():
    for s in ddl.setup_statements("c"):
        assert s.startswith(("CREATE TABLE IF NOT EXISTS c.gold.", "CREATE OR REPLACE VIEW c.gold.",
                             "CREATE TABLE IF NOT EXISTS c.ops.gold_runs"))
        code = re.sub(r"'(?:[^']|'')*'", "''", s)        # sin literales (comentarios de columna)
        assert not re.search(r"\b(DROP|DELETE|TRUNCATE|INSERT|MERGE|UPDATE)\b", code)


def test_columns_unique_keys_present_and_sk_types():
    for t, (key, cols, cluster) in ddl.GOLD.items():
        names = ddl.columns(t)
        assert len(names) == len(set(names)) and set(key) <= set(names), t
        assert not cluster or set(cluster) <= set(names)
        for c, typ, _ in cols:
            if c.endswith("_sk") and c != "date_sk":
                assert typ == "BIGINT", (t, c)


def test_no_hardcoded_currency_and_amounts_carry_currency():
    """Moneda en PEN por decision de Manu, pero Gold no fija ninguna: viaja en la columna currency."""
    text = SQL_FILE.read_text(encoding="utf-8")
    for code in ("INR", "PEN", "USD"):
        assert not re.search(rf"\b{code}\b", text), code
    for t, (_, cols, _) in ddl.GOLD.items():
        names = {c for c, _, _ in cols}
        if any(c in names for c in ("line_amount", "revenue", "unit_price")):
            assert "currency" in names, t


def test_passthrough_columns_exist_in_silver():
    """Lo que Gold copia de Silver existe en el DDL de Silver (nombre a nombre)."""
    for gt, se in (("dim_customer", "customers"), ("dim_product", "products")):
        skip = {transform.GOLD_SK[gt], "is_unknown_member", "_silver_updated_at", "_gold_loaded_at"}
        assert {c for c in ddl.columns(gt) if c not in skip} <= set(sddl.columns(se)), gt
    price_from_silver = {"company", "product_id", "product_key", "unit_price", "currency", "price_was_negative",
                         "price_was_invalid", "month_was_reformatted", "product_unresolved"}
    assert price_from_silver <= set(sddl.columns("prices"))
    fact_from_silver = {"company", "order_id", "line_no", "order_date", "order_month_start", "order_qty",
                        "unit_price", "line_amount", "currency", "price_missing", "customer_id_was_invalid",
                        "customer_unresolved", "product_unresolved", "date_was_reformatted"}
    assert fact_from_silver <= set(sddl.columns("orders"))
    assert fact_from_silver <= set(ddl.columns("fact_sales"))


def test_silver_types_preserved_in_gold():
    s = {c: t for c, t, _ in sddl.SILVER["orders"][1]}
    g = ddl.types("fact_sales")
    for c in ("line_amount", "unit_price", "order_qty", "order_date", "line_no"):
        assert g[c] == s[c], c


def test_unknown_member_select_covers_all_columns():
    for t in ("dim_customer", "dim_product", "dim_price"):
        sql = ddl.unknown_member_select(t)
        aliases = re.findall(r" AS `(\w+)`", sql)
        assert aliases == ddl.columns(t)
        assert "-1 AS `" in sql and "true AS `is_unknown_member`" in sql


def test_views_reference_existing_columns_and_tables():
    v = ddl.views("c")
    assert set(v) == set(ddl.VIEW_COMMENTS)
    assert "c.ops.dq_results" in v["v_kpi_data_quality"]
    for e in ("customers", "products", "prices", "orders"):
        assert f"c.quarantine.{e}" in v["v_kpi_quarantine_summary"]
    assert "revenue_rank <= 10" in v["v_kpi_top10_products"]
    # cada columna f./a./p./d. citada en las vistas existe en su tabla
    alias = {"f": "fact_sales", "a": "agg_sales_monthly", "p": "dim_product", "d": "dim_date"}
    for name, sql in v.items():
        for a, col in re.findall(r"\b([fapd])\.(\w+)", sql):
            assert col in ddl.columns(alias[a]), (name, a, col)


def test_quality_view_embeds_all_silver_rules():
    from silver import rules
    sql = ddl.views("c")["v_kpi_data_quality"]
    assert all(f"'{r.rule_id}'" in sql for r in rules.RULES)


def test_job_tasks_and_dependencies_match_transform():
    job = yaml.safe_load(JOB_FILE.read_text(encoding="utf-8"))["resources"]["jobs"]["gold_build"]
    tasks = {t["task_key"]: t for t in job["tasks"]}
    assert set(tasks) == {"setup", *transform.ORDER}
    assert "depends_on" not in tasks["setup"]
    for k in transform.ORDER:
        deps = {d["task_key"] for d in tasks[k].get("depends_on", [])}
        assert deps == (set(transform.DEPENDS[k]) or {"setup"}), k
        params = tasks[k]["spark_python_task"]["parameters"]
        assert params[params.index("--task") + 1] == k and "--skip-setup" in params
        assert tasks[k]["spark_python_task"]["python_file"] == "../src/gold/run.py"
    assert {p["name"] for p in job["parameters"]} == {"catalog"}



COMMENTS_FILE = ROOT / "src" / "gold" / "sql" / "02_gold_comments.sql"


def test_every_gold_column_has_a_comment():
    """GLD-13: tablas, ops.gold_runs y vistas declaran COMMENT en todas sus columnas."""
    for t, (_, cols, _) in ddl.GOLD.items():
        assert all(m.strip() for _, _, m in cols + ddl.AUDIT), t
    assert all(m.strip() for _, _, m in ddl.GOLD_RUNS)
    assert set(ddl.VIEW_COLUMNS) == set(ddl.VIEW_COMMENTS)
    for v, cols in ddl.VIEW_COLUMNS.items():
        names = [c for c, _ in cols]
        assert len(names) == len(set(names)) and all(m.strip() for _, m in cols), v
    for s in ddl.setup_statements("c"):
        head = s.split("\nAS\n")[0] if s.startswith("CREATE OR REPLACE VIEW") else s.split("\n)")[0]
        for line in head.splitlines()[1:]:
            if line.strip().startswith("`"):
                assert " COMMENT '" in line, line


def test_every_gold_object_declares_question_and_consumer():
    """GLD-11: cada tabla y vista dice que pregunta responde y quien la consume (BI | ML | AI)."""
    comments = [ddl.table_comment(t) for t in ddl.TABLES] + [ddl.view_comment(v) for v in ddl.VIEW_COMMENTS]
    for c in comments:
        assert re.search(r"Responde: .+\. Consumidor: (BI|ML|AI)(, (BI|ML|AI))*$", c), c
    assert set(ddl.BUSINESS) == set(ddl.TABLES) | set(ddl.VIEW_COMMENTS) | {"gold_runs"}


def test_comments_sql_in_sync_and_metadata_only():
    text = COMMENTS_FILE.read_text(encoding="utf-8")
    assert text == ddl.render_comments_sql("fmcg_lakehouse")
    stmts = split_statements(text)
    assert stmts == [s.strip() for s in ddl.comment_statements("fmcg_lakehouse")]
    for s in ddl.comment_statements("c"):
        assert s.startswith(("COMMENT ON TABLE c.gold.", "ALTER TABLE c.gold.", "COMMENT ON TABLE c.ops.gold_runs ",
                             "ALTER TABLE c.ops.gold_runs ", "CREATE OR REPLACE VIEW c.gold."))
        code = re.sub(r"'(?:[^']|'')*'", "''", s)
        assert not re.search(r"\b(DROP|DELETE|TRUNCATE|INSERT|MERGE|UPDATE)\b", code)
        if s.startswith("ALTER TABLE"):
            assert re.fullmatch(r"ALTER TABLE \S+ ALTER COLUMN `\w+` COMMENT ''", code), s
