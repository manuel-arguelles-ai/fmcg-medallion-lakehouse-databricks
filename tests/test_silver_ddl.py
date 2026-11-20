"""DDL de Silver: coherencia de columnas, sincronia con el SQL versionado y con Bronze."""
import re
from pathlib import Path

from bronze.registry import load_registry
from silver import ddl, rules as R

SQL_FILE = Path(__file__).resolve().parents[1] / "src" / "silver" / "sql" / "01_silver_ddl.sql"


def test_sql_file_in_sync_with_generator():
    assert SQL_FILE.read_text(encoding="utf-8") == ddl.render_sql("fmcg_lakehouse")
    assert SQL_FILE.read_text(encoding="utf-8").startswith("-- NO EJECUTADO")


def test_raw_columns_match_bronze_registry():
    for s in load_registry():
        assert list(s.columns) == ddl.RAW_COLUMNS[s.source]


def test_columns_unique_and_keys_present():
    for e, (key, cols) in ddl.SILVER.items():
        names = [c for c, _, _ in cols]
        assert len(names) == len(set(names)) and set(key) <= set(names)
        assert len(ddl.columns(e)) == len(set(ddl.columns(e)))


def test_every_rule_flag_documented_in_a_column_comment():
    """Cada columna *_was_*/..._unresolved/price_missing cita su regla en el comentario."""
    for e, (_, cols) in ddl.SILVER.items():
        for name, typ, comment in cols:
            if typ == "BOOLEAN" and name != "is_unknown_member" and name != "is_weekend":
                rid = re.search(r"[A-Z]{3}_\d\d", comment)
                assert rid and R.get_rule(rid.group()).entity == e, (e, name)


def test_statements_are_idempotent_forms():
    stmts = ddl.setup_statements("c")
    assert all(s.startswith(("CREATE TABLE IF NOT EXISTS", "MERGE INTO")) for s in stmts)
    assert sum(s.startswith("CREATE") for s in stmts) == 4 + 4 + 1 + 4
    assert all("quarantine." in s or "silver." in s or "ops." in s for s in stmts)


def test_unknown_member_and_ref_seed_do_not_overwrite():
    for e in ("customers", "products"):
        sql = ddl.unknown_member_sql("c", e)
        assert "WHEN NOT MATCHED" in sql and "WHEN MATCHED" not in sql and "'-1'" in sql
    for t in ddl.REF_DDL:
        assert "WHEN MATCHED" not in ddl.ref_seed_sql("c", t)
