import re

from bronze import ingest, schemas
from bronze.registry import get_spec, load_registry, ops_ddl, seed_sql
from datagen.catalog_data import COLUMNS


def test_registry_expands_8_entries():
    specs = load_registry()
    assert len(specs) == 8
    assert {(s.company, s.source) for s in specs} == {
        (c, s) for c in ("madre", "hija") for s in COLUMNS}
    assert {s.source_system for s in specs} == {"madre_erp", "hija_export"}


def test_registry_columns_match_datagen():
    for s in load_registry():
        assert list(s.columns) == COLUMNS[s.source]


def test_load_types_and_frequency():
    s = get_spec("hija", "orders")
    assert (s.load_type, s.frequency) == ("incremental", "daily")
    assert get_spec("madre", "prices").frequency == "monthly"
    assert all(x.landing_retention_days == 90 for x in load_registry())


def test_file_pattern_matches_datagen_names():
    s = get_spec("madre", "orders")
    assert re.match(s.file_pattern, "orders_madre_20250101.csv")
    assert not re.match(s.file_pattern, "orders_hija_20250101.csv")
    assert s.landing_path("c") == "/Volumes/c/bronze/landing/madre/orders"


def test_schema_all_string_and_declared():
    for s in load_registry():
        ddl = schemas.ddl_schema_string(s)
        types = re.findall(r"`\w+` (\w+)", ddl)
        assert types == ["STRING"] * len(s.columns)


def test_table_columns_order_and_metadata():
    s = get_spec("hija", "customers")
    cols = schemas.table_columns(s)
    assert cols[:3] == ["customer_id", "customer_name", "city"]
    assert cols[3] == "_rescued_data"
    required = {"_company", "_source_system", "_source", "_source_file", "_file_size",
                "_file_modification_time", "_ingest_ts", "_ingest_date", "_batch_id", "_load_type"}
    assert required <= set(cols)
    assert len(cols) == len(set(cols))


def test_table_ddl_properties_and_staging():
    s = get_spec("madre", "orders")
    ddl = schemas.table_ddl(s, "cat")
    assert "cat.bronze.madre_orders" in ddl
    assert "'delta.enableChangeDataFeed' = 'true'" in ddl
    assert "'delta.logRetentionDuration' = '30 days'" in ddl
    assert "'delta.deletedFileRetentionDuration' = '7 days'" in ddl
    assert "`order_qty` STRING" in ddl and "`_ingest_date` DATE" in ddl
    st = schemas.table_ddl(s, "cat", staging=True)
    assert "cat.bronze.staging_madre_orders" in st and "TBLPROPERTIES" not in st


def test_seed_sql_and_ops_ddl():
    sql = seed_sql("cat")
    assert sql.count("'madre_erp'") == 4 and sql.count("'hija_export'") == 4
    assert "cat.ops.source_registry" in sql and "/Volumes/cat/bronze/landing/hija/orders" in sql
    ddls = "\n".join(ops_ddl("cat"))
    for t in ("source_registry", "landing_rejects", "bronze_runs"):
        assert f"cat.ops.{t}" in ddls


def test_ingest_helpers():
    a = ingest.parse_args(["--company", "hija", "--source", "orders", "--catalog", "x"])
    assert (a.company, a.source, a.catalog, a.generation) == ("hija", "orders", "x", "1")
    s = get_spec("hija", "orders")
    p = ingest.paths(s, "x")
    assert p["bronze"] == "x.bronze.hija_orders" and p["staging"] == "x.bronze.staging_hija_orders"
    assert p["checkpoint"].startswith("/Volumes/x/ops/checkpoints/")
    import datetime as dt
    b = ingest.new_batch_id(s, dt.datetime(2025, 1, 2, 3, 4, 5))
    assert b.startswith("hija_orders_20250102T030405_")
