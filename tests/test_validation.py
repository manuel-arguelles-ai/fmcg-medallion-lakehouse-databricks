from pathlib import Path

from bronze import validation as v
from bronze.registry import get_spec

SPEC = get_spec("madre", "orders")
HDR = "order_id,line_no,order_date,customer_id,product_id,order_qty\n"
GOOD = "/Volumes/c/bronze/landing/madre/orders/ingest_date=2025-01-01/orders_madre_20250101.csv"


def rules(path=GOOD, size=100, head=(HDR, "1,1,2025-01-01,1,1,1\n")):
    return {r.rule_id for r in v.validate_file(path, size, list(head), SPEC)}


def test_good_file_passes():
    assert rules() == set()


def test_dbfs_prefix_is_normalized():
    assert v.validate_file("dbfs:" + GOOD, 10, [HDR, "x\n"], SPEC) == []


def test_empty_file():
    assert v.R_EMPTY in rules(size=0, head=())


def test_extension():
    assert v.R_EXT in rules(GOOD.replace(".csv", ".txt"))


def test_name_pattern():
    assert v.R_NAME in rules(GOOD.replace("orders_madre_20250101", "pedidos"))
    assert v.R_NAME in rules(GOOD.replace("orders_madre", "orders_hija"))


def test_header_mismatch_and_order():
    assert v.R_HEADER in rules(head=("order_id,line_no\n", "1,1\n"))
    assert v.R_HEADER in rules(head=("line_no,order_id,order_date,customer_id,product_id,order_qty\n", "1\n"))
    assert v.R_HEADER in rules(head=())


def test_header_tolerates_bom_and_spaces():
    assert rules(head=("﻿order_id, line_no,order_date,customer_id,product_id,order_qty\n", "1\n")) == set()


def test_no_data_rows():
    assert v.R_NO_ROWS in rules(head=(HDR, ""))
    assert v.R_NO_ROWS in rules(head=(HDR,))


def test_ingest_folder():
    assert v.R_FOLDER in rules(GOOD.replace("ingest_date=2025-01-01", "2025-01-01"))
    assert v.R_FOLDER in rules(GOOD.replace("ingest_date=2025-01-01", "ingest_date=2025-13-45"))
    assert v.R_FOLDER in rules(GOOD.replace("ingest_date=2025-01-01", "ingest_date=20250101"))
    assert v.check_ingest_folder("/a/ingest_date=2025-02-29/f.csv")  # no bisiesto


def test_multiple_rules_reported_together():
    assert rules(GOOD.replace(".csv", ".txt"), size=0, head=()) >= {v.R_EMPTY, v.R_EXT, v.R_NAME}


def test_scan_landing_dir(tmp_path, small_gen):
    """Sobre datos reales del datagen + archivos malos inyectados."""
    import shutil
    out, _ = small_gen
    root = tmp_path / "landing" / "madre" / "orders"
    shutil.copytree(out / "madre" / "orders", root)
    day = root / "ingest_date=2025-01-05"
    (day / "orders_madre_20250105_extra.csv").write_text("a,b\n1,2\n")
    (day / "orders_madre_20250106.csv").write_text("")
    (day / "notes.txt").write_text("hola")
    (root / "sin_carpeta").mkdir()
    (root / "sin_carpeta" / "orders_madre_20250107.csv").write_text(HDR + "1,1,2025-01-07,1,1,1\n")
    ok, rej = v.validate_landing(str(root), SPEC)
    assert len(ok) == 59
    bad = {Path(r.file_path).name for r in rej}
    assert bad == {"orders_madre_20250105_extra.csv", "orders_madre_20250106.csv", "notes.txt",
                   "orders_madre_20250107.csv"}
    # skip: ya conocidos no se vuelven a evaluar
    ok2, rej2 = v.validate_landing(str(root), SPEC, skip=set(ok) | {r.file_path for r in rej})
    assert ok2 == [] and rej2 == []


def test_all_generated_files_pass_validation(small_gen):
    out, manifest = small_gen
    for f in manifest["files"]:
        spec = get_spec(f["company"], f["source"])
        p = str(out / f["path"])
        assert v.validate_file(p, f["bytes"], v.read_head(p), spec) == [], f["path"]
