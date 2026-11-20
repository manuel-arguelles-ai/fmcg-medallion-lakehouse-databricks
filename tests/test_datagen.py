import csv
import hashlib
import json
from collections import Counter
from pathlib import Path

from datagen.generator import ANOMALY_DOCS, generate
from datagen.catalog_data import COLUMNS
from conftest import SMALL


def _hash_tree(root: Path):
    return {p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob("*")) if p.is_file()}


def test_deterministic_same_seed(small_gen, tmp_path):
    out, _ = small_gen
    generate(tmp_path, **SMALL)
    assert _hash_tree(out) == _hash_tree(tmp_path)


def test_different_seed_differs(small_gen, tmp_path):
    out, _ = small_gen
    generate(tmp_path, **{**SMALL, "seed": 8})
    assert _hash_tree(out) != _hash_tree(tmp_path)


def test_manifest_matches_files(small_gen):
    out, manifest = small_gen
    on_disk = json.loads((out / "MANIFEST.json").read_text())
    assert on_disk == manifest
    csvs = sorted(p.relative_to(out).as_posix() for p in out.rglob("*.csv"))
    assert csvs == [f["path"] for f in manifest["files"]]
    for f in manifest["files"]:
        p = out / f["path"]
        with p.open(newline="", encoding="utf-8") as fh:
            rows = list(csv.reader(fh))
        assert rows[0] == COLUMNS[f["source"]]
        assert len(rows) - 1 == f["rows"]
        assert p.stat().st_size == f["bytes"]
        assert f"ingest_date={f['ingest_date']}" in f["path"]


def test_totals_are_sum_of_files(small_gen):
    _, manifest = small_gen
    agg = Counter()
    for f in manifest["files"]:
        agg[(f["company"], f["source"])] += f["rows"]
    for comp, srcs in manifest["totals"].items():
        for src, t in srcs.items():
            assert t["rows"] == agg[(comp, src)]


def test_madre_clean_hija_dirty(small_gen):
    _, manifest = small_gen
    for src, t in manifest["totals"]["madre"].items():
        assert t["anomalies"] == {}, src
    expected = {
        "customers": {"dup_rows", "name_whitespace", "name_case_mixed", "city_typo", "city_null"},
        "products": {"dup_rows", "category_lowercase", "name_typo_term", "product_id_non_numeric"},
        "prices": {"negative_price", "non_numeric_price", "date_format_ymd_slash",
                   "date_format_dmy_dash", "date_format_dmy_slash"},
        "orders": {"dup_rows", "customer_id_invalid", "order_qty_null", "date_format_ymd_slash",
                   "date_format_dmy_dash", "date_format_dmy_slash", "date_format_weekday_long"},
    }
    for src, names in expected.items():
        got = set(manifest["totals"]["hija"][src]["anomalies"])
        assert names <= got, (src, names - got)
        assert got <= set(ANOMALY_DOCS)


def _read(out, comp, src):
    for p in sorted((out / comp / src).rglob("*.csv")):
        with p.open(newline="", encoding="utf-8") as fh:
            yield from csv.DictReader(fh)


def test_manifest_counts_match_content_hija_orders(small_gen):
    out, manifest = small_gen
    an = manifest["totals"]["hija"]["orders"]["anomalies"]
    rows = list(_read(out, "hija", "orders"))
    assert sum(r["order_qty"] == "" for r in rows) == an["order_qty_null"]
    assert sum(not r["customer_id"].isdigit() for r in rows) == an["customer_id_invalid"]
    assert sum("," in r["order_date"] for r in rows) == an["date_format_weekday_long"]
    assert sum(r["order_date"][4:5] == "/" for r in rows) == an["date_format_ymd_slash"]


def test_manifest_counts_match_content_hija_prices_products(small_gen):
    out, manifest = small_gen
    pr = list(_read(out, "hija", "prices"))
    an = manifest["totals"]["hija"]["prices"]["anomalies"]
    assert sum(r["unit_price"].startswith("-") for r in pr) == an["negative_price"]
    assert sum(not r["unit_price"].lstrip("-").replace(".", "").isdigit() for r in pr) == an["non_numeric_price"]
    pd_ = list(_read(out, "hija", "products"))
    an = manifest["totals"]["hija"]["products"]["anomalies"]
    assert sum(not r["product_id"].isdigit() for r in pd_) == an["product_id_non_numeric"]
    assert sum("Energisante" in r["product_name"] for r in pd_) == an["name_typo_term"]


def test_madre_content_is_clean(small_gen):
    out, _ = small_gen
    for r in _read(out, "madre", "orders"):
        assert r["customer_id"].isdigit() and r["order_qty"].isdigit()
        assert len(r["order_date"]) == 10 and r["order_date"][4] == "-"
    for r in _read(out, "madre", "customers"):
        assert r["customer_name"] == r["customer_name"].strip() and r["city"]


def test_orders_one_file_per_day_and_totals_near_target(small_gen):
    _, manifest = small_gen
    files = [f for f in manifest["files"] if f["source"] == "orders" and f["company"] == "madre"]
    assert len(files) == 59  # ene+feb 2025
    total = manifest["totals"]["madre"]["orders"]["rows"]
    assert abs(total - 8000 * 0.7) / (8000 * 0.7) < 0.15
