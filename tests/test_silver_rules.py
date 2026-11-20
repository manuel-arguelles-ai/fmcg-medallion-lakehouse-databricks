"""Reglas puras de Silver: casos borde y deteccion de CADA anomalia sembrada por datagen."""
import csv
import datetime as dt
from collections import Counter
from decimal import Decimal
from pathlib import Path

import pytest

from datagen import catalog_data as cd
from silver import reference_data as ref
from silver import rules as R

CITY = ref.CITY_MAPPING
CAT = ref.CATEGORY_DIVISION
TYPOS = [(w, c) for _, _, w, c in ref.TYPO_FIXES]


# ---------------------------------------------------------------- utilidades
def read_rows(out: Path, company: str, source: str) -> list[dict]:
    rows = []
    for f in sorted((out / company / source).glob("ingest_date=*/*.csv")):
        with f.open(newline="", encoding="utf-8") as fh:
            for i, r in enumerate(csv.DictReader(fh)):
                r["_source_file"], r["_ingest_ts"] = f.name, f.parent.name
                rows.append(r)
    return rows


def applied_counts(results) -> Counter:
    c = Counter()
    for r in results:
        c.update(r.applied)
        c.update(rid for rid, _ in r.rejects)
    return c


# ---------------------------------------------------------------- fechas
@pytest.mark.parametrize("raw,key,expected", [
    ("2025-07-01", "ymd_dash", dt.date(2025, 7, 1)),
    ("2025/07/01", "ymd_slash", dt.date(2025, 7, 1)),
    ("01-07-2025", "dmy_dash", dt.date(2025, 7, 1)),
    ("01/07/2025", "dmy_slash", dt.date(2025, 7, 1)),
    ("Tuesday, July 01, 2025", "weekday_long", dt.date(2025, 7, 1)),
    ("Sunday, December 7, 2025", "weekday_long", dt.date(2025, 12, 7)),
    ("  15/03/2024 ", "dmy_slash", dt.date(2024, 3, 15)),   # dd/MM, nunca MM/dd
])
def test_parse_date_four_formats_plus_iso(raw, key, expected):
    assert R.parse_date(raw) == (expected, key)


@pytest.mark.parametrize("raw", [None, "", "N/A", "31/02/2025", "2025-13-01", "Tuesday, Julio 01, 2025", "7/1/25"])
def test_parse_date_unparseable(raw):
    assert R.parse_date(raw) == (None, None)


def test_every_datagen_date_format_parses():
    from datagen.generator import fmt_date
    d = dt.date(2025, 7, 1)
    for key in ("ymd_dash", "ymd_slash", "dmy_dash", "dmy_slash", "weekday_long"):
        assert R.parse_date(fmt_date(d, key)) == (d, key)


# ---------------------------------------------------------------- numericos e ids
def test_price_cases():
    assert R.parse_price("14.39") == (Decimal("14.39"), "ok")
    assert R.parse_price("-14.39") == (Decimal("14.39"), "negative")
    assert R.parse_price("2.005") == (Decimal("2.01"), "ok")
    for bad in ("N/A", "abc", "--", "s/p", "", None, "1,5"):
        assert R.parse_price(bad) == (None, "invalid")


def test_qty_cases():
    assert R.parse_qty("3") == (3, "ok")
    assert R.parse_qty("") == (None, "null") and R.parse_qty(None) == (None, "null")
    assert R.parse_qty("0")[1] == "invalid" and R.parse_qty("-2")[1] == "invalid" and R.parse_qty("2.5")[1] == "invalid"


def test_ids():
    assert R.normalize_id("500123") == "500123"
    for bad in ("N/A", "UNKNOWN", "#REF!", "sin_dato", "", None):
        assert R.normalize_id(bad) is None
    assert R.normalize_product_id("PX-2001") == ("2001", True)
    assert R.normalize_product_id("2001") == ("2001", False)
    assert R.normalize_product_id("abc") == (None, False)
    assert R.normalize_product_id("PX-") == (None, False)


def test_text_helpers():
    assert R.collapse_ws("  a   b  ") == "a b" and R.collapse_ws("   ") is None
    assert R.initcap("SPORT BAR inti 3") == "Sport Bar Inti 3"
    assert R.split_variant("Isotonica Gol (Limon 500ml)") == ("Isotonica Gol", "Limon 500ml")
    assert R.split_variant("Sin variante") == ("Sin variante", None)
    assert R.apply_typo_fixes("Bebida Energisante Rayo", TYPOS) == "Bebida Energizante Rayo"
    assert R.product_code("x") == "2d711642b726b04401627ca9fbac32f5c8530fb1903cc4db02258717921a4881"


def test_rule_catalog_is_consistent():
    ids = [r.rule_id for r in R.RULES]
    assert len(ids) == len(set(ids))
    assert {r.action for r in R.RULES} <= {R.CORREGIR, R.MARCAR, R.CUARENTENA}
    assert all((r.severity == "error") == (r.action == R.CUARENTENA) for r in R.RULES)
    assert {r.entity for r in R.RULES} == {"customers", "products", "prices", "orders"}


# ---------------------------------------------------------------- reglas por fila (casos borde)
def test_customer_edge_cases():
    r = R.clean_customer({"customer_id": "N/A", "customer_name": "x", "city": "Lima"}, CITY)
    assert r.clean is None and r.rejects[0][0] == "CUS_01"
    r = R.clean_customer({"customer_id": "5", "customer_name": "  ", "city": "Lima"}, CITY)
    assert r.clean is None and r.rejects[0][0] == "CUS_02"
    r = R.clean_customer({"customer_id": "5", "customer_name": "  GOL  BAR inti 5 ", "city": "Cuzco"}, CITY)
    assert r.clean["customer_name"] == "Gol Bar Inti 5" and r.clean["city"] == "Cusco"
    assert {"CUS_03", "CUS_04", "CUS_05"} <= set(r.applied)
    r = R.clean_customer({"customer_id": "5", "customer_name": "A", "city": None}, CITY)
    assert r.clean["city"] == "Unknown" and r.clean["city_was_null"] and r.applied == ["CUS_06"]
    r = R.clean_customer({"customer_id": "5", "customer_name": "A", "city": "gotham"}, CITY)
    assert r.clean["city"] == "Gotham" and r.clean["city_is_unmapped"]


def test_product_edge_cases():
    r = R.clean_product({"product_id": "PX-2001", "product_name": "Bebida Energisante Rayo (Zero 355ml)",
                         "category": "bebidas energizantes"}, CAT, TYPOS)
    c = r.clean
    assert c["product_id"] == "2001" and c["product_id_was_prefixed"]
    assert c["product_name"] == "Bebida Energizante Rayo (Zero 355ml)" and c["variant"] == "Zero 355ml"
    assert c["category"] == "Bebidas Energizantes" and c["division"] == "Bebidas"
    assert {"PRD_02", "PRD_04", "PRD_05"} == set(r.applied)
    assert R.clean_product({"product_id": "abc", "product_name": "x", "category": "Galletas"}, CAT, TYPOS).clean is None
    r = R.clean_product({"product_id": "1", "product_name": "x", "category": "Rara"}, CAT, TYPOS)
    assert r.clean["division"] == "Unknown" and "PRD_06" in r.applied


def test_price_edge_cases():
    base = {"product_id": "2001", "price_month": "01/03/2024", "unit_price": "-2.50"}
    r = R.clean_price(base)
    assert r.clean["unit_price"] == Decimal("2.50") and r.clean["price_was_negative"]
    assert r.clean["price_month"] == dt.date(2024, 3, 1) and {"PRC_03", "PRC_04"} == set(r.applied)
    r = R.clean_price({**base, "unit_price": "abc"})
    assert r.clean is None and r.rejects == [("PRC_05", "unit_price no numerico: 'abc'")]
    assert R.clean_price({**base, "price_month": "mañana"}).rejects[0][0] == "PRC_02"
    assert R.clean_price({**base, "product_id": "?"}).rejects[0][0] == "PRC_01"
    assert R.clean_price(base, known_products={"1"}).clean["product_unresolved"]


def test_price_invalid_as_marked_zero(monkeypatch):
    monkeypatch.setattr(R, "PRICE_INVALID_ACTION", R.MARCAR)
    r = R.clean_price({"product_id": "1", "price_month": "2024-03-01", "unit_price": "N/A"})
    assert r.clean["unit_price"] == Decimal("0.00") and r.clean["price_was_invalid"] and not r.rejects


PRICES = {("2001", dt.date(2025, 7, 1)): Decimal("2.50")}


def order(**kw):
    base = {"order_id": "9", "line_no": "1", "order_date": "Tuesday, July 01, 2025",
            "customer_id": "500001", "product_id": "2001", "order_qty": "4"}
    return {**base, **kw}


def clean_order(**kw):
    return R.clean_order(order(**kw), {"500001"}, {"2001"}, PRICES)


def test_order_happy_path_and_derivations():
    r = clean_order()
    c = r.clean
    assert c["order_date"] == dt.date(2025, 7, 1) and c["order_month_start"] == dt.date(2025, 7, 1)
    assert (c["order_year"], c["order_quarter"], c["order_month"], c["is_weekend"]) == (2025, 3, 7, False)
    assert c["line_amount"] == Decimal("10.00") and c["currency"] == R.CURRENCY
    assert r.applied == ["ORD_06"]
    assert clean_order(order_date="2025-07-05").clean["is_weekend"] is True   # sabado


@pytest.mark.parametrize("kw,rule", [
    ({"order_qty": ""}, "ORD_03"), ({"order_qty": None}, "ORD_03"), ({"order_qty": "-1"}, "ORD_04"),
    ({"order_qty": "x"}, "ORD_04"), ({"order_date": "N/A"}, "ORD_02"), ({"order_date": None}, "ORD_02"),
    ({"order_id": ""}, "ORD_01"), ({"line_no": "a"}, "ORD_01"), ({"product_id": "abc"}, "ORD_05"),
    ({"product_id": None}, "ORD_05"),
])
def test_order_quarantine_rules(kw, rule):
    r = clean_order(**kw)
    assert r.clean is None and rule in [x for x, _ in r.rejects]


@pytest.mark.parametrize("bad", ["N/A", "UNKNOWN", "#REF!", "sin_dato", "", None])
def test_order_invalid_customer_goes_to_unknown_member(bad):
    r = clean_order(customer_id=bad)
    assert r.clean["customer_id"] == "-1" and r.clean["customer_id_was_invalid"] and "ORD_07" in r.applied
    assert r.clean["line_amount"] == Decimal("10.00")   # la venta sigue valiendo


def test_order_unresolved_customer_product_and_price():
    r = clean_order(customer_id="999")
    assert r.clean["customer_id"] == "-1" and r.clean["customer_unresolved"] and "ORD_08" in r.applied
    r = clean_order(product_id="PX-3000")
    assert r.clean["product_id"] == "-1" and r.clean["product_unresolved"] and r.clean["line_amount"] is None
    r = clean_order(order_date="2025-08-01")
    assert r.clean["price_missing"] and r.clean["line_amount"] is None and "ORD_10" in r.applied
    assert R.clean_order(order(), prices=None).clean["line_amount"] is None   # sin resolucion


def test_dedup_latest_ingest_wins_and_is_deterministic():
    rows = [
        {"k": 1, "v": "viejo", "_ingest_ts": "2025-01-01", "_source_file": "a"},
        {"k": 1, "v": "nuevo", "_ingest_ts": "2025-02-01", "_source_file": "a"},
        {"k": 1, "v": "empate", "_ingest_ts": "2025-02-01", "_source_file": "b"},
        {"k": 2, "v": "solo", "_ingest_ts": "2025-01-01", "_source_file": "a"},
    ]
    win, removed = R.dedup_latest(rows, lambda r: r["k"])
    assert removed == 2 and {r["k"]: r["v"] for r in win} == {1: "empate", 2: "solo"}
    win2, _ = R.dedup_latest(list(reversed(rows)), lambda r: r["k"])
    assert {r["k"]: r["v"] for r in win2} == {1: "empate", 2: "solo"}


# ---------------------------------------------------------------- anomalias sembradas (datagen) vs reglas
def manifest_counts(manifest, company, source):
    return manifest["totals"][company][source]["anomalies"]


def test_hija_customers_seeded_anomalies_detected(small_gen):
    out, m = small_gen
    rows = read_rows(out, "hija", "customers")
    res = [R.clean_customer(r, CITY) for r in rows]
    a, exp = applied_counts(res), manifest_counts(m, "hija", "customers")
    assert a["CUS_03"] == exp["name_whitespace"]
    assert a["CUS_04"] == exp["name_case_mixed"]
    assert a["CUS_05"] == exp["city_typo"]
    assert a["CUS_06"] == exp["city_null"]
    assert a["CUS_01"] == a["CUS_02"] == a["CUS_07"] == 0
    assert all(r.clean for r in res)
    _, removed = R.dedup_latest(
        [dict(x, _ingest_ts=x["_ingest_ts"] + x["_source_file"]) for x in rows],
        lambda r: (r["_source_file"], r["customer_id"]))
    assert removed == exp["dup_rows"]   # copias dentro del mismo snapshot


def test_hija_products_seeded_anomalies_detected(small_gen):
    out, m = small_gen
    rows = read_rows(out, "hija", "products")
    res = [R.clean_product(r, CAT, TYPOS) for r in rows]
    a, exp = applied_counts(res), manifest_counts(m, "hija", "products")
    assert a["PRD_02"] == exp["product_id_non_numeric"]
    assert a["PRD_04"] == exp["name_typo_term"]
    assert a["PRD_05"] == exp["category_lowercase"]
    assert a["PRD_01"] == a["PRD_03"] == a["PRD_06"] == 0
    assert all(r.clean for r in res)
    # los ids con prefijo se recuperan: coinciden con los ids numericos de orders/prices
    assert all(r.clean["product_id"].isdigit() for r in res)
    _, removed = R.dedup_latest(
        [dict(x, _ingest_ts=x["_ingest_ts"] + x["_source_file"]) for x in rows],
        lambda r: (r["_source_file"], r["product_id"]))
    assert removed == exp["dup_rows"]


def test_hija_prices_seeded_anomalies_detected(small_gen):
    out, m = small_gen
    res = [R.clean_price(r) for r in read_rows(out, "hija", "prices")]
    a, exp = applied_counts(res), manifest_counts(m, "hija", "prices")
    assert a["PRC_04"] == exp["negative_price"]
    assert a["PRC_05"] == exp["non_numeric_price"]
    assert a["PRC_03"] == exp["date_format_dmy_dash"] + exp["date_format_dmy_slash"] + exp["date_format_ymd_slash"]
    assert a["PRC_01"] == a["PRC_02"] == 0
    assert sum(1 for r in res if r.clean) == len(res) - exp["non_numeric_price"]
    assert all(r.clean["unit_price"] > 0 for r in res if r.clean)


def test_hija_orders_seeded_anomalies_detected(small_gen):
    out, m = small_gen
    rows = read_rows(out, "hija", "orders")
    res = [R.clean_order(r) for r in rows]
    a, exp = applied_counts(res), manifest_counts(m, "hija", "orders")
    assert a["ORD_03"] == exp["order_qty_null"]
    assert a["ORD_07"] == exp["customer_id_invalid"]
    assert a["ORD_06"] == sum(exp[f"date_format_{k}"] for k in ("ymd_slash", "dmy_dash", "dmy_slash", "weekday_long"))
    assert a["ORD_01"] == a["ORD_02"] == a["ORD_04"] == a["ORD_05"] == 0
    assert sum(1 for r in res if r.clean is None) == exp["order_qty_null"]
    # los 4 formatos (incl. dia de semana) se parsean a fechas dentro del rango generado
    assert all(dt.date(2025, 1, 1) <= r.clean["order_date"] <= dt.date(2025, 2, 28) for r in res if r.clean)
    # duplicados (mismo archivo o archivo del dia siguiente): clave (order_id, line_no) con ingest mas reciente
    ranked = [dict(x, _ingest_ts=x["_ingest_ts"] + x["_source_file"]) for x in rows]
    _, removed = R.dedup_latest(ranked, lambda r: (r["order_id"], r["line_no"]))
    assert removed == exp["dup_rows"]


def test_dedup_orders_picks_latest_file_for_cross_file_duplicates(small_gen):
    out, _ = small_gen
    rows = read_rows(out, "hija", "orders")
    seen = {}
    for r in rows:
        seen.setdefault((r["order_id"], r["line_no"]), []).append(r)
    cross = [v for v in seen.values() if len(v) > 1 and len({x["_source_file"] for x in v}) > 1]
    assert cross, "el dataset pequeno debe traer duplicados entre archivos"
    for v in cross:
        win, _ = R.dedup_latest([dict(x, _ingest_ts=x["_ingest_ts"]) for x in v], lambda r: 1)
        assert win[0]["_source_file"] == max(x["_source_file"] for x in v)


@pytest.mark.parametrize("source,fn", [
    ("customers", lambda r: R.clean_customer(r, CITY)),
    ("products", lambda r: R.clean_product(r, CAT, TYPOS)),
    ("prices", R.clean_price),
    ("orders", R.clean_order),
])
def test_clean_madre_triggers_no_rule(small_gen, source, fn):
    """La madre limpia no dispara NINGUNA regla (ni cuarentena ni correccion ni marca)."""
    out, _ = small_gen
    rows = read_rows(out, "madre", source)
    assert rows
    bad = [(r, x.applied, x.rejects) for r in rows if (x := fn(r)).applied or x.rejects or x.clean is None]
    assert not bad, bad[:3]
