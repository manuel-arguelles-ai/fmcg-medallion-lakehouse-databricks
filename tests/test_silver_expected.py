"""Conteos esperados (silver.expected) contra el manifest del dataset pequeno."""
from silver.expected import expected


def test_expected_counts_are_consistent_with_manifest(small_gen):
    out, m = small_gen
    e = expected(out)
    t = m["totals"]
    for entity in ("customers", "products", "prices", "orders"):
        assert e[(entity, "madre")]["silver"] == e[(entity, "madre")]["bronze"] - e[(entity, "madre")]["rules"].get(
            {"customers": "CUS_08", "products": "PRD_07", "prices": "PRC_06", "orders": "ORD_11"}[entity], 0)
        assert e[(entity, "madre")]["quarantine_rules"] == {}
        assert e[(entity, "hija")]["bronze"] == t["hija"][entity]["rows"]
    h = e[("orders", "hija")]
    assert h["quarantine_rules"].get("ORD_03", 0) <= t["hija"]["orders"]["anomalies"]["order_qty_null"]
    assert h["rules"]["ORD_11"] == t["hija"]["orders"]["anomalies"]["dup_rows"]
    assert e[("prices", "hija")]["quarantine_rules"] == {"PRC_05": t["hija"]["prices"]["anomalies"]["non_numeric_price"]}
