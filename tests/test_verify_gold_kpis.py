"""Parte pura de scripts/verify_gold_kpis.py (sin red ni Databricks)."""
import sys
from decimal import Decimal as D
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import verify_gold_kpis as V  # noqa: E402
from silver.expected import expected  # noqa: E402


@pytest.fixture(scope="module")
def kpis(small_gen):
    out, _ = small_gen
    silver = V.silver_rows(out)
    return out, silver, V.compute_kpis(silver)


def test_silver_rows_match_expected_counts(kpis):
    out, silver, _ = kpis
    e = expected(out)
    for c in V.COMPANIES:
        assert len(silver[c]["orders"]) == e[("orders", c)]["silver"]
        assert len(silver[c]["products"]) == e[("products", c)]["silver"]


def test_sales_totals_are_consistent(kpis):
    _, silver, k = kpis
    assert sum(v["order_lines"] for v in k["sales"].values()) == sum(len(s["orders"]) for s in silver.values())
    rev = sum((o["line_amount"] or 0 for s in silver.values() for o in s["orders"]), D(0))
    assert sum(v["revenue"] for v in k["sales"].values()) == rev
    assert sum(v["revenue"] for v in k["mix"].values()) == rev
    assert sum(v["revenue"] for (p, *_), v in k["top"].items() if p == "Total") <= rev


def test_mix_shares_sum_to_one_per_company_month(kpis):
    _, _, k = kpis
    sums = {}
    for (c, ym, _), v in k["mix"].items():
        if v["revenue_share"] is not None:
            sums[(c, ym)] = sums.get((c, ym), 0) + v["revenue_share"]
    assert sums and all(abs(s - 1) < D("1e-9") for s in sums.values())


def test_comparison_shares_and_mom(kpis):
    _, _, k = kpis
    by_month = {}
    for (c, ym), v in k["comp"].items():
        by_month.setdefault(ym, []).append(v["revenue_share_month"])
    assert all(abs(sum(x) - 1) < D("1e-9") for x in by_month.values() if len(x) == 2)
    first = {c: min(ym for cc, ym in k["comp"] if cc == c) for c in V.COMPANIES}
    for c, ym in first.items():
        assert k["comp"][(c, ym)]["revenue_mom_pct"] is None


def test_top_n_rank_semantics_with_ties():
    prod = {("Total", "madre", n): {"revenue": D(r), "qty": 1}
            for n, r in [("a", 50), ("b", 40), ("c", 40), ("d", 30), ("e", 0)]}
    t = V.top_n(prod, 3)
    assert {k[2]: v["rank"] for k, v in t.items()} == {"a": 1, "b": 2, "c": 2}
    assert ("Total", "madre", "e") not in t


def test_top_n_cuts_at_n():
    prod = {("Total", "m", str(i)): {"revenue": D(100 - i), "qty": 1} for i in range(20)}
    t = V.top_n(prod, 10)
    assert len(t) == 10 and max(v["rank"] for v in t.values()) == 10


def test_close_tolerance_and_nulls():
    assert V.close("10.00", "10.01") and not V.close("10.00", "10.02")
    assert V.close(None, None) and not V.close(None, 0)


def test_compare_detects_differences():
    e = {("madre", "2025-01"): {"revenue": D("100.00"), "total_qty": 5}}
    assert V.compare("s", e, e, ["revenue", "total_qty"]) == []
    bad = {("madre", "2025-01"): {"revenue": D("100.50"), "total_qty": 5}}
    assert len(V.compare("s", e, bad, ["revenue", "total_qty"])) == 1
    assert "falta" in V.compare("s", e, {}, ["revenue"])[0]
    assert "sobra" in V.compare("s", {}, e, ["revenue"])[0]
    qty = {("madre", "2025-01"): {"revenue": D("100.00"), "total_qty": 6}}
    assert len(V.compare("s", e, qty, ["revenue", "total_qty"])) == 1


def test_compare_null_revenue_is_zero_and_ratio_tolerance():
    e = {"k": {"revenue": D("0"), "revenue_share": D("0.5")}}
    a = {"k": {"revenue": None, "revenue_share": D("0.5000005")}}
    assert V.compare("m", e, a, ["revenue", "revenue_share"]) == []
    a["k"]["revenue_share"] = D("0.51")
    assert len(V.compare("m", e, a, ["revenue", "revenue_share"])) == 1


def test_query_rejects_non_select():
    for sql in ("DROP TABLE x", "INSERT OVERWRITE t SELECT 1", "  delete from t"):
        with pytest.raises(ValueError):
            V.query(sql, "wh")
