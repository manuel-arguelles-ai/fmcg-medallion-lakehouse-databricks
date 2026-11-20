"""Logica pura de Gold: calendario, vigencias de precio, features ML con lags de calendario, KPIs."""
import datetime as dt
from decimal import Decimal as D

import pytest

from gold import ddl, model as M

d = dt.date


def test_date_sk_and_unknown():
    assert M.date_sk(d(2025, 7, 1)) == 20250701
    assert M.date_sk(None) == -1


def test_calendar_bounds_full_years():
    assert M.calendar_bounds(d(2024, 3, 5), d(2025, 11, 2)) == (d(2024, 1, 1), d(2025, 12, 31))
    with pytest.raises(ValueError):
        M.calendar_bounds(d(2025, 1, 2), d(2025, 1, 1))


def test_date_rows_cover_range_plus_unknown_and_match_ddl():
    rows = M.date_rows(d(2024, 1, 1), d(2025, 12, 31))
    assert len(rows) == 366 + 365 + 1                          # 2024 bisiesto + miembro -1
    assert [r["date_sk"] for r in rows[:-1]] == sorted({r["date_sk"] for r in rows[:-1]})
    assert rows[-1]["date_sk"] == -1 and rows[-1]["is_unknown_member"] and rows[-1]["date"] is None
    cols = [c for c, _, _ in ddl.GOLD["dim_date"][1]]
    assert all(list(r) == cols for r in rows)                  # mismas columnas y orden que el DDL


def test_date_row_attributes():
    r = M.date_row(d(2025, 2, 28))   # viernes, fin de mes
    assert (r["year"], r["quarter"], r["quarter_label"], r["month"], r["month_name"]) == (2025, 1, "Q1", 2, "febrero")
    assert (r["day_of_week"], r["day_name"], r["is_weekend"], r["is_month_end"]) == (5, "viernes", False, True)
    assert (r["month_start"], r["month_end"], r["year_month"]) == (d(2025, 2, 1), d(2025, 2, 28), "2025-02")
    s = M.date_row(d(2025, 3, 2))    # domingo
    assert s["is_weekend"] and s["day_of_week"] == 7 and s["day_name"] == "domingo"
    assert M.date_row(d(2024, 12, 30))["week_of_year"] == 1    # semana ISO cruza el anio
    assert M.date_row(d(2024, 12, 31))["day_of_year"] == 366


def test_month_index_consecutive_across_year():
    assert M.month_index(d(2025, 1, 1)) - M.month_index(d(2024, 12, 1)) == 1


def test_price_validity_is_the_month_only():
    assert M.price_validity(d(2024, 2, 1)) == (d(2024, 2, 1), d(2024, 2, 29))
    assert M.price_validity(d(2025, 12, 15)) == (d(2025, 12, 1), d(2025, 12, 31))


def test_previous_month_prices_respect_calendar_gaps():
    rows = [
        {"company": "hija", "product_id": "7", "price_month": d(2025, 1, 1), "unit_price": D("10.00")},
        {"company": "hija", "product_id": "7", "price_month": d(2025, 2, 1), "unit_price": D("11.00")},
        {"company": "hija", "product_id": "7", "price_month": d(2025, 4, 1), "unit_price": D("12.00")},  # marzo falta
        {"company": "madre", "product_id": "7", "price_month": d(2025, 3, 1), "unit_price": D("99.00")},
    ]
    out = {(r["company"], r["price_month"]): r for r in M.previous_month_prices(rows)}
    assert out[("hija", d(2025, 1, 1))]["prev_unit_price"] is None
    assert out[("hija", d(2025, 2, 1))]["price_change_pct"] == D("0.1000")
    assert out[("hija", d(2025, 4, 1))]["prev_unit_price"] is None      # no toma enero/febrero ni la madre
    assert out[("madre", d(2025, 3, 1))]["prev_unit_price"] is None


def test_pct_change_edge_cases():
    assert M.pct_change(None, D("1")) is None and M.pct_change(D("0"), D("1")) is None
    assert M.pct_change(D("3"), D("1")) == D("-0.6667")


def _agg(month, rev, qty=1, cust=1, prod=1):
    return {"company": "madre", "customer_sk": cust, "product_sk": prod, "currency": "XXX",
            "month_start": month, "revenue": rev, "total_qty": qty}


def test_monthly_features_calendar_lags_gaps_and_target():
    rows = [_agg(d(2025, 1, 1), D("10"), 1), _agg(d(2025, 2, 1), D("20"), 2),
            _agg(d(2025, 5, 1), D("50"), 5),                   # hueco en marzo y abril
            _agg(d(2025, 6, 1), D("60"), 6, cust=2)]           # otro grupo
    f = {(r["customer_sk"], r["month_start"]): r for r in M.monthly_features(rows)}
    feb, may = f[(1, d(2025, 2, 1))], f[(1, d(2025, 5, 1))]
    assert (feb["revenue_lag_1"], feb["revenue_lag_2"], feb["qty_lag_1"]) == (D("10"), D("0"), 1)
    assert feb["revenue_avg_3m"] == D("3.33")
    assert (may["revenue_lag_1"], may["revenue_lag_2"], may["revenue_lag_3"]) == (D("0"), D("0"), D("20"))
    assert may["months_since_prev_purchase"] == 3 and may["purchase_month_seq"] == 3
    assert f[(1, d(2025, 1, 1))]["months_since_prev_purchase"] is None
    assert feb["target_revenue_next_month"] == D("0")                    # marzo sin compras: se sabe que es 0
    assert f[(1, d(2025, 1, 1))]["target_revenue_next_month"] == D("20")
    assert may["target_revenue_next_month"] == D("0")                    # junio existe en los datos (otro grupo)
    assert f[(2, d(2025, 6, 1))]["target_revenue_next_month"] is None    # julio aun no se conoce
    assert f[(2, d(2025, 6, 1))]["revenue_lag_1"] == D("0")             # no mezcla grupos


def test_monthly_features_null_revenue_counts_as_zero_in_lags():
    f = M.monthly_features([_agg(d(2025, 1, 1), None), _agg(d(2025, 2, 1), D("5"))])
    feb = [r for r in f if r["month_start"] == d(2025, 2, 1)][0]
    assert feb["revenue_lag_1"] == D("0")


def test_feature_columns_match_ddl():
    f = M.monthly_features([_agg(d(2025, 1, 1), D("1"))])[0]
    ddl_cols = {c for c, _, _ in ddl.GOLD["feature_sales_monthly"][1]}
    assert ddl_cols == set(f) | {"order_lines"}      # order_lines pasa tal cual del agregado


def test_top_n_rank_with_ties():
    out = M.top_n({"a": 10, "b": 30, "c": 30, "d": 5}, n=3)
    assert out == [("b", 30, 1), ("c", 30, 1), ("a", 10, 3)]
    assert len(M.top_n({i: i for i in range(20)})) == 10


def test_shares():
    assert M.shares({"madre": 75, "hija": 25}) == {"madre": 0.75, "hija": 0.25}
    assert M.shares({"x": 0}) == {"x": None}
