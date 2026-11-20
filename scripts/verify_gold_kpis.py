"""Verificacion INDEPENDIENTE de los KPI de Gold (criterio de salida de H5, docs/09 §7).

Calcula los KPI en Python puro desde los CSV de data/generated (reglas puras de src/silver para obtener las
filas Silver esperadas, sin Spark ni tablas de Gold) y los compara con las vistas/tablas de Gold consultadas
con la Statement Execution API. SOLO SELECT. Sale con codigo != 0 si algo difiere (tolerancia 0.01).

  DATABRICKS_HOST=https://<workspace> DATABRICKS_AUTH_TYPE=azure-cli \
  .venv/bin/python scripts/verify_gold_kpis.py [--root data/generated] [--warehouse <warehouse-id>]
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "infra")]

from silver import reference_data as ref  # noqa: E402
from silver import rules as R  # noqa: E402
from silver.expected import read_rows  # noqa: E402

TOL = Decimal("0.01")
COMPANIES = ("madre", "hija")
CATALOG = "fmcg_lakehouse"


# ---------------------------------------------------------------- filas Silver esperadas (misma logica que silver.expected)
def silver_rows(root: Path) -> dict:
    """{company: {'products': {pid: clean}, 'orders': [clean, ...]}} aplicando reglas + dedup como Silver."""
    typos = [(w, c) for _, _, w, c in ref.TYPO_FIXES]
    out = {}
    for company in COMPANIES:
        def run(source, fn, key_fn, key_ok):
            raw = read_rows(root, company, source)
            winners, _ = R.dedup_latest([r for r in raw if key_ok(r)], key_fn)
            return [x.clean for x in map(fn, winners) if x.clean]

        cust = run("customers", lambda r: R.clean_customer(r, ref.CITY_MAPPING),
                   lambda r: r["customer_id"], lambda r: R.normalize_id(r["customer_id"]))
        prod = run("products", lambda r: R.clean_product(r, ref.CATEGORY_DIVISION, typos),
                   lambda r: R.normalize_product_id(r["product_id"])[0],
                   lambda r: R.normalize_product_id(r["product_id"])[0])
        known_p, known_c = {p["product_id"] for p in prod}, {c["customer_id"] for c in cust}
        pr_key = lambda r: (R.normalize_product_id(r["product_id"])[0], R.parse_date(r["price_month"])[0] and
                            R.month_start(R.parse_date(r["price_month"])[0]))
        prices = run("prices", lambda r: R.clean_price(r, known_p), pr_key, lambda r: None not in pr_key(r))
        price_map = {(p["product_id"], p["price_month"]): p["unit_price"] for p in prices}
        o_key = lambda r: (R.normalize_id(r["order_id"]), R.normalize_id(r["line_no"]))
        orders = run("orders", lambda r: R.clean_order(r, known_c, known_p, price_map), o_key,
                     lambda r: None not in o_key(r))
        out[company] = {"products": {p["product_id"]: p for p in prod}, "orders": orders}
    return out


# ---------------------------------------------------------------- KPI esperados (puros)
def _ym(d) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def compute_kpis(silver: dict) -> dict:
    """Devuelve los KPI esperados como dicts planos (sin tocar Gold)."""
    monthly = defaultdict(lambda: {"revenue": Decimal(0), "qty": 0, "lines": 0, "orders": set(), "nopr": 0})
    cat = defaultdict(lambda: {"revenue": Decimal(0), "qty": 0})
    prod = defaultdict(lambda: {"revenue": Decimal(0), "qty": 0})
    for company, s in silver.items():
        for o in s["orders"]:
            ym = _ym(o["order_date"])
            m = monthly[(company, ym)]
            amt = o["line_amount"] or Decimal(0)
            m["revenue"] += amt
            m["qty"] += o["order_qty"]
            m["lines"] += 1
            m["orders"].add(o["order_id"])
            m["nopr"] += 1 if o["price_missing"] else 0
            # producto -1 (no resuelto) cae al miembro desconocido: company 'Unknown', categoria 'Unknown'
            p = s["products"].get(o["product_id"])
            pkey = (company, p["product_name"]) if p else ("Unknown", "Unknown")
            category = p["category"] if p else "Unknown"
            for period in (str(o["order_date"].year), "Total"):
                prod[(period, *pkey)]["revenue"] += amt
                prod[(period, *pkey)]["qty"] += o["order_qty"]
            cat[(company, ym, category)]["revenue"] += amt
            cat[(company, ym, category)]["qty"] += o["order_qty"]

    sales = {k: {"revenue": v["revenue"], "total_qty": v["qty"], "order_lines": v["lines"],
                 "orders": len(v["orders"]), "lines_without_price": v["nopr"]} for k, v in monthly.items()}

    month_total = defaultdict(Decimal)
    for (c, ym, _), v in cat.items():
        month_total[(c, ym)] += v["revenue"]
    mix = {k: {"revenue": v["revenue"], "total_qty": v["qty"],
               "revenue_share": (v["revenue"] / month_total[k[:2]]) if month_total[k[:2]] else None}
           for k, v in cat.items()}

    all_month = defaultdict(Decimal)
    for (c, ym), v in sales.items():
        all_month[ym] += v["revenue"]
    comp = {}
    for c in {c for c, _ in sales}:
        prev = None
        for ym in sorted(y for cc, y in sales if cc == c):
            v = sales[(c, ym)]
            comp[(c, ym)] = {
                "revenue": v["revenue"], "total_qty": v["total_qty"], "orders": v["orders"],
                "revenue_share_month": v["revenue"] / all_month[ym] if all_month[ym] else None,
                # igual que la vista: LAG por filas existentes (no por calendario)
                "revenue_mom_pct": (v["revenue"] / prev - 1) if prev else None}
            prev = v["revenue"]
    return {"sales": sales, "top": top_n(prod, 10), "mix": mix, "comp": comp}


def top_n(prod: dict, n: int) -> dict:
    """{(periodo, rank): [(company, product_name, revenue, qty)]} con semantica RANK (empates comparten puesto).
    Solo entran productos con ingreso (> 0); el miembro desconocido no tiene ingreso."""
    by_period = defaultdict(list)
    for (period, company, name), v in prod.items():
        if v["revenue"] > 0:
            by_period[period].append((company, name, v["revenue"], v["qty"]))
    out = {}
    for period, rows in by_period.items():
        rows.sort(key=lambda r: -r[2])
        for i, r in enumerate(rows):
            rank = 1 + sum(1 for x in rows if x[2] > r[2])
            if rank > n:
                break
            out[(period, r[0], r[1])] = {"rank": rank, "revenue": r[2], "total_qty": r[3]}
    return out


# ---------------------------------------------------------------- comparacion (pura)
def _num(x):
    return None if x is None else Decimal(str(x))


def close(a, b, tol: Decimal = TOL) -> bool:
    a, b = _num(a), _num(b)
    if a is None or b is None:
        return a is None and b is None
    return abs(a - b) <= tol


def compare(name: str, expected: dict, actual: dict, fields: list[str], tol: Decimal = TOL,
            ratio_fields: tuple = ("revenue_share", "revenue_share_month", "revenue_mom_pct")) -> list[str]:
    """Lista de diferencias (vacia = OK). Claves faltantes/sobrantes y campos fuera de tolerancia.
    Las razones se comparan con tolerancia 1e-6; el resto con `tol`. Ingreso Gold NULL = 0."""
    diffs = []
    for k in sorted(set(expected) | set(actual), key=str):
        if k not in actual:
            diffs.append(f"{name}{k}: falta en Gold (esperado {expected[k]})")
        elif k not in expected:
            diffs.append(f"{name}{k}: sobra en Gold ({actual[k]})")
        else:
            for f in fields:
                e, a = expected[k].get(f), actual[k].get(f)
                if f == "revenue":
                    e, a = e or 0, a or 0
                t = Decimal("0.000001") if f in ratio_fields else tol
                if not close(e, a, t):
                    diffs.append(f"{name}{k}.{f}: esperado {e} vs Gold {a}")
    return diffs


# ---------------------------------------------------------------- Gold (solo SELECT)
def query(sql: str, warehouse: str) -> list[list]:
    if not sql.lstrip().upper().startswith(("SELECT", "WITH")):
        raise ValueError("solo se permiten consultas SELECT")
    import run_sql
    r = run_sql.run(sql, warehouse)
    if r["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(r["status"].get("error", r["status"]))
    rows = list(r.get("result", {}).get("data_array") or [])
    nxt = r.get("result", {}).get("next_chunk_internal_link")
    while nxt:
        c = run_sql.api("get", nxt)
        rows += c.get("data_array") or []
        nxt = c.get("next_chunk_internal_link")
    return rows


def fetch_gold(warehouse: str) -> dict:
    g = f"{CATALOG}.gold"
    ym = "date_format(month_start, 'yyyy-MM')"
    sales = query(f"SELECT company, year_month, revenue, total_qty, order_lines, orders, lines_without_price "
                  f"FROM {g}.v_kpi_sales_monthly", warehouse)
    top = query(f"SELECT period, company, product_name, revenue_rank, revenue, total_qty "
                f"FROM {g}.v_kpi_top10_products", warehouse)
    mix = query(f"SELECT company, {ym}, category, revenue, total_qty, revenue_share "
                f"FROM {g}.v_kpi_category_mix", warehouse)
    comp = query(f"SELECT company, {ym}, revenue, total_qty, orders, revenue_share_month, revenue_mom_pct "
                 f"FROM {g}.v_kpi_company_comparison", warehouse)
    cnt = query(f"SELECT COUNT(*), SUM(line_amount), SUM(order_qty) FROM {g}.fact_sales", warehouse)
    d = _num
    return {
        "sales": {(r[0], r[1]): dict(zip(["revenue", "total_qty", "order_lines", "orders", "lines_without_price"],
                                         [d(r[2]), int(r[3]), int(r[4]), int(r[5]), int(r[6])])) for r in sales},
        "top": {(r[0], r[1], r[2]): {"rank": int(r[3]), "revenue": d(r[4]), "total_qty": int(r[5])} for r in top},
        "mix": {(r[0], r[1], r[2]): {"revenue": d(r[3]), "total_qty": int(r[4]), "revenue_share": d(r[5])}
                for r in mix},
        "comp": {(r[0], r[1]): {"revenue": d(r[2]), "total_qty": int(r[3]), "orders": int(r[4]),
                                "revenue_share_month": d(r[5]), "revenue_mom_pct": d(r[6])} for r in comp},
        "fact": {"lines": int(cnt[0][0]), "revenue": d(cnt[0][1]), "qty": int(cnt[0][2])},
    }


CHECKS = [
    ("sales", "ingresos/cantidad/lineas/pedidos por (company, mes)",
     ["revenue", "total_qty", "order_lines", "orders", "lines_without_price"]),
    ("top", "top 10 productos por ingreso (periodo = anio | Total)", ["rank", "revenue", "total_qty"]),
    ("mix", "mix por categoria (ingreso, cantidad, participacion)", ["revenue", "total_qty", "revenue_share"]),
    ("comp", "comparativo madre vs hija", ["revenue", "total_qty", "orders", "revenue_share_month",
                                          "revenue_mom_pct"]),
]


def totals(exp: dict) -> dict:
    s = exp["sales"].values()
    return {"lines": sum(v["order_lines"] for v in s), "revenue": sum((v["revenue"] for v in s), Decimal(0)),
            "qty": sum(v["total_qty"] for v in s)}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--root", default=str(ROOT / "data/generated"))
    ap.add_argument("--warehouse", default="<warehouse-id>")
    a = ap.parse_args()
    exp = compute_kpis(silver_rows(Path(a.root)))
    gold = fetch_gold(a.warehouse)
    bad = 0
    et, gt = totals(exp), gold["fact"]
    print(f"{'total fact_sales':58s} esperado lineas={et['lines']} ingreso={et['revenue']} qty={et['qty']}")
    print(f"{'':58s} Gold     lineas={gt['lines']} ingreso={gt['revenue']} qty={gt['qty']}")
    if et["lines"] != gt["lines"] or not close(et["revenue"], gt["revenue"]) or et["qty"] != gt["qty"]:
        bad += 1
        print("  DIFIERE (totales)")
    for key, title, fields in CHECKS:
        diffs = compare(f"{key}", exp[key], gold[key], fields)
        print(f"{title:58s} {len(exp[key]):5d} filas  {'OK' if not diffs else f'DIFIERE ({len(diffs)})'}")
        for d in diffs[:15]:
            print("   ", d)
        bad += bool(diffs)
    print("VEREDICTO:", "Gold coincide con el calculo independiente" if not bad else f"{bad} chequeo(s) difieren")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
