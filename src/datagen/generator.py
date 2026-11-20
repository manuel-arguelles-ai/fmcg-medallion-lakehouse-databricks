"""Generador sintetico reproducible de las 2 companias x 4 fuentes.

Todo sale de random.Random con semillas derivadas de (seed, compania, fuente): misma semilla y
mismos parametros => bytes identicos. La `madre` es limpia; la `hija` lleva anomalias sembradas,
y cada una queda contada en MANIFEST.json (conteo sobre las filas EMITIDAS, copias incluidas).
"""
from __future__ import annotations

import bisect
import csv
import datetime as dt
import json
import random
import shutil
from collections import Counter
from pathlib import Path

from . import catalog_data as cd

COMPANIES = ("madre", "hija")
SOURCES = ("customers", "products", "prices", "orders")

# Catalogo documentado de anomalias (solo la hija las tiene; la madre debe dar 0 en todas).
ANOMALY_DOCS = {
    "dup_rows": "fila copiada tal cual (en customers/products dentro del mismo snapshot; en orders en el mismo archivo o en el del dia siguiente)",
    "dup_cross_file": "subconjunto de dup_rows de orders reenviado en el archivo del dia siguiente",
    "name_whitespace": "customer_name con espacios al inicio/final/dobles",
    "name_case_mixed": "customer_name en MAYUSCULAS o minusculas",
    "city_typo": "city con error ortografico (p. ej. Trujilo, Cuzco)",
    "city_null": "city vacia",
    "category_lowercase": "category en minusculas",
    "name_typo_term": "product_name con el typo recurrente Energisante (correcto: Energizante)",
    "product_id_non_numeric": "product_id no numerico en products (formato PX-<n>); orders/prices usan el id numerico",
    "date_format_ymd_slash": "fecha con formato yyyy/MM/dd",
    "date_format_dmy_dash": "fecha con formato dd-MM-yyyy",
    "date_format_dmy_slash": "fecha con formato dd/MM/yyyy",
    "date_format_weekday_long": "fecha con formato 'Tuesday, July 01, 2025'",
    "negative_price": "unit_price negativo",
    "non_numeric_price": "unit_price no numerico (N/A, abc, --, s/p)",
    "customer_id_invalid": "customer_id no numerico en orders (N/A, UNKNOWN, #REF!, sin_dato)",
    "order_qty_null": "order_qty vacio",
}

# Tasas de siembra en la hija (fraccion de filas/entidades).
RATES = {
    "name_whitespace": 0.08, "name_case_mixed": 0.08, "city_typo": 0.07, "city_null": 0.03,
    "category_lowercase": 0.20, "product_id_non_numeric": 0.12,
    "negative_price": 0.02, "non_numeric_price": 0.02,
    "customer_id_invalid": 0.015, "order_qty_null": 0.015,
    "dup_dim": 0.03, "dup_order": 0.015, "dup_cross_share": 0.4,
}
ORDER_DATE_FORMATS_HIJA = ["ymd_slash", "dmy_dash", "dmy_slash", "weekday_long"]
PRICE_DATE_FORMATS_HIJA = ["ymd_dash", "ymd_slash", "dmy_slash", "dmy_dash"]
WEEKDAY_FACTOR = [0.9, 0.95, 1.0, 1.0, 1.1, 1.2, 0.85]  # lunes..domingo


def fmt_date(d: dt.date, key: str) -> str:
    """Formatea una fecha segun la clave de formato (locale-independiente)."""
    if key == "ymd_dash":
        return d.strftime("%Y-%m-%d")
    if key == "ymd_slash":
        return d.strftime("%Y/%m/%d")
    if key == "dmy_dash":
        return d.strftime("%d-%m-%Y")
    if key == "dmy_slash":
        return d.strftime("%d/%m/%Y")
    if key == "weekday_long":
        return f"{cd.WEEKDAYS[d.weekday()]}, {cd.MONTHS[d.month - 1]} {d.day:02d}, {d.year}"
    raise ValueError(key)


def _date_tag(key: str) -> list[str]:
    return [] if key == "ymd_dash" else [f"date_format_{key}"]


def _rng(seed: int, company: str, source: str) -> random.Random:
    return random.Random(f"{seed}:{company}:{source}")


def _pick_idx(rng: random.Random, n: int, k: int) -> set[int]:
    """k indices distintos de range(n), al menos 1 si n>0 y k>0."""
    return set(rng.sample(range(n), min(n, k))) if n and k else set()


def _count(n: int, rate: float) -> int:
    return max(1, round(n * rate)) if rate > 0 else 0


class Writer:
    """Escribe CSV por archivo y acumula las entradas del manifest."""

    def __init__(self, out: Path):
        self.out = out
        self.files: list[dict] = []

    def write(self, company, source, ingest_date: dt.date, filename, header, rows):
        """rows = lista de (valores, tags)."""
        rel = Path(company) / source / f"ingest_date={ingest_date.isoformat()}" / filename
        path = self.out / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        tags = Counter()
        with path.open("w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh, lineterminator="\n")
            w.writerow(header)
            for values, t in rows:
                w.writerow(values)
                for x in t:
                    tags[x] += 1
        self.files.append({
            "company": company, "source": source, "path": rel.as_posix(),
            "ingest_date": ingest_date.isoformat(), "rows": len(rows),
            "bytes": path.stat().st_size, "anomalies": dict(sorted(tags.items())),
        })


def _snapshot_dates(start: dt.date, end: dt.date, mode: str) -> list[dt.date]:
    days = (end - start).days + 1
    all_days = [start + dt.timedelta(d) for d in range(days)]
    return all_days if mode == "daily" else [d for d in all_days if d.day == 1 or d == start]


def _with_dups(rng, rows, rate):
    """Inserta copias (tag dup_rows) de filas aleatorias en posiciones aleatorias."""
    out = list(rows)
    for i in sorted(_pick_idx(rng, len(rows), _count(len(rows), rate))):
        values, tags = rows[i]
        out.insert(rng.randrange(len(out) + 1), (values, tags + ["dup_rows"]))
    return out


# ---------------------------------------------------------------- customers
def build_customers(company, seed, n, start, end):
    """Maestro de clientes: lista de dicts con la fila ya 'sucia' si es hija y la fecha de alta."""
    rng = _rng(seed, company, "customers")
    dirty = company == "hija"
    prefixes = cd.HIJA_CUSTOMER_PREFIX if dirty else cd.MADRE_CUSTOMER_PREFIX
    base_id = 500001 if dirty else 100001
    span = (end - start).days
    rows = []
    for i in range(n):
        name = f"{rng.choice(prefixes)} {rng.choice(cd.CUSTOMER_NAMES)} {i + 1}"
        city = rng.choice(cd.CITIES)
        since = start if rng.random() < 0.6 else start + dt.timedelta(rng.randrange(max(1, span // 2)))
        rows.append({"id": str(base_id + i), "name": name, "city": city, "since": since, "tags": []})
    rows.sort(key=lambda r: (r["since"], r["id"]))
    if dirty:
        n = len(rows)
        for i in _pick_idx(rng, n, _count(n, RATES["name_whitespace"])):
            r = rows[i]
            r["name"] = rng.choice(["  {}", "{}  ", " {} ", "{}   "]).format(r["name"].replace(" ", "  ", 1))
            r["tags"].append("name_whitespace")
        for i in _pick_idx(rng, n, _count(n, RATES["name_case_mixed"])):
            r = rows[i]
            r["name"] = r["name"].upper() if rng.random() < 0.5 else r["name"].lower()
            r["tags"].append("name_case_mixed")
        for i in _pick_idx(rng, n, _count(n, RATES["city_typo"])):
            r = rows[i]
            r["city"] = rng.choice(cd.CITY_TYPOS[r["city"]]) if r["city"] in cd.CITY_TYPOS else r["city"]
            r["tags"].append("city_typo")
        for i in _pick_idx(rng, n, _count(n, RATES["city_null"])):
            r = rows[i]
            r["city"] = ""
            r["tags"] = [t for t in r["tags"] if t != "city_typo"] + ["city_null"]
    return rows


def gen_customers(w, company, seed, customers, dates):
    rng = _rng(seed, company, "customers.snap")
    for d in dates:
        rows = [([c["id"], c["name"], c["city"]], list(c["tags"])) for c in customers if c["since"] <= d]
        if company == "hija":
            rows = _with_dups(rng, rows, RATES["dup_dim"])
        w.write(company, "customers", d, f"customers_{company}_{d:%Y%m%d}.csv", cd.COLUMNS["customers"], rows)


# ---------------------------------------------------------------- products
def build_products(company, seed):
    rng = _rng(seed, company, "products")
    dirty = company == "hija"
    tree = cd.HIJA_PRODUCTS if dirty else cd.MADRE_PRODUCTS
    pid = 2001 if dirty else 1001
    prods = []
    for cat, bases in tree.items():
        for base, variants in bases:
            for v in variants:
                prods.append({"id": str(pid), "clean_id": str(pid), "name": f"{base} ({v})",
                              "cat": cat, "base_price": round(rng.uniform(2.5, 18.0), 2), "tags": []})
                pid += 1
    if dirty:
        n = len(prods)
        for p in prods:  # typo recurrente: todos los nombres con el termino
            if cd.TYPO_TERM[0] in p["name"]:
                p["name"] = p["name"].replace(*cd.TYPO_TERM)
                p["tags"].append("name_typo_term")
        for i in _pick_idx(rng, n, _count(n, RATES["category_lowercase"])):
            prods[i]["cat"] = prods[i]["cat"].lower()
            prods[i]["tags"].append("category_lowercase")
        for i in _pick_idx(rng, n, _count(n, RATES["product_id_non_numeric"])):
            prods[i]["id"] = f"PX-{prods[i]['clean_id']}"
            prods[i]["tags"].append("product_id_non_numeric")
    return prods


def gen_products(w, company, seed, products, dates):
    rng = _rng(seed, company, "products.snap")
    for d in dates:
        rows = [([p["id"], p["name"], p["cat"]], list(p["tags"])) for p in products]
        if company == "hija":
            rows = _with_dups(rng, rows, RATES["dup_dim"])
        w.write(company, "products", d, f"products_{company}_{d:%Y%m%d}.csv", cd.COLUMNS["products"], rows)


# ---------------------------------------------------------------- prices
def gen_prices(w, company, seed, products, start, end):
    rng = _rng(seed, company, "prices")
    dirty = company == "hija"
    months, d = [], dt.date(start.year, start.month, 1)
    while d <= end:
        months.append(d)
        d = dt.date(d.year + (d.month == 12), d.month % 12 + 1, 1)
    current = {p["clean_id"]: p["base_price"] for p in products}
    for m in months:
        rows = []
        n = len(products)
        neg = _pick_idx(rng, n, _count(n, RATES["negative_price"])) if dirty else set()
        rest = [i for i in range(n) if i not in neg]
        bad = {rest[j] for j in _pick_idx(rng, len(rest), _count(n, RATES["non_numeric_price"]))} if dirty else set()
        for i, p in enumerate(products):
            cid = p["clean_id"]
            current[cid] = round(max(0.5, current[cid] * rng.uniform(0.98, 1.03)), 2)
            price, tags = f"{current[cid]:.2f}", []
            key = "ymd_dash"
            if dirty:
                key = rng.choice(PRICE_DATE_FORMATS_HIJA)
                tags += _date_tag(key)
                if i in neg:
                    price, tags = f"-{current[cid]:.2f}", tags + ["negative_price"]
                elif i in bad:
                    price, tags = rng.choice(cd.NON_NUMERIC_PRICES), tags + ["non_numeric_price"]
            rows.append(([cid, fmt_date(m, key), price], tags))
        w.write(company, "prices", m, f"prices_{company}_{m:%Y%m%d}.csv", cd.COLUMNS["prices"], rows)


# ---------------------------------------------------------------- orders
def gen_orders(w, company, seed, customers, products, start, end, total_lines):
    rng = _rng(seed, company, "orders")
    dirty = company == "hija"
    n_days = (end - start).days + 1
    per_day = total_lines / n_days
    since = [c["since"] for c in customers]  # ordenado por alta
    pids = [p["clean_id"] for p in products]
    n_prod = len(pids)
    qty_pop, qty_cum = [1, 2, 3, 4, 6, 12, 24], [30, 55, 75, 87, 94, 98, 100]
    nl_pop, nl_cum = [1, 2, 3, 4, 5], [30, 60, 80, 92, 100]
    order_id, carry = 0, []
    for k in range(n_days):
        d = start + dt.timedelta(k)
        target = round(per_day * WEEKDAY_FACTOR[d.weekday()] * rng.uniform(0.9, 1.1))
        active = bisect.bisect_right(since, d)
        rows = list(carry)
        carry = []
        lines = 0
        while lines < target:
            order_id += 1
            n_lines = rng.choices(nl_pop, cum_weights=nl_cum)[0]
            ci = int(active * rng.random() ** 1.4)
            cust = customers[min(ci, active - 1)]["id"]
            chosen = rng.sample(range(n_prod), min(n_lines, n_prod))
            for ln, pi in enumerate(chosen, 1):
                tags, key = [], "ymd_dash"
                qty = str(rng.choices(qty_pop, cum_weights=qty_cum)[0])
                cid = cust
                if dirty:
                    key = rng.choice(ORDER_DATE_FORMATS_HIJA)
                    tags += _date_tag(key)
                    if rng.random() < RATES["customer_id_invalid"]:
                        cid, tags = rng.choice(cd.INVALID_CUSTOMER_IDS), tags + ["customer_id_invalid"]
                    if rng.random() < RATES["order_qty_null"]:
                        qty, tags = "", tags + ["order_qty_null"]
                row = ([str(order_id), str(ln), fmt_date(d, key), cid, pids[pi], qty], tags)
                rows.append(row)
                lines += 1
                if dirty and rng.random() < RATES["dup_order"]:
                    if rng.random() < RATES["dup_cross_share"] and k < n_days - 1:
                        carry.append((row[0], tags + ["dup_rows", "dup_cross_file"]))
                    else:
                        rows.append((row[0], tags + ["dup_rows"]))
        w.write(company, "orders", d, f"orders_{company}_{d:%Y%m%d}.csv", cd.COLUMNS["orders"], rows)


# ---------------------------------------------------------------- orquestacion
def generate(out: Path, seed=42, start=dt.date(2024, 1, 1), end=dt.date(2025, 12, 31),
             total_lines=1_000_000, hija_share=0.3, dim_snapshots="monthly",
             n_customers=(1500, 600)) -> dict:
    """Genera todo bajo `out` y devuelve (y escribe) el manifest."""
    out = Path(out)
    for c in COMPANIES:
        shutil.rmtree(out / c, ignore_errors=True)
    w = Writer(out)
    lines = {"madre": round(total_lines * (1 - hija_share)), "hija": round(total_lines * hija_share)}
    dates = _snapshot_dates(start, end, dim_snapshots)
    for c, n_cust in zip(COMPANIES, n_customers):
        customers = build_customers(c, seed, n_cust, start, end)
        products = build_products(c, seed)
        gen_customers(w, c, seed, customers, dates)
        gen_products(w, c, seed, products, dates)
        gen_prices(w, c, seed, products, start, end)
        gen_orders(w, c, seed, customers, products, start, end, lines[c])
    totals = {}
    for f in w.files:
        t = totals.setdefault(f["company"], {}).setdefault(
            f["source"], {"files": 0, "rows": 0, "bytes": 0, "anomalies": Counter()})
        t["files"] += 1
        t["rows"] += f["rows"]
        t["bytes"] += f["bytes"]
        t["anomalies"].update(f["anomalies"])
    for comp in totals.values():
        for t in comp.values():
            t["anomalies"] = dict(sorted(t["anomalies"].items()))
    manifest = {
        "generator_version": 1,
        "params": {"seed": seed, "start": start.isoformat(), "end": end.isoformat(),
                   "total_lines": total_lines, "hija_share": hija_share,
                   "dim_snapshots": dim_snapshots, "n_customers": list(n_customers)},
        "anomaly_catalog": ANOMALY_DOCS,
        "totals": totals,
        "files": sorted(w.files, key=lambda f: f["path"]),
    }
    (out / "MANIFEST.json").write_text(json.dumps(manifest, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    return manifest
