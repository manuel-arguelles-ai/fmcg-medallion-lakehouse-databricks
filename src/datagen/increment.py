"""Lote INCREMENTAL pequeno y reproducible de pedidos de la hija (prueba de H6: modo incremental).

Lee lo ya generado en `root` (pedidos, clientes, productos y precios de la hija) y escribe UN archivo nuevo
  hija/orders/ingest_date=<ultima fecha + 2 dias>/orders_hija_<yyyymmdd>.csv
con pedidos que llegan tarde (fechados en los dos ultimos meses ya cargados, que si tienen precios) y con
anomalias sembradas en posiciones fijas, mas `increments/lote_<ingest_date>.json` con su descripcion.

Anomalias del lote (todas cubiertas por reglas de Silver):
  order_qty_null       1 linea con cantidad vacia                 -> ORD_03 cuarentena
  customer_id_invalid  1 linea con customer_id 'N/A'              -> ORD_07 cliente -1 (la venta cuenta)
  price_missing        1 linea de un producto-mes sin precio      -> ORD_10 line_amount NULL (si existe tal mes)
  dup_rows             1 linea nueva repetida dentro del archivo  -> ORD_11 dedup
  dup_existing         1 linea YA cargada reenviada identica      -> MERGE la actualiza (mismo valor), no suma
Misma semilla + mismos datos de entrada => bytes identicos. No sobrescribe un lote existente distinto.

  cd src && python -m datagen.increment --root ../data/generated [--seed 42] [--orders 12]
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import json
import random
from collections import Counter
from pathlib import Path

from silver import rules as R

from . import catalog_data as cd
from .generator import ORDER_DATE_FORMATS_HIJA, _date_tag, fmt_date

COMPANY = "hija"


def _lot_paths(root: Path) -> set[str]:
    """Rutas (relativas) de lotes incrementales ya escritos: no son parte de la base del lote."""
    return {json.loads(f.read_text(encoding="utf-8"))["path"] for f in (root / "increments").glob("lote_*.json")}


def _read(root: Path, source: str) -> list[tuple[str, dict]]:
    """[(ingest_date, fila)] de los archivos BASE de la hija para `source` (sin lotes incrementales)."""
    out, skip = [], _lot_paths(root)
    for f in sorted((root / COMPANY / source).glob("ingest_date=*/*.csv")):
        if f.relative_to(root).as_posix() in skip:
            continue
        with f.open(newline="", encoding="utf-8") as fh:
            out += [(f.parent.name.split("=", 1)[1], r) for r in csv.DictReader(fh)]
    return out


def _month_end(d: dt.date) -> dt.date:
    nxt = dt.date(d.year + (d.month == 12), d.month % 12 + 1, 1)
    return nxt - dt.timedelta(1)


def build_lot(root: Path, seed: int = 42, n_orders: int = 12) -> tuple[dt.date, list[tuple[list[str], list[str]]], dict]:
    """(ingest_date, filas [(valores, tags)], descripcion). Puro: no escribe nada."""
    rng = random.Random(f"{seed}:{COMPANY}:orders:increment")
    orders = _read(root, "orders")
    if not orders:
        raise RuntimeError(f"sin pedidos de la hija bajo {root}")
    last_ingest = max(dt.date.fromisoformat(d) for d, _ in orders)
    ingest_date = last_ingest + dt.timedelta(2)
    max_oid = max(int(r["order_id"]) for _, r in orders if R.normalize_id(r["order_id"]))

    customers = sorted({cid for _, r in _read(root, "customers") if (cid := R.normalize_id(r["customer_id"]))}, key=int)
    products = sorted({pid for _, r in _read(root, "products")
                       if (pid := R.normalize_product_id(r["product_id"])[0])}, key=int)
    # precios no utilizables por producto-mes (no numericos): una linea con ese producto queda price_missing
    priced, unpriced = set(), set()
    for _, r in _read(root, "prices"):
        d, _ = R.parse_date(r["price_month"])
        pid = R.normalize_product_id(r["product_id"])[0]
        if d and pid:
            (priced if R.parse_price(r["unit_price"])[1] != "invalid" else unpriced).add((pid, R.month_start(d)))
    unpriced -= priced

    prev_month_end = last_ingest.replace(day=1) - dt.timedelta(1)
    dates = [last_ingest, last_ingest - dt.timedelta(1), prev_month_end]  # tardios: ultimos 2 meses con precio
    qty_pop, qty_cum = [1, 2, 3, 4, 6, 12, 24], [30, 55, 75, 87, 94, 98, 100]

    rows: list[tuple[list[str], list[str]]] = []
    for k in range(n_orders):
        oid = max_oid + 1 + k
        d = dates[0] if k < n_orders // 3 else dates[1] if k < 2 * n_orders // 3 else dates[2]
        cust = rng.choice(customers)
        for ln, pid in enumerate(rng.sample(products, rng.choice([1, 2, 3])), 1):
            key = rng.choice(ORDER_DATE_FORMATS_HIJA)
            qty = str(rng.choices(qty_pop, cum_weights=qty_cum)[0])
            rows.append(([str(oid), str(ln), fmt_date(d, key), cust, pid, qty], _date_tag(key)))

    # anomalias en posiciones fijas (lineas 0..3 del lote)
    rows[0][0][5] = ""
    rows[0][1].append("order_qty_null")
    rows[1][0][3] = cd.INVALID_CUSTOMER_IDS[0]
    rows[1][1].append("customer_id_invalid")
    cand = sorted(x for x in unpriced if x[1] >= prev_month_end.replace(day=1))
    price_missing = None
    if cand:
        pid, month = cand[0]
        d = _month_end(month) if _month_end(month) <= last_ingest else last_ingest
        key = ORDER_DATE_FORMATS_HIJA[0]
        rows[2] = ([rows[2][0][0], rows[2][0][1], fmt_date(d, key), rows[2][0][3], pid, rows[2][0][5]],
                   _date_tag(key) + ["price_missing"])
        price_missing = {"product_id": pid, "month": month.isoformat()}
    rows.insert(rng.randrange(len(rows) + 1), (list(rows[3][0]), rows[3][1] + ["dup_rows"]))
    # linea ya cargada (del ultimo archivo) con cantidad y cliente validos, reenviada tal cual
    last = [r for d_, r in orders if d_ == last_ingest.isoformat()
            and R.parse_qty(r["order_qty"])[1] == "ok" and R.normalize_id(r["customer_id"])]
    existing = last[rng.randrange(len(last))]
    rows.append(([existing[c] for c in cd.COLUMNS["orders"]], ["dup_existing"]))

    tags = Counter(t for _, ts in rows for t in ts)
    desc = {
        "company": COMPANY, "source": "orders", "ingest_date": ingest_date.isoformat(), "seed": seed,
        "rows": len(rows), "new_orders": n_orders, "order_id_range": [max_oid + 1, max_oid + n_orders],
        "order_dates": sorted({d.isoformat() for d in dates}), "anomalies": dict(sorted(tags.items())),
        "price_missing": price_missing,
        "dup_existing": {"order_id": existing["order_id"], "line_no": existing["line_no"]},
    }
    return ingest_date, rows, desc


def write_lot(root: Path, seed: int = 42, n_orders: int = 12) -> dict:
    """Escribe el CSV y su descripcion. Si el archivo ya existe con otro contenido, falla (landing inmutable)."""
    root = Path(root)
    ingest_date, rows, desc = build_lot(root, seed, n_orders)
    rel = Path(COMPANY) / "orders" / f"ingest_date={ingest_date.isoformat()}" / f"orders_{COMPANY}_{ingest_date:%Y%m%d}.csv"
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(cd.COLUMNS["orders"])
    for values, _ in rows:
        w.writerow(values)
    content = buf.getvalue()
    path = root / rel
    if path.exists() and path.read_text(encoding="utf-8") != content:
        raise RuntimeError(f"{path} ya existe con otro contenido: el landing es inmutable")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    desc = {**desc, "path": rel.as_posix(), "bytes": path.stat().st_size}
    (root / "increments").mkdir(exist_ok=True)
    (root / "increments" / f"lote_{ingest_date.isoformat()}.json").write_text(
        json.dumps(desc, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    return desc


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Lote incremental de pedidos de la hija")
    ap.add_argument("--root", default="data/generated")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--orders", type=int, default=12)
    a = ap.parse_args()
    print(json.dumps(write_lot(Path(a.root), a.seed, a.orders), indent=1, ensure_ascii=False))
