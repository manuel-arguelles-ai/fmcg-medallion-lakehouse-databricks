"""Conteos ESPERADOS de Silver/cuarentena calculados con las reglas puras sobre los CSV de datagen.

Sirve para reconciliar la corrida en el workspace (modo `full`, Bronze = todos los archivos):
    cd src && python -m silver.expected ../data/generated
Emula Bronze: _ingest_ts igual para todo; el desempate del dedup es _source_file (la ruta con la fecha mas nueva gana).
"""
from __future__ import annotations

import csv
import sys
from collections import Counter
from pathlib import Path

from silver import reference_data as ref
from silver import rules as R


def read_rows(root: Path, company: str, source: str) -> list[dict]:
    rows = []
    for f in sorted((root / company / source).glob("ingest_date=*/*.csv")):
        with f.open(newline="", encoding="utf-8") as fh:
            rows += [{**r, "_ingest_ts": "", "_source_file": f"{f.parent.name}/{f.name}"} for r in csv.DictReader(fh)]
    return rows


def expected(root: Path) -> dict:
    """{(entidad, compania): {'bronze', 'silver', 'quarantine', 'rules': {rule_id: n}}}."""
    typos = [(w, c) for _, _, w, c in ref.TYPO_FIXES]
    out = {}
    for company in ("madre", "hija"):
        def run(source, fn, key_fn, key_ok):
            raw = read_rows(root, company, source)
            keyed = [r for r in raw if key_ok(r)]
            winners, removed = R.dedup_latest(keyed, key_fn)
            res = [fn(r) for r in winners]
            c = Counter(x for r in res for x in r.applied)
            c.update(rid for r in res for rid, _ in r.rejects)
            c.update(rid for r in raw if not key_ok(r) for rid in [fn(r).rejects[0][0]])
            return raw, winners, res, c, removed

        cust_raw, cw, cres, cc, rem = run("customers", lambda r: R.clean_customer(r, ref.CITY_MAPPING),
                                          lambda r: r["customer_id"], lambda r: R.normalize_id(r["customer_id"]))
        cc["CUS_08"] = rem
        prod_raw, pw, pres, pc, rem = run("products", lambda r: R.clean_product(r, ref.CATEGORY_DIVISION, typos),
                                          lambda r: R.normalize_product_id(r["product_id"])[0],
                                          lambda r: R.normalize_product_id(r["product_id"])[0])
        pc["PRD_07"] = rem
        known_p = {r.clean["product_id"] for r in pres if r.clean}
        known_c = {r.clean["customer_id"] for r in cres if r.clean}
        pr_key = lambda r: (R.normalize_product_id(r["product_id"])[0], R.parse_date(r["price_month"])[0] and
                            R.month_start(R.parse_date(r["price_month"])[0]))
        pr_raw, prw, prres, prc, rem = run("prices", lambda r: R.clean_price(r, known_p), pr_key,
                                           lambda r: None not in pr_key(r))
        prc["PRC_06"] = rem
        price_map = {(r.clean["product_id"], r.clean["price_month"]): r.clean["unit_price"] for r in prres if r.clean}
        o_key = lambda r: (R.normalize_id(r["order_id"]), R.normalize_id(r["line_no"]))
        o_raw, ow, ores, oc, rem = run("orders", lambda r: R.clean_order(r, known_c, known_p, price_map), o_key,
                                       lambda r: None not in o_key(r))
        oc["ORD_11"] = rem
        for name, raw, res, c in (("customers", cust_raw, cres, cc), ("products", prod_raw, pres, pc),
                                  ("prices", pr_raw, prres, prc), ("orders", o_raw, ores, oc)):
            ok = sum(1 for r in res if r.clean)
            qrules = {r.rule_id for r in R.RULES if r.action == R.CUARENTENA}
            out[(name, company)] = {"bronze": len(raw), "silver": ok, "rules": dict(sorted(c.items())),
                                    "quarantine_rules": {k: v for k, v in sorted(c.items()) if k in qrules}}
    return out


if __name__ == "__main__":
    for (entity, company), v in expected(Path(sys.argv[1] if len(sys.argv) > 1 else "data/generated")).items():
        print(f"{entity:10s} {company:6s} bronze={v['bronze']:>8} silver={v['silver']:>8} "
              f"cuarentena(reglas)={v['quarantine_rules']}")
        print(f"{'':18s}reglas={v['rules']}")
