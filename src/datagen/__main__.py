"""CLI: python -m datagen --out data/generated [--small] [--seed N] ..."""
import argparse
import datetime as dt
import time
from pathlib import Path

from .generator import generate


def _month(s: str, last: bool) -> dt.date:
    y, m = map(int, s.split("-"))
    first = dt.date(y, m, 1)
    return (dt.date(y + (m == 12), m % 12 + 1, 1) - dt.timedelta(1)) if last else first


def main(argv=None):
    p = argparse.ArgumentParser(description="Generador sintetico FMCG (madre/hija)")
    p.add_argument("--out", default="data/generated")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--start", default="2024-01", help="mes inicial YYYY-MM")
    p.add_argument("--end", default="2025-12", help="mes final YYYY-MM")
    p.add_argument("--total-lines", type=int, default=1_000_000, help="lineas de pedido, ambas companias")
    p.add_argument("--hija-share", type=float, default=0.3)
    p.add_argument("--dim-snapshots", choices=["monthly", "daily"], default="monthly",
                   help="cadencia de los snapshots de customers/products")
    p.add_argument("--small", action="store_true", help="prueba rapida: 3 meses, 20.000 lineas, menos clientes")
    a = p.parse_args(argv)
    if a.small:
        a.start, a.end, a.total_lines = "2025-10", "2025-12", 20_000
    n_cust = (200, 80) if a.small else (1500, 600)
    t0 = time.time()
    m = generate(Path(a.out), a.seed, _month(a.start, False), _month(a.end, True),
                 a.total_lines, a.hija_share, a.dim_snapshots, n_cust)
    nfiles = len(m["files"])
    size = sum(f["bytes"] for f in m["files"])
    lines = sum(m["totals"][c]["orders"]["rows"] for c in m["totals"])
    print(f"{nfiles} archivos, {lines} filas de orders, {size / 1e6:.1f} MB, {time.time() - t0:.1f} s -> {a.out}")


if __name__ == "__main__":
    main()
