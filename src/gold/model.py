"""Logica PURA de Gold (sin Spark): calendario, vigencias, claves y features ML.

Es el oraculo testeable (pytest) de lo que `transform.py` hace con expresiones Spark, y la
documentacion ejecutable de cada decision. `dim_date` se construye DIRECTAMENTE con estas funciones
(son ~1.000 filas: se generan en el driver), asi que esa tabla queda probada localmente de punta a punta.
"""
from __future__ import annotations

import calendar
import datetime as dt
from collections import defaultdict
from decimal import ROUND_HALF_UP, Decimal

UNKNOWN_SK = -1                 # miembro desconocido de todas las dimensiones (customer/product/price/date)
UNKNOWN_TEXT = "Unknown"
MONTHS_ES = ("enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto",
             "septiembre", "octubre", "noviembre", "diciembre")
DAYS_ES = ("lunes", "martes", "miercoles", "jueves", "viernes", "sabado", "domingo")   # ISO 1..7
FEATURE_LAGS = (1, 2, 3)


# ------------------------------------------------------------------ calendario (dim_date)
def date_sk(d: dt.date | None) -> int:
    """yyyymmdd como entero; NULL -> -1 (miembro desconocido)."""
    return UNKNOWN_SK if d is None else d.year * 10000 + d.month * 100 + d.day


def month_end(d: dt.date) -> dt.date:
    return d.replace(day=calendar.monthrange(d.year, d.month)[1])


def month_index(d: dt.date) -> int:
    """Meses corridos desde el anio 0: dos meses consecutivos difieren en 1 (tambien dic -> ene)."""
    return d.year * 12 + d.month - 1


def calendar_bounds(min_date: dt.date, max_date: dt.date) -> tuple[dt.date, dt.date]:
    """El calendario cubre anios completos (1-ene del primero al 31-dic del ultimo)."""
    if min_date > max_date:
        raise ValueError(f"rango invalido: {min_date} > {max_date}")
    return dt.date(min_date.year, 1, 1), dt.date(max_date.year, 12, 31)


def date_row(d: dt.date) -> dict:
    iso = d.isocalendar()
    q = (d.month - 1) // 3 + 1
    return {
        "date_sk": date_sk(d), "date": d, "year": d.year, "quarter": q, "quarter_label": f"Q{q}",
        "month": d.month, "month_name": MONTHS_ES[d.month - 1], "year_month": f"{d.year:04d}-{d.month:02d}",
        "month_start": d.replace(day=1), "month_end": month_end(d), "day": d.day,
        "day_of_week": iso[2], "day_name": DAYS_ES[iso[2] - 1], "is_weekend": iso[2] >= 6,
        "week_of_year": iso[1], "day_of_year": d.timetuple().tm_yday, "is_month_end": d == month_end(d),
        "is_unknown_member": False,
    }


def unknown_date_row() -> dict:
    row = {k: None for k in date_row(dt.date(2000, 1, 1))}
    row.update(date_sk=UNKNOWN_SK, quarter_label=UNKNOWN_TEXT, month_name=UNKNOWN_TEXT,
               year_month=UNKNOWN_TEXT, day_name=UNKNOWN_TEXT, is_weekend=False, is_month_end=False,
               is_unknown_member=True)
    return row


def date_rows(start: dt.date, end: dt.date) -> list[dict]:
    """Filas de dim_date (dia a dia, ambos extremos incluidos) + el miembro -1 al final."""
    n = (end - start).days + 1
    return [date_row(start + dt.timedelta(days=i)) for i in range(n)] + [unknown_date_row()]


# ------------------------------------------------------------------ precios con vigencia (dim_price)
def price_validity(price_month: dt.date) -> tuple[dt.date, dt.date]:
    """Vigencia de un precio mensual: todo su mes. NO se estira hasta el siguiente precio: si falta el
    precio de un mes (cuarentena PRC_05), Silver deja line_amount NULL (ORD_10) y Gold respeta eso."""
    start = price_month.replace(day=1)
    return start, month_end(start)


def pct_change(prev: Decimal | None, cur: Decimal | None) -> Decimal | None:
    """Variacion relativa (cur/prev - 1) con 4 decimales; NULL si falta alguno o prev es 0."""
    if prev is None or cur is None or prev == 0:
        return None
    return ((cur / prev) - 1).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)


def previous_month_prices(rows: list[dict]) -> list[dict]:
    """rows: {company, product_id, price_month, unit_price}. Agrega prev_unit_price (precio del mes
    calendario ANTERIOR, NULL si ese mes no tiene precio) y price_change_pct."""
    by_key = {(r["company"], r["product_id"], month_index(r["price_month"])): r["unit_price"] for r in rows}
    out = []
    for r in rows:
        prev = by_key.get((r["company"], r["product_id"], month_index(r["price_month"]) - 1))
        out.append({**r, "prev_unit_price": prev, "price_change_pct": pct_change(prev, r["unit_price"])})
    return out


# ------------------------------------------------------------------ features ML (feature_sales_monthly)
def monthly_features(rows: list[dict], max_month: dt.date | None = None) -> list[dict]:
    """Features por (company, customer_sk, product_sk, currency) y mes, con lags de CALENDARIO.

    rows: una fila por grupo y mes con compras: {company, customer_sk, product_sk, currency,
    month_start, revenue, total_qty}. Un mes sin compras no tiene fila, por eso los lags se buscan
    por mes calendario (no "la fila anterior"): mes ausente -> 0.
      revenue_lag_k / qty_lag_k  = valor de hace k meses (0 si no compro)
      revenue_avg_3m             = (lag1 + lag2 + lag3) / 3 (excluye el mes actual: sin fuga)
      months_since_prev_purchase = meses desde la compra anterior del grupo (NULL si es la primera)
      purchase_month_seq         = n-esimo mes con compras del grupo (1, 2, ...)
      target_revenue_next_month  = ingreso del mes siguiente (0 si no compra); NULL si el mes
                                   siguiente es posterior a max_month (todavia no se conoce)
    """
    if max_month is None:
        max_month = max(r["month_start"] for r in rows) if rows else None
    groups = defaultdict(dict)
    for r in rows:
        g = (r["company"], r["customer_sk"], r["product_sk"], r["currency"])
        groups[g][month_index(r["month_start"])] = r
    out = []
    zero = Decimal("0")
    for g, months in groups.items():
        seq = sorted(months)
        for i, m in enumerate(seq):
            r = months[m]
            f = dict(r)
            for k in FEATURE_LAGS:
                p = months.get(m - k)
                f[f"revenue_lag_{k}"] = p["revenue"] if p and p["revenue"] is not None else zero
                f[f"qty_lag_{k}"] = p["total_qty"] if p and p["total_qty"] is not None else 0
            f["revenue_avg_3m"] = (sum((f[f"revenue_lag_{k}"] for k in FEATURE_LAGS), zero) / 3).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP)
            f["months_since_prev_purchase"] = m - seq[i - 1] if i else None
            f["purchase_month_seq"] = i + 1
            nxt = months.get(m + 1)
            known = max_month is not None and m + 1 <= month_index(max_month)
            nxt_rev = nxt["revenue"] if nxt and nxt["revenue"] is not None else zero
            f["target_revenue_next_month"] = nxt_rev if known else None
            out.append(f)
    return out


# ------------------------------------------------------------------ KPIs (oraculos de las vistas)
def top_n(revenue_by_item: dict, n: int = 10) -> list[tuple]:
    """[(item, revenue, rank)] con RANK() (empates comparten puesto y saltan el siguiente)."""
    ordered = sorted(revenue_by_item.items(), key=lambda kv: (-kv[1], str(kv[0])))
    out, rank = [], 0
    for i, (item, rev) in enumerate(ordered, start=1):
        if i == 1 or rev != ordered[i - 2][1]:
            rank = i
        if rank > n:
            break
        out.append((item, rev, rank))
    return out


def shares(values: dict) -> dict:
    """Participacion de cada clave sobre el total (NULL-safe: total 0 -> None)."""
    total = sum(v for v in values.values() if v is not None)
    return {k: (None if not total or v is None else v / total) for k, v in values.items()}
