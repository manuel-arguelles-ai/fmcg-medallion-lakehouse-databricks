"""Reglas de calidad de Silver: catalogo de reglas + funciones PURAS (sin Spark).

`transform.py` aplica las mismas reglas con expresiones Spark; estas funciones son el oraculo
testeable (pytest) y la documentacion ejecutable de cada decision. Acciones:
  corregir   -> se arregla de forma determinista y la fila sigue (se cuenta en dq_results)
  marcar     -> la fila sigue, con una bandera `*_was_*` / `*_unresolved`
  cuarentena -> la fila NO entra a Silver; va a quarantine.<entidad> con rule_id y motivo
"""
from __future__ import annotations

import datetime as dt
import hashlib
import re
from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal

CORREGIR, MARCAR, CUARENTENA = "corregir", "marcar", "cuarentena"
UNKNOWN_ID = "-1"            # miembro desconocido explicito (existe en las dimensiones)
UNKNOWN_TEXT = "Unknown"
CURRENCY = "PEN"             # moneda implicita del dominio sintetico (ver docs/08, duda D17)
PRICE_INVALID_ACTION = CUARENTENA   # alternativa: MARCAR (precio 0 + price_was_invalid). Duda abierta.


@dataclass(frozen=True)
class Rule:
    rule_id: str
    entity: str
    description: str
    severity: str   # error | warning | info
    action: str


def _r(rule_id, description, severity, action):
    return Rule(rule_id, {"CUS": "customers", "PRD": "products", "PRC": "prices", "ORD": "orders"}[rule_id[:3]],
                description, severity, action)


RULES: tuple[Rule, ...] = (
    _r("CUS_01", "customer_id nulo o no numerico", "error", CUARENTENA),
    _r("CUS_02", "customer_name nulo o vacio", "error", CUARENTENA),
    _r("CUS_03", "espacios sobrantes en customer_name (trim + colapso)", "info", CORREGIR),
    _r("CUS_04", "customer_name fuera de initcap (MAYUSCULAS/minusculas)", "info", CORREGIR),
    _r("CUS_05", "typo de ciudad corregido con ref_city_mapping", "info", CORREGIR),
    _r("CUS_06", "ciudad nula -> 'Unknown' (city_was_null)", "warning", MARCAR),
    _r("CUS_07", "ciudad ausente de ref_city_mapping (city_is_unmapped)", "warning", MARCAR),
    _r("CUS_08", "duplicado por (company, customer_id): gana mayor _ingest_ts", "info", CORREGIR),
    _r("PRD_01", "product_id nulo o no reconocible (ni numerico ni PX-<n>)", "error", CUARENTENA),
    _r("PRD_02", "product_id con prefijo PX-<n> normalizado a <n>", "info", CORREGIR),
    _r("PRD_03", "product_name nulo o vacio", "error", CUARENTENA),
    _r("PRD_04", "typo en product_name corregido con ref_typo_fixes", "info", CORREGIR),
    _r("PRD_05", "category normalizada (mayusculas) con ref_category_division", "info", CORREGIR),
    _r("PRD_06", "category ausente de ref_category_division -> division 'Unknown'", "warning", MARCAR),
    _r("PRD_07", "duplicado por (company, product_id): gana mayor _ingest_ts", "info", CORREGIR),
    _r("PRC_01", "product_id nulo o no reconocible", "error", CUARENTENA),
    _r("PRC_02", "price_month nulo o con formato no soportado", "error", CUARENTENA),
    _r("PRC_03", "price_month con formato distinto de yyyy-MM-dd (normalizado)", "info", CORREGIR),
    _r("PRC_04", "unit_price negativo -> valor absoluto + price_was_negative", "warning", CORREGIR),
    _r("PRC_05", "unit_price nulo o no numerico", "error", PRICE_INVALID_ACTION),
    _r("PRC_06", "duplicado por (company, product_id, price_month)", "info", CORREGIR),
    _r("PRC_07", "producto del precio ausente de silver.products (product_unresolved)", "warning", MARCAR),
    _r("ORD_01", "order_id o line_no nulo o no numerico", "error", CUARENTENA),
    _r("ORD_02", "order_date nulo o con formato no soportado", "error", CUARENTENA),
    _r("ORD_03", "order_qty nulo", "error", CUARENTENA),
    _r("ORD_04", "order_qty no es entero positivo", "error", CUARENTENA),
    _r("ORD_05", "product_id nulo o no reconocible", "error", CUARENTENA),
    _r("ORD_06", "order_date con formato distinto de yyyy-MM-dd (normalizado)", "info", CORREGIR),
    _r("ORD_07", "customer_id no numerico -> miembro -1 (customer_id_was_invalid)", "warning", CORREGIR),
    _r("ORD_08", "customer_id numerico ausente de silver.customers -> -1 (customer_unresolved)", "warning", MARCAR),
    _r("ORD_09", "product_id ausente de silver.products -> -1, monto NULL (product_unresolved)", "warning", MARCAR),
    _r("ORD_10", "sin precio vigente del mes -> line_amount NULL (price_missing)", "warning", MARCAR),
    _r("ORD_11", "duplicado por (company, order_id, line_no): gana mayor _ingest_ts", "info", CORREGIR),
)
RULES_BY_ID = {r.rule_id: r for r in RULES}


def get_rule(rule_id: str) -> Rule:
    return RULES_BY_ID[rule_id]


def rules_for(entity: str) -> list[Rule]:
    return [r for r in RULES if r.entity == entity]


# ------------------------------------------------------------------ texto
def collapse_ws(v: str | None) -> str | None:
    """trim + espacios internos colapsados; vacio -> None."""
    if v is None:
        return None
    s = re.sub(r"\s+", " ", v).strip()
    return s or None


def initcap(s: str) -> str:
    """Equivale a F.initcap: primera letra de cada palabra en mayuscula, el resto en minuscula."""
    return " ".join(w[:1].upper() + w[1:].lower() for w in s.split(" "))


def norm_key(v: str | None) -> str | None:
    """Clave de lookup en tablas de referencia: lower(trim)."""
    s = collapse_ws(v)
    return s.lower() if s else None


def apply_typo_fixes(text: str, fixes: list[tuple[str, str]]) -> str:
    """Reemplaza palabras completas erroneas por las correctas (sensible a mayusculas)."""
    for wrong, right in fixes:
        text = re.sub(rf"\b{re.escape(wrong)}\b", right, text)
    return text


def product_code(name: str) -> str:
    """sha2(product_name, 256) como en la referencia."""
    return hashlib.sha256(name.encode("utf-8")).hexdigest()


def split_variant(name: str) -> tuple[str, str | None]:
    """'Isotonica Gol (Limon 500ml)' -> ('Isotonica Gol', 'Limon 500ml')."""
    m = re.search(r"\(([^)]*)\)", name)
    variant = m.group(1).strip() if m and m.group(1).strip() else None
    base = re.sub(r"\s*\([^)]*\)\s*$", "", name).strip()
    return (base or name), variant


# ------------------------------------------------------------------ identificadores
ID_RE = r"^\d{1,18}$"
PRODUCT_ID_RE = r"(?i)^(?:PX-)?(\d{1,18})$"


def normalize_id(raw: str | None) -> str | None:
    """Id numerico limpio o None (nulo, 'N/A', '#REF!', ...)."""
    s = (raw or "").strip()
    return s if re.match(ID_RE, s) else None


def normalize_product_id(raw: str | None) -> tuple[str | None, bool]:
    """(id, tenia_prefijo). 'PX-2001' -> ('2001', True); '2001' -> ('2001', False); resto -> (None, False)."""
    s = (raw or "").strip()
    m = re.match(PRODUCT_ID_RE, s)
    if not m:
        return None, False
    return m.group(1), s.upper().startswith("PX-")


# ------------------------------------------------------------------ fechas
@dataclass(frozen=True)
class DateFormat:
    key: str
    regex: str
    spark_pattern: str
    strip_weekday: bool = False   # 'Tuesday, July 01, 2025' -> 'July 01, 2025' antes de parsear


DATE_FORMATS: tuple[DateFormat, ...] = (
    DateFormat("ymd_dash", r"^\d{4}-\d{2}-\d{2}$", "yyyy-MM-dd"),
    DateFormat("ymd_slash", r"^\d{4}/\d{2}/\d{2}$", "yyyy/MM/dd"),
    DateFormat("dmy_dash", r"^\d{2}-\d{2}-\d{4}$", "dd-MM-yyyy"),
    DateFormat("dmy_slash", r"^\d{2}/\d{2}/\d{4}$", "dd/MM/yyyy"),   # dia primero, nunca MM/dd
    DateFormat("weekday_long", r"^[A-Za-z]+, [A-Za-z]+ \d{1,2}, \d{4}$", "MMMM d, yyyy", strip_weekday=True),
)
ISO_KEY = "ymd_dash"
_MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august",
     "september", "october", "november", "december"], 1)}


def parse_date(raw: str | None) -> tuple[dt.date | None, str | None]:
    """(fecha, clave_de_formato). Formato no soportado o fecha imposible -> (None, None)."""
    s = (raw or "").strip()
    for f in DATE_FORMATS:
        if not re.match(f.regex, s):
            continue
        try:
            if f.key == "ymd_dash":
                y, m, d = s.split("-")
            elif f.key == "ymd_slash":
                y, m, d = s.split("/")
            elif f.key == "dmy_dash":
                d, m, y = s.split("-")
            elif f.key == "dmy_slash":
                d, m, y = s.split("/")
            else:
                mon, d, y = re.match(r"^[A-Za-z]+, ([A-Za-z]+) (\d{1,2}), (\d{4})$", s).groups()
                m = _MONTHS[mon.lower()]
            return dt.date(int(y), int(m), int(d)), f.key
        except (ValueError, KeyError):
            return None, None
    return None, None


def month_start(d: dt.date) -> dt.date:
    return d.replace(day=1)


# ------------------------------------------------------------------ numericos
PRICE_RE = r"^-?\d{1,9}(\.\d+)?$"
QTY_RE = r"^\d{1,9}$"


def parse_price(raw: str | None) -> tuple[Decimal | None, str]:
    """(valor, estado) con estado ok | negative | invalid. Negativo devuelve el valor absoluto."""
    s = (raw or "").strip()
    if not re.match(PRICE_RE, s):
        return None, "invalid"
    v = Decimal(s).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return (abs(v), "negative") if v < 0 else (v, "ok")


def parse_qty(raw: str | None) -> tuple[int | None, str]:
    """(cantidad, estado) con estado ok | null | invalid (no entero o <= 0)."""
    s = (raw or "").strip()
    if not s:
        return None, "null"
    if not re.match(QTY_RE, s) or int(s) <= 0:
        return None, "invalid"
    return int(s), "ok"


# ------------------------------------------------------------------ resultado por fila
@dataclass
class RowResult:
    clean: dict | None = None                       # None si hay rechazos
    rejects: list[tuple[str, str]] = field(default_factory=list)   # (rule_id, motivo)
    applied: list[str] = field(default_factory=list)  # reglas corregir/marcar que dispararon

    def hit(self, rule_id: str):
        self.applied.append(rule_id)

    def reject(self, rule_id: str, reason: str):
        self.rejects.append((rule_id, reason))


def _finish(res: RowResult, clean: dict) -> RowResult:
    res.clean = None if res.rejects else clean
    return res


# ------------------------------------------------------------------ customers
def clean_customer(raw: dict, city_map: dict[str, str]) -> RowResult:
    res = RowResult()
    cid = normalize_id(raw.get("customer_id"))
    if cid is None:
        res.reject("CUS_01", f"customer_id invalido: {raw.get('customer_id')!r}")
    name0 = collapse_ws(raw.get("customer_name"))
    if name0 is None:
        res.reject("CUS_02", "customer_name vacio")
    name = None
    if name0 is not None:
        if (raw.get("customer_name") or "") != name0:
            res.hit("CUS_03")
        name = initcap(name0)
        if name != name0:
            res.hit("CUS_04")
    city0 = collapse_ws(raw.get("city"))
    city_corrected = city_unmapped = False
    if city0 is None:
        city = UNKNOWN_TEXT
        res.hit("CUS_06")
    elif norm_key(city0) in city_map:
        city = city_map[norm_key(city0)]
        if city != city0:
            city_corrected = True
            res.hit("CUS_05")
    else:
        city, city_unmapped = initcap(city0), True
        res.hit("CUS_07")
    return _finish(res, {
        "customer_id": cid, "customer_name": name, "city": city,
        "name_was_normalized": "CUS_03" in res.applied or "CUS_04" in res.applied,
        "city_was_corrected": city_corrected, "city_was_null": city0 is None,
        "city_is_unmapped": city_unmapped,
    })


# ------------------------------------------------------------------ products
def clean_product(raw: dict, category_map: dict[str, tuple[str, str]],
                  typo_fixes: list[tuple[str, str]]) -> RowResult:
    res = RowResult()
    pid, prefixed = normalize_product_id(raw.get("product_id"))
    if pid is None:
        res.reject("PRD_01", f"product_id invalido: {raw.get('product_id')!r}")
    elif prefixed:
        res.hit("PRD_02")
    name0 = collapse_ws(raw.get("product_name"))
    name = None
    if name0 is None:
        res.reject("PRD_03", "product_name vacio")
    else:
        name = apply_typo_fixes(name0, typo_fixes)
        if name != name0:
            res.hit("PRD_04")
    cat0 = collapse_ws(raw.get("category"))
    key = norm_key(cat0)
    if key in category_map:
        category, division = category_map[key]
        if category != cat0:
            res.hit("PRD_05")
    else:
        category, division = (initcap(cat0) if cat0 else UNKNOWN_TEXT), UNKNOWN_TEXT
        res.hit("PRD_06")
    base, variant = split_variant(name) if name else (None, None)
    return _finish(res, {
        "product_id": pid, "product_id_was_prefixed": prefixed,
        "product_name": name, "product": base, "variant": variant,
        "product_code": product_code(name) if name else None,
        "category": category, "division": division,
        "name_was_corrected": "PRD_04" in res.applied,
        "category_was_normalized": "PRD_05" in res.applied,
        "division_was_unknown": division == UNKNOWN_TEXT,
    })


# ------------------------------------------------------------------ prices
def clean_price(raw: dict, known_products: set[str] | None = None) -> RowResult:
    res = RowResult()
    pid, _ = normalize_product_id(raw.get("product_id"))
    if pid is None:
        res.reject("PRC_01", f"product_id invalido: {raw.get('product_id')!r}")
    d, fmt = parse_date(raw.get("price_month"))
    if d is None:
        res.reject("PRC_02", f"price_month no parseable: {raw.get('price_month')!r}")
    elif fmt != ISO_KEY:
        res.hit("PRC_03")
    value, status = parse_price(raw.get("unit_price"))
    invalid = status == "invalid"
    if status == "negative":
        res.hit("PRC_04")
    if invalid:
        if PRICE_INVALID_ACTION == CUARENTENA:
            res.reject("PRC_05", f"unit_price no numerico: {raw.get('unit_price')!r}")
        else:
            res.hit("PRC_05")
            value = Decimal("0.00")
    unresolved = known_products is not None and pid is not None and pid not in known_products
    if unresolved:
        res.hit("PRC_07")
    return _finish(res, {
        "product_id": pid, "price_month": month_start(d) if d else None, "unit_price": value,
        "currency": CURRENCY, "price_was_negative": status == "negative",
        "price_was_invalid": invalid, "month_was_reformatted": fmt not in (None, ISO_KEY),
        "product_unresolved": unresolved,
    })


# ------------------------------------------------------------------ orders
def clean_order(raw: dict, known_customers: set[str] | None = None,
                known_products: set[str] | None = None,
                prices: dict[tuple[str, dt.date], Decimal] | None = None) -> RowResult:
    """`known_*`/`prices` = None desactiva la resolucion (solo reglas de formato)."""
    res = RowResult()
    oid, line = normalize_id(raw.get("order_id")), normalize_id(raw.get("line_no"))
    if oid is None or line is None:
        res.reject("ORD_01", f"order_id/line_no invalido: {raw.get('order_id')!r}/{raw.get('line_no')!r}")
    d, fmt = parse_date(raw.get("order_date"))
    if d is None:
        res.reject("ORD_02", f"order_date no parseable: {raw.get('order_date')!r}")
    elif fmt != ISO_KEY:
        res.hit("ORD_06")
    qty, qstatus = parse_qty(raw.get("order_qty"))
    if qstatus == "null":
        res.reject("ORD_03", "order_qty nulo")
    elif qstatus == "invalid":
        res.reject("ORD_04", f"order_qty invalido: {raw.get('order_qty')!r}")
    pid, _ = normalize_product_id(raw.get("product_id"))
    if pid is None:
        res.reject("ORD_05", f"product_id invalido: {raw.get('product_id')!r}")
    cust = normalize_id(raw.get("customer_id"))
    cust_invalid = cust is None
    cust_unresolved = False
    if cust_invalid:
        res.hit("ORD_07")
        cust = UNKNOWN_ID
    elif known_customers is not None and cust not in known_customers:
        res.hit("ORD_08")
        cust, cust_unresolved = UNKNOWN_ID, True
    prod_unresolved = False
    if pid is not None and known_products is not None and pid not in known_products:
        res.hit("ORD_09")
        pid, prod_unresolved = UNKNOWN_ID, True
    unit = amount = None
    price_missing = False
    if prices is not None and d is not None and pid is not None:
        unit = prices.get((pid, month_start(d))) if not prod_unresolved else None
        price_missing = unit is None and not prod_unresolved
        if price_missing:
            res.hit("ORD_10")
        if unit is not None and qty is not None:
            amount = (unit * qty).quantize(Decimal("0.01"))
    ms = month_start(d) if d else None
    return _finish(res, {
        "order_id": oid, "line_no": int(line) if line else None, "order_date": d,
        "order_year": d.year if d else None, "order_quarter": (d.month - 1) // 3 + 1 if d else None,
        "order_month": d.month if d else None, "order_month_start": ms,
        "is_weekend": d.weekday() >= 5 if d else None,
        "customer_id": cust, "product_id": pid, "order_qty": qty,
        "unit_price": unit, "line_amount": amount, "currency": CURRENCY,
        "date_was_reformatted": fmt not in (None, ISO_KEY),
        "customer_id_was_invalid": cust_invalid, "customer_unresolved": cust_unresolved,
        "product_unresolved": prod_unresolved, "price_missing": price_missing,
    })


# ------------------------------------------------------------------ dedup
def dedup_latest(rows: list[dict], key_fn) -> tuple[list[dict], int]:
    """Dedup determinista: por clave gana mayor (_ingest_ts, _source_file). Devuelve (ganadores, n_removidos)."""
    best: dict = {}
    for r in rows:
        k = key_fn(r)
        rank = (r.get("_ingest_ts") or "", r.get("_source_file") or "")
        if k not in best or rank > best[k][0]:
            best[k] = (rank, r)
    return [v[1] for v in best.values()], len(rows) - len(best)
