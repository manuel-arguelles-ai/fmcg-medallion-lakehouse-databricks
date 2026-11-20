"""Validaciones minimas PRE-carga de archivos de landing (funciones puras, sin Spark).

Un archivo que falla alguna regla NO entra a Bronze y se describe para `ops.landing_rejects`.
"""
from __future__ import annotations

import csv
import datetime as dt
import io
import os
import re
from dataclasses import dataclass

from .registry import SourceSpec

R_EMPTY = "R01_EMPTY_FILE"
R_EXT = "R02_EXTENSION"
R_NAME = "R03_FILE_NAME"
R_HEADER = "R04_HEADER"
R_FOLDER = "R05_INGEST_DATE_FOLDER"
R_NO_ROWS = "R06_NO_DATA_ROWS"

_FOLDER_RE = re.compile(r"(?:^|/)ingest_date=([^/]+)(?=/)")


@dataclass(frozen=True)
class Reject:
    """Descripcion de un rechazo (una fila de ops.landing_rejects, sin batch/timestamp)."""
    file_path: str
    file_size: int
    rule_id: str
    reason: str


def normalize_path(path: str) -> str:
    """Quita el esquema `dbfs:` que Spark antepone a rutas de Volume."""
    return path[5:] if path.startswith("dbfs:") else path


def check_size(size: int) -> str | None:
    return None if size > 0 else "archivo vacio (0 bytes)"


def check_extension(path: str) -> str | None:
    return None if path.endswith(".csv") else "la extension no es .csv"


def check_name(path: str, pattern: str) -> str | None:
    name = path.rsplit("/", 1)[-1]
    return None if re.match(pattern, name) else f"el nombre '{name}' no cumple el patron {pattern}"


def check_ingest_folder(path: str) -> str | None:
    """Exactamente una carpeta ingest_date=YYYY-MM-DD con fecha de calendario valida."""
    found = _FOLDER_RE.findall(normalize_path(path))
    if len(found) != 1:
        return "falta la carpeta ingest_date=YYYY-MM-DD" if not found else "hay varias carpetas ingest_date"
    try:
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", found[0]):
            raise ValueError
        dt.date.fromisoformat(found[0])
    except ValueError:
        return f"carpeta ingest_date invalida: {found[0]!r}"
    return None


def parse_header(line: str, delimiter: str = ",") -> list[str]:
    """Columnas del encabezado, sin BOM ni espacios laterales."""
    row = next(csv.reader(io.StringIO(line.lstrip("﻿")), delimiter=delimiter), [])
    return [c.strip() for c in row]


def check_header(line: str | None, expected: tuple[str, ...] | list[str], delimiter: str = ",") -> str | None:
    if line is None:
        return "sin encabezado"
    got = parse_header(line, delimiter)
    return None if got == list(expected) else f"encabezado {got} != esperado {list(expected)}"


def validate_file(path: str, size: int, head_lines: list[str], spec: SourceSpec) -> list[Reject]:
    """Aplica las 6 reglas. `head_lines` = primeras lineas del archivo (basta con 2)."""
    path = normalize_path(path)
    out = []

    def add(rule, reason):
        if reason:
            out.append(Reject(path, size, rule, reason))

    add(R_EMPTY, check_size(size))
    add(R_EXT, check_extension(path))
    add(R_NAME, check_name(path, spec.file_pattern))
    add(R_FOLDER, check_ingest_folder(path))
    if size > 0:
        add(R_HEADER, check_header(head_lines[0] if head_lines else None, spec.columns, spec.delimiter))
        if len(head_lines) < 2 or not head_lines[1].strip():
            add(R_NO_ROWS, "el archivo no tiene filas de datos")
    return out


def read_head(path: str, n: int = 2) -> list[str]:
    """Primeras n lineas (UTF-8). Una lectura invalida devuelve [] y la regla de encabezado la rechaza."""
    try:
        with open(path, "rb") as fh:
            return [fh.readline().decode("utf-8") for _ in range(n)]
    except (UnicodeDecodeError, OSError):
        return []


def list_files(root: str) -> list[tuple[str, int]]:
    """(ruta, tamano) de todos los archivos bajo `root` (sistema de archivos montado, p. ej. /Volumes)."""
    found = []
    for base, _dirs, files in os.walk(root):
        for f in files:
            p = os.path.join(base, f)
            found.append((p, os.path.getsize(p)))
    return sorted(found)


def validate_landing(root: str, spec: SourceSpec, skip: set[str] = frozenset()):
    """Valida los archivos de `root` que no estan en `skip` (ya cargados o ya rechazados).

    Devuelve (aceptados, rechazos) con rutas normalizadas.
    """
    ok, rejects = [], []
    for path, size in list_files(root):
        path = normalize_path(path)
        if path in skip:
            continue
        r = validate_file(path, size, read_head(path), spec)
        (rejects.extend(r) if r else ok.append(path))
    return ok, rejects
