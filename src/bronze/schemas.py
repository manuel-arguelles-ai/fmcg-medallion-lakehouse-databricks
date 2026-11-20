"""Esquemas DECLARADOS de Bronze: todas las columnas de negocio son STRING (nada de inferSchema).

Se generan desde el registro; los metadatos de trazabilidad tienen tipos propios.
"""
from __future__ import annotations

from .registry import SourceSpec

RESCUED = "_rescued_data"

# (nombre, tipo SQL) de los metadatos que Bronze agrega a cada fila.
METADATA_COLUMNS = [
    ("_company", "STRING"), ("_source_system", "STRING"), ("_source", "STRING"),
    ("_source_file", "STRING"), ("_file_size", "BIGINT"), ("_file_modification_time", "TIMESTAMP"),
    ("_ingest_ts", "TIMESTAMP"), ("_ingest_date", "DATE"), ("_batch_id", "STRING"),
    ("_load_type", "STRING"),
]

TABLE_PROPERTIES = {
    "delta.enableChangeDataFeed": "true",
    "delta.logRetentionDuration": "30 days",
    "delta.deletedFileRetentionDuration": "7 days",
}


def ddl_schema_string(spec: SourceSpec) -> str:
    """Esquema para `cloudFiles`/`.schema()` en formato DDL: solo columnas del archivo, todas STRING."""
    return ", ".join(f"`{c}` STRING" for c in spec.columns)


def spark_schema(spec: SourceSpec):
    """StructType equivalente (import perezoso: pyspark solo existe en el cluster)."""
    from pyspark.sql.types import StringType, StructField, StructType
    return StructType([StructField(c, StringType(), True) for c in spec.columns])


def table_columns(spec: SourceSpec) -> list[str]:
    """Orden fisico de columnas de la tabla Bronze."""
    return [*spec.columns, RESCUED, *(n for n, _ in METADATA_COLUMNS)]


def table_ddl(spec: SourceSpec, catalog: str, table: str | None = None, staging: bool = False) -> str:
    """CREATE TABLE IF NOT EXISTS para la tabla Bronze, o la de paso (`staging=True`, sin CDF/retencion)."""
    cols = [f"`{c}` STRING" for c in spec.columns] + [f"`{RESCUED}` STRING"] + \
           [f"`{n}` {t}" for n, t in METADATA_COLUMNS]
    name = f"{catalog}.bronze.{table or (spec.staging_table if staging else spec.table)}"
    ddl = f"CREATE TABLE IF NOT EXISTS {name} (\n  " + ",\n  ".join(cols) + "\n)"
    if not staging:
        ddl += " TBLPROPERTIES (" + ", ".join(f"'{k}' = '{v}'" for k, v in TABLE_PROPERTIES.items()) + ")"
    return ddl
