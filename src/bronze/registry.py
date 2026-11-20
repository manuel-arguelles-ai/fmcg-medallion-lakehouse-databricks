"""Carga el registro de fuentes (registry.yaml) y lo expande a una entrada por compania x fuente."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

REGISTRY_PATH = Path(__file__).with_name("registry.yaml")


@dataclass(frozen=True)
class SourceSpec:
    source_system: str
    company: str
    source: str
    format: str
    delimiter: str
    frequency: str
    load_type: str  # full | incremental
    columns: tuple[str, ...]
    landing_retention_days: int
    bronze_retention: str
    landing_template: str  # con {catalog}

    @property
    def table(self) -> str:
        return f"{self.company}_{self.source}"

    @property
    def staging_table(self) -> str:
        return f"staging_{self.company}_{self.source}"

    @property
    def file_pattern(self) -> str:
        """Regex del nombre de archivo, p. ej. orders_madre_20250101.csv."""
        return rf"^{self.source}_{self.company}_\d{{8}}\.csv$"

    def landing_path(self, catalog: str) -> str:
        return self.landing_template.format(catalog=catalog) + f"/{self.company}/{self.source}"


def load_registry(path: Path | str = REGISTRY_PATH) -> list[SourceSpec]:
    cfg = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    d = cfg["defaults"]
    specs = []
    for company, c in cfg["companies"].items():
        for source, s in cfg["sources"].items():
            specs.append(SourceSpec(
                source_system=c["source_system"], company=company, source=source,
                format=s.get("format", d["format"]), delimiter=s.get("delimiter", d["delimiter"]),
                frequency=s["frequency"], load_type=s["load_type"], columns=tuple(s["columns"]),
                landing_retention_days=int(s.get("landing_retention_days", d["landing_retention_days"])),
                bronze_retention=s.get("bronze_retention", d["bronze_retention"]),
                landing_template=cfg["landing_root"]))
    return specs


def get_spec(company: str, source: str, specs: list[SourceSpec] | None = None) -> SourceSpec:
    for s in specs or load_registry():
        if s.company == company and s.source == source:
            return s
    raise KeyError(f"fuente no registrada: {company}/{source}")


def _q(v: str) -> str:
    return "'" + v.replace("'", "''") + "'"


def seed_sql(catalog: str, specs: list[SourceSpec] | None = None) -> str:
    """SQL idempotente (MERGE) que siembra ops.source_registry desde el registro."""
    rows = []
    for s in specs or load_registry():
        cols = "array(" + ", ".join(_q(c) for c in s.columns) + ")"
        rows.append(
            f"  ({_q(s.source_system)}, {_q(s.company)}, {_q(s.source)}, {_q(s.format)}, {_q(s.frequency)}, "
            f"{_q(s.landing_path(catalog))}, {cols}, {_q(s.load_type)}, "
            f"{s.landing_retention_days}, {_q(s.bronze_retention)})")
    values = ",\n".join(rows)
    return (
        f"MERGE INTO {catalog}.ops.source_registry AS t\nUSING (\n  SELECT * FROM VALUES\n{values}\n"
        "  AS v(source_system, company, source, format, frequency, landing_path, expected_columns, "
        "load_type, landing_retention_days, bronze_retention)\n) AS s\n"
        "ON t.company = s.company AND t.source = s.source\n"
        "WHEN MATCHED THEN UPDATE SET t.source_system = s.source_system, t.format = s.format, "
        "t.frequency = s.frequency, t.landing_path = s.landing_path, t.expected_columns = s.expected_columns, "
        "t.load_type = s.load_type, t.landing_retention_days = s.landing_retention_days, "
        "t.bronze_retention = s.bronze_retention, t.updated_at = current_timestamp()\n"
        "WHEN NOT MATCHED THEN INSERT (source_system, company, source, format, frequency, landing_path, "
        "expected_columns, load_type, landing_retention_days, bronze_retention, updated_at) VALUES "
        "(s.source_system, s.company, s.source, s.format, s.frequency, s.landing_path, s.expected_columns, "
        "s.load_type, s.landing_retention_days, s.bronze_retention, current_timestamp())")


OPS_DDL = {
    "source_registry": """CREATE TABLE IF NOT EXISTS {catalog}.ops.source_registry (
  source_system STRING, company STRING, source STRING, format STRING, frequency STRING,
  landing_path STRING, expected_columns ARRAY<STRING>, load_type STRING,
  landing_retention_days INT, bronze_retention STRING, updated_at TIMESTAMP)""",
    "landing_rejects": """CREATE TABLE IF NOT EXISTS {catalog}.ops.landing_rejects (
  rejected_at TIMESTAMP, batch_id STRING, company STRING, source STRING,
  file_path STRING, file_size BIGINT, rule_id STRING, reason STRING)""",
    "bronze_runs": """CREATE TABLE IF NOT EXISTS {catalog}.ops.bronze_runs (
  batch_id STRING, company STRING, source STRING, target_table STRING, load_type STRING,
  started_at TIMESTAMP, finished_at TIMESTAMP, status STRING, files_new INT, files_rejected INT,
  rows_read BIGINT, rows_written BIGINT, rows_rescued BIGINT, rows_rejected BIGINT, error STRING)""",
}


def ops_ddl(catalog: str) -> list[str]:
    return [v.format(catalog=catalog) for v in OPS_DDL.values()]


SEED_HEADER = (
    "-- NO EJECUTADO — requiere confirmacion de Manu.\n"
    "-- Siembra de ops.source_registry. GENERADO desde src/bronze/registry.yaml; no editar a mano:\n"
    "--   cd src && python -m bronze.registry --seed fmcg_lakehouse > ../infra/02b-seed-source-registry.sql\n"
    "-- (un test verifica que este archivo coincide con el registro). Ejecutar DESPUES de 02-unity-catalog.sql.\n"
)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Imprime el SQL de siembra de ops.source_registry")
    ap.add_argument("--seed", metavar="CATALOG", required=True)
    print(SEED_HEADER + seed_sql(ap.parse_args().seed) + ";")
