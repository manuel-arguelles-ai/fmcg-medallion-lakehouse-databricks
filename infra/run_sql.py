"""Ejecuta un archivo .sql sentencia por sentencia en un SQL Warehouse de Databricks.

Usa el CLI de Databricks ya autenticado (no maneja credenciales) y la Statement Execution API.
Se detiene en la primera sentencia que falle.

  DATABRICKS_HOST=https://<workspace> DATABRICKS_AUTH_TYPE=azure-cli \
  python3 infra/run_sql.py <archivo.sql> --warehouse <id> [--dry-run]
"""
import argparse
import json
import re
import subprocess
import sys
import time


def split_statements(text: str) -> list[str]:
    """Quita comentarios de linea (--) y separa por ';' (el SQL del repo no usa ';' dentro de literales)."""
    lines = [re.sub(r"^\s*--.*$", "", ln) for ln in text.splitlines()]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def api(method: str, path: str, body: dict | None = None) -> dict:
    cmd = ["databricks", "api", method, path]
    if body is not None:
        cmd += ["--json", json.dumps(body)]
    out = subprocess.run(cmd, capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError((out.stderr or out.stdout).strip()[:600])
    return json.loads(out.stdout)


def run(statement: str, warehouse: str) -> dict:
    r = api("post", "/api/2.0/sql/statements", {
        "warehouse_id": warehouse, "statement": statement,
        "wait_timeout": "50s", "on_wait_timeout": "CONTINUE"})
    sid = r["statement_id"]
    while r["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(3)
        r = api("get", f"/api/2.0/sql/statements/{sid}")
    return r


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("file")
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    stmts = split_statements(open(a.file, encoding="utf-8").read())
    print(f"{len(stmts)} sentencias en {a.file}")
    for i, s in enumerate(stmts, 1):
        head = " ".join(s.split())[:90]
        if a.dry_run:
            print(f"[{i}] (dry-run) {head}")
            continue
        r = run(s, a.warehouse)
        st = r["status"]
        if st["state"] != "SUCCEEDED":
            print(f"[{i}] FALLO {head}\n    {st.get('error', {}).get('message', st)}")
            return 1
        print(f"[{i}] ok  {head}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
