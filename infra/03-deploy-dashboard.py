"""Crea o actualiza (idempotente) el dashboard AI/BI 'FMCG Sales Insights' y lo publica.

Fuente de verdad: dashboards/fmcg_sales_insights.lvdash.json (Lakeview serializado).
Busca un dashboard ACTIVO con el mismo nombre en la carpeta destino: si existe lo actualiza (PATCH),
si no lo crea (POST). Luego lo publica con credenciales embebidas sobre el warehouse indicado.
Usa el CLI de Databricks ya autenticado (no maneja credenciales; nunca hace login).

  export DATABRICKS_HOST=https://<workspace> DATABRICKS_AUTH_TYPE=azure-cli
  python3 infra/03-deploy-dashboard.py --warehouse <warehouse-id> [--verify] [--no-publish] [--dry-run]
  python3 infra/03-deploy-dashboard.py --warehouse <warehouse-id> --export   # baja el borrador al JSON

--verify ejecuta el SQL de cada dataset por la Statement Execution API (parametros con su valor por
defecto) y falla si alguno no corre.
"""
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_JSON = ROOT / "dashboards" / "fmcg_sales_insights.lvdash.json"
NAME = "FMCG Sales Insights"


def api(method: str, path: str, body: dict | None = None) -> dict:
    cmd = ["databricks", "api", method, path]
    if body is not None:
        cmd += ["--json", json.dumps(body)]
    out = subprocess.run(cmd, capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError(f"{method.upper()} {path}: {(out.stderr or out.stdout).strip()[:800]}")
    return json.loads(out.stdout or "{}")


def me() -> str:
    return api("get", "/api/2.0/preview/scim/v2/Me")["userName"]


def find(name: str, parent: str) -> dict | None:
    token = None
    while True:
        path = "/api/2.0/lakeview/dashboards?page_size=100" + (f"&page_token={token}" if token else "")
        r = api("get", path)
        for d in r.get("dashboards", []):
            if d.get("display_name") != name or d.get("lifecycle_state", "ACTIVE") != "ACTIVE":
                continue
            # el listado no trae la ruta: se consulta el dashboard para comparar la carpeta
            full = api("get", f"/api/2.0/lakeview/dashboards/{d['dashboard_id']}")
            if full.get("parent_path", "").removeprefix("/Workspace") == parent.removeprefix("/Workspace"):
                return full
        token = r.get("next_page_token")
        if not token:
            return None


def run_sql(statement: str, warehouse: str, params: list[dict]) -> int:
    # se envuelve en COUNT(*) para no traer el resultado completo
    body = {"warehouse_id": warehouse, "statement": f"SELECT COUNT(*) FROM (\n{statement}\n)", "wait_timeout": "50s",
            "on_wait_timeout": "CONTINUE", "parameters": params}
    r = api("post", "/api/2.0/sql/statements", body)
    while r["status"]["state"] in ("PENDING", "RUNNING"):
        time.sleep(2)
        r = api("get", f"/api/2.0/sql/statements/{r['statement_id']}")
    if r["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(json.dumps(r["status"])[:800])
    return int(r["result"]["data_array"][0][0])


def verify(dash: dict, warehouse: str) -> bool:
    ok = True
    for d in dash["datasets"]:
        sql = "".join(d["queryLines"])
        params = []
        for p in d.get("parameters", []):
            vals = p.get("defaultSelection", {}).get("values", {}).get("values", [])
            # sin valor por defecto = NULL (el SQL lo trata como "sin filtro")
            params.append({"name": p["keyword"], "type": p["dataType"],
                           **({"value": vals[0]["value"]} if vals else {})})
        try:
            n = run_sql(sql, warehouse, params)
            print(f"  OK   {d['name']:<16} {n:>6} filas")
        except RuntimeError as e:
            ok = False
            print(f"  FAIL {d['name']:<16} {e}")
    return ok


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--json", default=str(DEFAULT_JSON))
    ap.add_argument("--name", default=NAME)
    ap.add_argument("--parent", help="carpeta del workspace (por defecto /Users/<usuario>)")
    ap.add_argument("--verify", action="store_true")
    ap.add_argument("--no-publish", action="store_true")
    ap.add_argument("--export", action="store_true", help="descarga el borrador actual al JSON y termina")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    parent = a.parent or f"/Users/{me()}"
    existing = find(a.name, parent)

    if a.export:
        if not existing:
            print(f"no existe '{a.name}' en {parent}")
            return 1
        d = api("get", f"/api/2.0/lakeview/dashboards/{existing['dashboard_id']}")
        Path(a.json).write_text(json.dumps(json.loads(d["serialized_dashboard"]), ensure_ascii=False, indent=2) + "\n",
                                encoding="utf-8")
        print(f"exportado a {a.json}")
        return 0

    dash = json.loads(Path(a.json).read_text(encoding="utf-8"))
    if a.verify:
        print("Verificando datasets:")
        if not verify(dash, a.warehouse):
            return 1

    body = {"display_name": a.name, "warehouse_id": a.warehouse,
            "serialized_dashboard": json.dumps(dash, ensure_ascii=False)}
    action = f"PATCH {existing['dashboard_id']}" if existing else f"POST en {parent}"
    if a.dry_run:
        print(f"[dry-run] {action}; publicar={not a.no_publish}")
        return 0
    if existing:
        d = api("patch", f"/api/2.0/lakeview/dashboards/{existing['dashboard_id']}", body)
    else:
        d = api("post", "/api/2.0/lakeview/dashboards", {**body, "parent_path": parent})
    did = d["dashboard_id"]
    print(f"{action} -> dashboard_id={did}")

    if not a.no_publish:
        api("post", f"/api/2.0/lakeview/dashboards/{did}/published",
            {"warehouse_id": a.warehouse, "embed_credentials": True})
        print("publicado")
    print(f"borrador:  {host}/sql/dashboardsv3/{did}")
    print(f"publicado: {host}/dashboardsv3/{did}/published")
    return 0


if __name__ == "__main__":
    sys.exit(main())
