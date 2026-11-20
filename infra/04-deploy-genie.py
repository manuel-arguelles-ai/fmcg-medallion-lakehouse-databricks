"""Crea o actualiza (idempotente) el espacio Genie 'FMCG Sales Genie' sobre fmcg_lakehouse.gold.

Fuente de verdad: genie/fmcg_sales_genie.json (formato legible). Este script lo convierte al
`serialized_space` de la API de Genie (/api/2.0/genie/spaces), busca un espacio con el mismo titulo
y lo actualiza (PATCH) o lo crea (POST). Usa el CLI de Databricks ya autenticado (nunca hace login).

  export DATABRICKS_HOST=https://<workspace> DATABRICKS_AUTH_TYPE=azure-cli
  python3 infra/04-deploy-genie.py --warehouse <warehouse-id> [--dry-run] [--print]
  python3 infra/04-deploy-genie.py --warehouse <warehouse-id> --ask "¿Cuáles son los ingresos totales?"

--ask abre una conversacion, espera la respuesta e imprime el SQL generado y el resultado.
Los id del serialized_space son md5 del contenido: el mismo JSON produce el mismo payload.
"""
import argparse
import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_JSON = ROOT / "genie" / "fmcg_sales_genie.json"


def api(method: str, path: str, body: dict | None = None) -> dict:
    cmd = ["databricks", "api", method, path]
    if body is not None:
        cmd += ["--json", json.dumps(body, ensure_ascii=False)]
    for attempt in range(3):  # reintenta cortes de red transitorios
        out = subprocess.run(cmd, capture_output=True, text=True)
        if out.returncode == 0:
            return json.loads(out.stdout or "{}")
        err = (out.stderr or out.stdout).strip()
        if "timeout" not in err.lower() or attempt == 2:
            raise RuntimeError(f"{method.upper()} {path}: {err[:800]}")
        time.sleep(5)
    return {}


def hid(*parts: str) -> str:
    return hashlib.md5("|".join(parts).encode()).hexdigest()


def serialize(spec: dict) -> dict:
    tables = []
    for t in sorted(spec["tables"], key=lambda x: x["identifier"]):
        e = {"identifier": t["identifier"]}
        if t.get("description"):
            e["description"] = [t["description"]]
        cols = t.get("columns") or {}
        if cols:
            e["column_configs"] = [{"column_name": c, "description": [d]} for c, d in sorted(cols.items())]
        tables.append(e)
    questions = sorted(({"id": hid("q", q), "question": [q]} for q in spec["sample_questions"]),
                       key=lambda x: x["id"])
    text = "\n".join(f"- {i}" for i in spec["instructions"])
    examples = sorted(({"id": hid("sql", x["question"]), "question": [x["question"]], "sql": [x["sql"]]}
                       for x in spec.get("example_sqls", [])), key=lambda x: x["id"])
    out = {
        "version": 1,
        "config": {"sample_questions": questions},
        "data_sources": {"tables": tables},
        "instructions": {"text_instructions": [{"id": hid("ti", text), "content": [text]}]},
    }
    if examples:
        out["instructions"]["example_question_sqls"] = examples
    return out


def find(title: str) -> dict | None:
    token = None
    while True:
        r = api("get", "/api/2.0/genie/spaces" + (f"?page_token={token}" if token else ""))
        for s in r.get("spaces", []):
            if s.get("title") == title:
                return s
        token = r.get("next_page_token")
        if not token:
            return None


def ask(space_id: str, question: str) -> dict:
    r = api("post", f"/api/2.0/genie/spaces/{space_id}/start-conversation", {"content": question})
    conv, msg = r["conversation_id"], r["message_id"]
    path = f"/api/2.0/genie/spaces/{space_id}/conversations/{conv}/messages/{msg}"
    for _ in range(90):
        m = api("get", path)
        if m.get("status") in ("COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"):
            break
        time.sleep(3)
    result = {"status": m.get("status"), "conversation_id": conv, "message_id": msg, "text": None,
              "sql": None, "columns": None, "rows": None, "error": m.get("error")}
    for att in m.get("attachments") or []:
        if att.get("text"):
            result["text"] = att["text"].get("content")
        if att.get("query"):
            result["sql"] = att["query"].get("query")
            result["text"] = result["text"] or att["query"].get("description")
            qr = api("get", f"{path}/attachments/{att['attachment_id']}/query-result")
            st = qr.get("statement_response", {})
            result["columns"] = [c["name"] for c in st.get("manifest", {}).get("schema", {}).get("columns", [])]
            result["rows"] = st.get("result", {}).get("data_array")
    return result


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--warehouse", required=True)
    ap.add_argument("--json", default=str(DEFAULT_JSON))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--print", action="store_true", help="imprime el serialized_space")
    ap.add_argument("--ask", action="append", help="pregunta a enviar tras desplegar (repetible)")
    ap.add_argument("--skip-deploy", action="store_true", help="solo pregunta (requiere que exista)")
    a = ap.parse_args()

    spec = json.loads(Path(a.json).read_text(encoding="utf-8"))
    ser = serialize(spec)
    if a.print:
        print(json.dumps(ser, ensure_ascii=False, indent=2))
    existing = find(spec["title"])

    if a.skip_deploy:
        if not existing:
            print("no existe el espacio")
            return 1
        sid = existing["space_id"]
    else:
        body = {"title": spec["title"], "description": spec["description"], "warehouse_id": a.warehouse,
                "serialized_space": json.dumps(ser, ensure_ascii=False)}
        action = f"PATCH {existing['space_id']}" if existing else "POST"
        if a.dry_run:
            print(f"[dry-run] {action}")
            return 0
        if existing:
            r = api("patch", f"/api/2.0/genie/spaces/{existing['space_id']}", body)
        else:
            r = api("post", "/api/2.0/genie/spaces", body)
        sid = r.get("space_id") or existing["space_id"]
        print(f"{action} -> space_id={sid}")

    import os
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    print(f"url: {host}/genie/rooms/{sid}")

    for q in a.ask or []:
        res = ask(sid, q)
        print(json.dumps({"question": q, **res}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
