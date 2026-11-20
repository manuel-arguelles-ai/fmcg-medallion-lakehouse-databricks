# src/

- `datagen/` — generador sintético reproducible. `cd src && python -m datagen --out ../data/generated [--small] [--seed N]`
  (`--small`: 3 meses, ~20 mil líneas, en < 1 s). Ver `python -m datagen --help`. Escribe `MANIFEST.json` con conteos exactos
  por archivo y por anomalía sembrada.
- `bronze/` — capa Bronze: `registry.yaml` (fuentes como datos), `registry.py`, `schemas.py` (todo STRING),
  `validation.py` (reglas pre-carga puras), `ingest.py` (Auto Loader; una compañía × fuente por ejecución).

Pruebas: `.venv/bin/pytest` desde la raíz del repo. Detalle en `docs/05-bronce-implementacion.md`.
