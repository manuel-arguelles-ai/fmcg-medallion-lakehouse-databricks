#!/usr/bin/env bash
# =====================================================================================
# NO EJECUTADO — requiere confirmacion de Manu.
# Sube data/generated/ (madre/, hija/, MANIFEST.json) al contenedor `landing` de ADLS Gen2.
#   - Dry-run (no sube nada):   ./infra/03-upload-landing.sh --dry-run
#   - Subida real:              ./infra/03-upload-landing.sh
# Requisitos: sesion `az` ya iniciada por Manu (este script NUNCA hace `az login`), rol
# "Storage Blob Data Contributor" de tu usuario sobre la cuenta, y data generada:
#   (cd src && python -m datagen --out ../data/generated)
# Por defecto NO sobrescribe blobs existentes (el landing es inmutable: un archivo = una carga).
# Para sobrescribir, exportar OVERWRITE=true (cuidado: Auto Loader no recarga archivos ya vistos).
# =====================================================================================
set -euo pipefail

SA="${STORAGE_ACCOUNT:-stfmcglakehousedevcus001}"
CONTAINER="${CONTAINER:-landing}"
SRC="${SRC:-data/generated}"
OVERWRITE="${OVERWRITE:-false}"
DRY=""
[[ "${1:-}" == "--dry-run" ]] && DRY="--dryrun"

[[ -f "$SRC/MANIFEST.json" ]] || { echo "Falta $SRC/MANIFEST.json: genera los datos primero." >&2; exit 1; }
echo "Origen: $SRC  ->  https://$SA.blob.core.windows.net/$CONTAINER/  (dry-run: $([[ -n "$DRY" ]] && echo si || echo no), overwrite: $OVERWRITE)"

az storage blob upload-batch \
  --account-name "$SA" --destination "$CONTAINER" --source "$SRC" \
  --auth-mode login --overwrite "$OVERWRITE" $DRY -o none

# Verificacion (solo lectura): cuenta de blobs CSV subidos vs. archivos del MANIFEST.
if [[ -z "$DRY" ]]; then
  n_remote=$(az storage blob list --account-name "$SA" -c "$CONTAINER" --auth-mode login \
    --query "length([?ends_with(name, '.csv')])" -o tsv)
  n_local=$(python3 -c "import json;print(len(json.load(open('$SRC/MANIFEST.json'))['files']))")
  echo "CSV en landing: $n_remote | en MANIFEST: $n_local"
fi
