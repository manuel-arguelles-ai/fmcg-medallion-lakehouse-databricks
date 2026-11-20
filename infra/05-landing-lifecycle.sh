#!/usr/bin/env bash
# Retencion de landing a 90 dias (politica de ciclo de vida del storage). Idempotente (create sobrescribe la politica).
# Bronze es el historico: tras 90 dias no se puede reprocesar desde landing.  Uso: bash infra/05-landing-lifecycle.sh [--show]
set -euo pipefail
SA=stfmcglakehousedevcus001; RG=rg-fmcg-lakehouse-dev-cus-001
if [[ "${1:-}" == "--show" ]]; then
  az storage account management-policy show --account-name "$SA" -g "$RG" -o json; exit 0
fi
az storage account management-policy create --account-name "$SA" -g "$RG" -o none --policy '{"rules":[{"name":"landing-90d","enabled":true,"type":"Lifecycle","definition":{"filters":{"blobTypes":["blockBlob"],"prefixMatch":["landing/"]},"actions":{"baseBlob":{"delete":{"daysAfterModificationGreaterThan":90}}}}}]}'
