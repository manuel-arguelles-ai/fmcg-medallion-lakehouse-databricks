#!/usr/bin/env bash
# Registro de los comandos ejecutados el 2026-11-19 (ya aplicados; no volver a correr tal cual).
# Región: centralus. Sin NAT gateway: workspace con IPs públicas en los nodos (--enable-no-public-ip false).
set -euo pipefail
RG=rg-fmcg-lakehouse-dev-cus-001; LOC=centralus; SA=stfmcglakehousedevcus001
AC=ac-fmcg-lakehouse-dev-cus-001; DBW=dbw-fmcg-lakehouse-dev-cus-001
TAGS="project=fmcg-lakehouse env=dev"

az group create -n $RG -l $LOC --tags $TAGS -o none
az storage account create -n $SA -g $RG -l $LOC --sku Standard_LRS --kind StorageV2 --hns true \
  --min-tls-version TLS1_2 --allow-blob-public-access false --tags $TAGS -o none
for c in landing bronze silver gold uc-managed; do
  az storage container create --account-name $SA -n $c --auth-mode login -o none
done
az databricks access-connector create -g $RG -n $AC -l $LOC --identity-type SystemAssigned --tags $TAGS -o none
PRINCIPAL=$(az databricks access-connector show -g $RG -n $AC --query identity.principalId -o tsv)
az role assignment create --assignee-object-id "$PRINCIPAL" --assignee-principal-type ServicePrincipal \
  --role "Storage Blob Data Contributor" --scope "$(az storage account show -n $SA -g $RG --query id -o tsv)" -o none
az databricks workspace create -g $RG -n $DBW -l $LOC --sku premium --enable-no-public-ip false --tags $TAGS -o none
