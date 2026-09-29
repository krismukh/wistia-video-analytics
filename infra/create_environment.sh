#!/usr/bin/env bash
# =============================================================================
# Wistia Video Analytics : Phase 1 environment creation
#
# Creates every Azure resource the pipeline needs, in one resource group, so
# that the environment is reproducible from the repository.
#
# Run in Azure Cloud Shell (Bash):
#   bash create_environment.sh
# The script is idempotent: rerunning it skips resources that already exist.
#
# The script prompts for two secrets and never echoes or stores them:
#   1. the Synapse SQL administrator password
#   2. the Wistia API token (placed in Key Vault as secret wistia-api-token)
#
# Resources (West US 3, same region as the previous project):
#   rg-wistia            resource group
#   stwistiakcm          ADLS Gen2 storage (containers bronze, silver, gold, synapse)
#   kv-wistia-kcm        Key Vault (RBAC mode) holding the Wistia API token
#   syn-wistia-kcm       Synapse workspace with Spark pool sparkpool1
#   adf-wistia-kcm       Data Factory
# Role assignments:
#   you, ADF identity, Synapse identity  -> Storage Blob Data Contributor on stwistiakcm
#   you                                   -> Key Vault Secrets Officer on kv-wistia-kcm
#   Synapse identity, Data Factory identity -> Key Vault Secrets User on kv-wistia-kcm
#   ADF identity                          -> Synapse Contributor in syn-wistia-kcm
# =============================================================================
set -euo pipefail

SUBSCRIPTION="Azure subscription 1"
LOCATION="westus3"
RG="rg-wistia"
STORAGE="stwistiakcm"
KEYVAULT="kv-wistia-kcm"
SYNAPSE="syn-wistia-kcm"
SPARKPOOL="sparkpool1"
ADF="adf-wistia-kcm"
SQL_ADMIN="sqladminuser"
TOKEN_SECRET_NAME="wistia-api-token"

echo "== Subscription and resource providers"
az account set --subscription "$SUBSCRIPTION"
for ns in Microsoft.Storage Microsoft.KeyVault Microsoft.Synapse Microsoft.DataFactory; do
  az provider register --namespace "$ns" --wait
done
SUB_ID=$(az account show --query id -o tsv)
ME=$(az ad signed-in-user show --query id -o tsv)
echo "   subscription $SUB_ID, signed-in user object id $ME"

read -r -s -p "Synapse SQL admin password (min 8 chars, upper, lower, digit, symbol): " SQL_PASSWORD; echo
read -r -s -p "Wistia API token: " WISTIA_TOKEN; echo
[ -n "$SQL_PASSWORD" ] && [ -n "$WISTIA_TOKEN" ] || { echo "Both secrets are required."; exit 1; }

echo "== Resource group"
az group create --name "$RG" --location "$LOCATION" -o none

echo "== Storage account (ADLS Gen2)"
az storage account create \
  --name "$STORAGE" --resource-group "$RG" --location "$LOCATION" \
  --sku Standard_LRS --kind StorageV2 --hns true \
  --allow-blob-public-access false --min-tls-version TLS1_2 -o none
STORAGE_ID=$(az storage account show --name "$STORAGE" --resource-group "$RG" --query id -o tsv)

echo "== Storage Blob Data Contributor for the signed-in user (needed to create containers)"
az role assignment create --assignee-object-id "$ME" --assignee-principal-type User \
  --role "Storage Blob Data Contributor" --scope "$STORAGE_ID" -o none || true
echo "   waiting 60 s for role propagation"; sleep 60

for c in bronze silver gold synapse; do
  az storage fs create --name "$c" --account-name "$STORAGE" --auth-mode login -o none || true
done
echo "   containers: bronze silver gold synapse"

echo "== Key Vault (RBAC authorization)"
if ! az keyvault show --name "$KEYVAULT" --resource-group "$RG" -o none 2>/dev/null; then
  az keyvault create --name "$KEYVAULT" --resource-group "$RG" --location "$LOCATION" \
    --enable-rbac-authorization true -o none
fi
KV_ID=$(az keyvault show --name "$KEYVAULT" --resource-group "$RG" --query id -o tsv)
az role assignment create --assignee-object-id "$ME" --assignee-principal-type User \
  --role "Key Vault Secrets Officer" --scope "$KV_ID" -o none || true
echo "   waiting 60 s for role propagation"; sleep 60
az keyvault secret set --vault-name "$KEYVAULT" --name "$TOKEN_SECRET_NAME" --value "$WISTIA_TOKEN" -o none
unset WISTIA_TOKEN
echo "   secret $TOKEN_SECRET_NAME stored"

echo "== Synapse workspace"
if ! az synapse workspace show --name "$SYNAPSE" --resource-group "$RG" -o none 2>/dev/null; then
  az synapse workspace create \
    --name "$SYNAPSE" --resource-group "$RG" --location "$LOCATION" \
    --storage-account "$STORAGE" --file-system synapse \
    --sql-admin-login-user "$SQL_ADMIN" --sql-admin-login-password "$SQL_PASSWORD" -o none
fi
unset SQL_PASSWORD
SYN_MI=$(az synapse workspace show --name "$SYNAPSE" --resource-group "$RG" --query identity.principalId -o tsv)
echo "   workspace identity $SYN_MI"

echo "== Synapse firewall: Azure services and this shell's public IP"
az synapse workspace firewall-rule create --name AllowAllWindowsAzureIps \
  --workspace-name "$SYNAPSE" --resource-group "$RG" \
  --start-ip-address 0.0.0.0 --end-ip-address 0.0.0.0 -o none || true
SHELL_IP=$(curl -s https://ifconfig.me || true)
if [ -n "$SHELL_IP" ]; then
  az synapse workspace firewall-rule create --name CloudShell \
    --workspace-name "$SYNAPSE" --resource-group "$RG" \
    --start-ip-address "$SHELL_IP" --end-ip-address "$SHELL_IP" -o none || true
  echo "   added $SHELL_IP (add your workstation IP in Synapse Studio later)"
fi

echo "== Spark pool"
if ! az synapse spark pool show --name "$SPARKPOOL" --workspace-name "$SYNAPSE" --resource-group "$RG" -o none 2>/dev/null; then
  az synapse spark pool create \
    --name "$SPARKPOOL" --workspace-name "$SYNAPSE" --resource-group "$RG" \
    --spark-version 3.4 --node-size Small --node-count 3 \
    --enable-auto-pause true --delay 15 \
    --enable-auto-scale false -o none
fi
echo "   $SPARKPOOL: Spark 3.4, Small, 3 nodes, auto-pause 15 min"

echo "== Data Factory"
az extension add --name datafactory --upgrade -o none 2>/dev/null || true
if ! az datafactory show --factory-name "$ADF" --resource-group "$RG" -o none 2>/dev/null; then
  az datafactory create --factory-name "$ADF" --resource-group "$RG" --location "$LOCATION" -o none
fi
ADF_MI=$(az datafactory show --factory-name "$ADF" --resource-group "$RG" --query identity.principalId -o tsv)
echo "   factory identity $ADF_MI"

echo "== Role assignments for managed identities"
for P in "$SYN_MI" "$ADF_MI"; do
  az role assignment create --assignee-object-id "$P" --assignee-principal-type ServicePrincipal \
    --role "Storage Blob Data Contributor" --scope "$STORAGE_ID" -o none || true
done
# Both identities read the Wistia token: the Synapse identity for interactive runs, and the Data Factory
# identity when a pipeline runs a notebook (the notebook then executes as the caller, i.e. ADF).
for P in "$SYN_MI" "$ADF_MI"; do
  az role assignment create --assignee-object-id "$P" --assignee-principal-type ServicePrincipal \
    --role "Key Vault Secrets User" --scope "$KV_ID" -o none || true
done
echo "   waiting 60 s for role propagation"; sleep 60

echo "== Synapse Contributor for the Data Factory identity (lets ADF run notebooks)"
az synapse role assignment create --workspace-name "$SYNAPSE" \
  --role "Synapse Contributor" --assignee-object-id "$ADF_MI" --assignee-principal-type ServicePrincipal -o none || \
  echo "   (if this failed with a firewall error, rerun this one command after adding your IP in Synapse Studio)"

echo
echo "== Done. Summary"
echo "   Resource group : $RG ($LOCATION)"
echo "   Storage        : $STORAGE  (abfss://<container>@$STORAGE.dfs.core.windows.net/)"
echo "   Key Vault      : $KEYVAULT  secret $TOKEN_SECRET_NAME"
echo "   Synapse        : $SYNAPSE  pool $SPARKPOOL  SQL admin $SQL_ADMIN"
echo "   Data Factory   : $ADF"
echo "   Synapse Studio : https://web.azuresynapse.net/?workspace=%2Fsubscriptions%2F$SUB_ID%2FresourceGroups%2F$RG%2Fproviders%2FMicrosoft.Synapse%2Fworkspaces%2F$SYNAPSE"
