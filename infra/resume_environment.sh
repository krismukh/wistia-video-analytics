#!/usr/bin/env bash
# Resume Phase 1 from the Synapse workspace step (storage, containers, and
# Key Vault with the token already exist). Prompts only for the SQL password.
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

az account set --subscription "$SUBSCRIPTION"
SUB_ID=$(az account show --query id -o tsv)
STORAGE_ID=$(az storage account show --name "$STORAGE" --resource-group "$RG" --query id -o tsv)
KV_ID=$(az keyvault show --name "$KEYVAULT" --resource-group "$RG" --query id -o tsv)

read -r -s -p "Synapse SQL admin password: " SQL_PASSWORD; echo

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

echo "== Synapse firewall"
az synapse workspace firewall-rule create --name AllowAllWindowsAzureIps \
  --workspace-name "$SYNAPSE" --resource-group "$RG" \
  --start-ip-address 0.0.0.0 --end-ip-address 0.0.0.0 -o none || true
SHELL_IP=$(curl -s https://ifconfig.me || true)
if [ -n "$SHELL_IP" ]; then
  az synapse workspace firewall-rule create --name CloudShell \
    --workspace-name "$SYNAPSE" --resource-group "$RG" \
    --start-ip-address "$SHELL_IP" --end-ip-address "$SHELL_IP" -o none || true
  echo "   added $SHELL_IP"
fi

echo "== Spark pool"
if ! az synapse spark pool show --name "$SPARKPOOL" --workspace-name "$SYNAPSE" --resource-group "$RG" -o none 2>/dev/null; then
  az synapse spark pool create \
    --name "$SPARKPOOL" --workspace-name "$SYNAPSE" --resource-group "$RG" \
    --spark-version 3.4 --node-size Small --node-count 3 \
    --enable-auto-pause true --delay 15 --enable-auto-scale false -o none
fi
echo "   $SPARKPOOL ready"

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

echo "== Synapse Contributor for the Data Factory identity"
az synapse role assignment create --workspace-name "$SYNAPSE" \
  --role "Synapse Contributor" --assignee-object-id "$ADF_MI" --assignee-principal-type ServicePrincipal -o none || \
  echo "   (firewall error? add your IP in Synapse Studio and rerun this one command)"

echo
echo "== Done. Summary"
echo "   Resource group : $RG ($LOCATION)"
echo "   Storage        : $STORAGE"
echo "   Key Vault      : $KEYVAULT  secret $TOKEN_SECRET_NAME"
echo "   Synapse        : $SYNAPSE  pool $SPARKPOOL  SQL admin $SQL_ADMIN  identity $SYN_MI"
echo "   Data Factory   : $ADF  identity $ADF_MI"
echo "   Synapse Studio : https://web.azuresynapse.net/?workspace=%2Fsubscriptions%2F$SUB_ID%2FresourceGroups%2F$RG%2Fproviders%2FMicrosoft.Synapse%2Fworkspaces%2F$SYNAPSE"
