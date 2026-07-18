#!/usr/bin/env bash
#
# provision_azure.sh
# -----------------------------------------------------------------------------
# Provisions the Azure resources for the Medallion Lakehouse & Entity Resolution
# tutorial:
#   1. Resource group
#   2. Azure Databricks workspace (Premium tier - required for Unity Catalog)
#   3. ADLS Gen2 storage account (hierarchical namespace enabled)
#   4. A landing container
#   5. An Access Connector (system-assigned managed identity)
#   6. A "Storage Blob Data Contributor" role assignment for that identity
#
# The design goal is that NO secret is stored anywhere: Databricks reaches the
# lake by impersonating the access connector's managed identity, and access is a
# plain, revocable Azure RBAC assignment.
#
# Prerequisites:
#   - Azure CLI installed and logged in:  az login
#   - (older CLI only)  az extension add --name databricks
#
# When finished with the whole tutorial, tear everything down with:
#   az group delete --name rg-poc-lakehouse
# -----------------------------------------------------------------------------

set -euo pipefail

# ----- Variables (edit as needed) --------------------------------------------
LOC=australiaeast
RG=rg-poc-lakehouse
DBW=dbw-poc-lakehouse
SA=stpoclakehouse            # must be globally unique, lowercase
CONN=dbac-poc-lakehouse
CONTAINER=poc-landing

# ----- 1. Resource group -----------------------------------------------------
az group create --name "$RG" --location "$LOC"

# ----- 2. Databricks workspace (Premium for Unity Catalog) -------------------
az databricks workspace create --resource-group "$RG" --name "$DBW" \
  --location "$LOC" --sku premium

# ----- 3. ADLS Gen2 storage account (hierarchical namespace = --hns) ---------
az storage account create --resource-group "$RG" --name "$SA" \
  --location "$LOC" --sku Standard_LRS --kind StorageV2 --hns true

# ----- 4. Landing container ---------------------------------------------------
az storage container create --account-name "$SA" --name "$CONTAINER" \
  --auth-mode login

# ----- 5. Access Connector (creates a system-assigned managed identity) ------
az databricks access-connector create --resource-group "$RG" --name "$CONN" \
  --location "$LOC" --identity-type SystemAssigned

# ----- 6a. Wait until the connector's managed identity is ready --------------
echo "Waiting for the access connector's managed identity to be ready..."
PRINCIPAL_ID=""
for i in {1..12}; do
  PRINCIPAL_ID=$(az databricks access-connector show \
    --resource-group "$RG" --name "$CONN" --query identity.principalId -o tsv)
  if [ -n "$PRINCIPAL_ID" ]; then
    echo "Got principalId: $PRINCIPAL_ID"
    break
  fi
  echo "  not ready yet, retrying in 10s ($i/12)..."
  sleep 10
done

if [ -z "$PRINCIPAL_ID" ]; then
  echo "ERROR: principalId never became available. Aborting." >&2
  exit 1
fi

SA_ID=$(az storage account show --resource-group "$RG" --name "$SA" --query id -o tsv)

# ----- 6b. Grant the role (retry, in case identity replication is catching up)
echo "Assigning Storage Blob Data Contributor..."
for i in {1..6}; do
  if az role assignment create \
      --assignee-object-id "$PRINCIPAL_ID" \
      --assignee-principal-type ServicePrincipal \
      --role "Storage Blob Data Contributor" --scope "$SA_ID"; then
    echo "Role assigned."
    break
  fi
  echo "  role assignment failed (likely propagation), retrying in 15s ($i/6)..."
  sleep 15
done

# ----- Output the Resource ID you paste into Databricks ----------------------
echo ""
echo "============================================================================"
echo "Access connector Resource ID (paste into the Databricks storage credential):"
az databricks access-connector show --resource-group "$RG" --name "$CONN" \
  --query id -o tsv
echo "============================================================================"
echo ""
echo "Next steps (in the Databricks workspace UI):"
echo "  1. Catalog -> External data -> Credentials -> create an Azure Managed"
echo "     Identity credential named 'cred_poc' using the Resource ID above."
echo "  2. Catalog -> External data -> External locations -> create"
echo "     'loc_poc_landing' -> abfss://${CONTAINER}@${SA}.dfs.core.windows.net/"
echo "     -> credential cred_poc -> Test connection."
echo "  3. Run the companion notebook to create the catalog, schemas, and volume."
