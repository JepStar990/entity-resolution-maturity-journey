// Entity Resolution Maturity Journey - Azure Fabric Infrastructure
//
// Provisions the Azure resources needed to run the 15-phase pipeline:
// - Fabric Capacity (F2 minimum)
// - Log Analytics Workspace for monitoring
// - Key Vault for secrets (LLM API keys, connection strings)
//
// Prerequisites:
// - User must be a native tenant member (not guest/#EXT# account)
// - User must have a Fabric license (free trial at app.fabric.microsoft.com)
// - Fabric capacity names: lowercase alphanumeric only (no hyphens)
// - Fabric capacity quota may need to be requested for paid SKUs
//
// After this Bicep deployment, run provision-fabric.py to create the
// Fabric workspace, lakehouse, notebooks, and pipelines via REST API.

@description('Azure region for all resources')
param location string = resourceGroup().location

@description('Environment name (dev, staging, prod)')
param environment string = 'dev'

@description('Fabric capacity SKU (F2, F4, F8, F16, F32, F64, F128, F256, F512, F1024, F2048)')
param fabricSku string = 'F2'

@description('Admin user principal IDs for Fabric capacity (array of AAD object IDs)')
param fabricAdmins array = []

@description('Tags applied to all resources')
param tags object = {
  project: 'entity-resolution-maturity-journey'
  environment: environment
  managedBy: 'bicep'
}

var resourceNames = {
  fabricCapacity: 'fabricer${toLower(environment)}'
  logAnalytics: 'log-er-${environment}'
  keyVault: 'kv-er-${environment}-${uniqueString(resourceGroup().id)}'
  appInsights: 'appi-er-${environment}'
}

// ---------------------------------------------------------------------------
// Log Analytics Workspace for monitoring and diagnostics
// ---------------------------------------------------------------------------
resource logAnalytics 'Microsoft.OperationalInsights/workspaces@2023-09-01' = {
  name: resourceNames.logAnalytics
  location: location
  tags: tags
  properties: {
    sku: {
      name: 'PerGB2018'
    }
    retentionInDays: 30
  }
}

// ---------------------------------------------------------------------------
// Application Insights for pipeline telemetry
// ---------------------------------------------------------------------------
resource appInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: resourceNames.appInsights
  location: location
  tags: tags
  kind: 'web'
  properties: {
    Application_Type: 'other'
    WorkspaceResourceId: logAnalytics.id
  }
}

// ---------------------------------------------------------------------------
// Key Vault for secrets management
// ---------------------------------------------------------------------------
resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: resourceNames.keyVault
  location: location
  tags: tags
  properties: {
    sku: {
      family: 'A'
      name: 'standard'
    }
    tenantId: subscription().tenantId
    enableRbacAuthorization: true
    enableSoftDelete: true
    softDeleteRetentionInDays: 90
  }
}

// ---------------------------------------------------------------------------
// Fabric Capacity
// ---------------------------------------------------------------------------
resource fabricCapacity 'Microsoft.Fabric/capacities@2023-11-01' = {
  name: resourceNames.fabricCapacity
  location: location
  tags: tags
  sku: {
    name: fabricSku
    tier: 'Fabric'
  }
  properties: {
    administration: {
      members: fabricAdmins
    }
  }
}

// ---------------------------------------------------------------------------
// Outputs
// ---------------------------------------------------------------------------
output fabricCapacityId string = fabricCapacity.id
output logAnalyticsWorkspaceId string = logAnalytics.id
output keyVaultUri string = keyVault.properties.vaultUri
output appInsightsInstrumentationKey string = appInsights.properties.InstrumentationKey
output appInsightsConnectionString string = appInsights.properties.ConnectionString
