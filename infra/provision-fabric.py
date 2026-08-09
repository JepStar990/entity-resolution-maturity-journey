#!/usr/bin/env python3
"""
Microsoft Fabric Workspace Provisioning Script

Creates and configures a complete Fabric workspace for the entity
resolution maturity pipeline using the Fabric REST API.

Creates:
- Fabric Workspace
- Lakehouse (Bronze, Silver, Gold via folders or separate lakehouses)
- Spark Environment with required libraries
- Notebooks (one per phase + orchestrator)
- Data Pipelines for orchestration
- Shortcuts between medallion layers

Prerequisites:
- Azure CLI logged in (`az login`)
- Fabric capacity provisioned (via Bicep or Azure Portal)
- Contributor/Admin role on the capacity

Usage:
    python infra/provision-fabric.py --workspace-name entity-resolution-prod \\
        --capacity-name fabric-er-prod --subscription-id <guid> \\
        --resource-group <name> --location eastus2
"""

import argparse
import json
import os
import sys
import time
import urllib.request
import urllib.error
from typing import Dict, List, Optional


def get_access_token() -> str:
    """Get an Azure AD access token for Fabric API via Azure CLI."""
    import subprocess
    result = subprocess.run(
        ["az", "account", "get-access-token", "--resource",
         "https://api.fabric.microsoft.com",
         "--query", "accessToken", "-o", "tsv"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to get access token. Run 'az login' first.\n{result.stderr}")
    return result.stdout.strip()


def fabric_request(
    method: str,
    url: str,
    token: str,
    body: Optional[Dict] = None,
    timeout: int = 60,
) -> Optional[Dict]:
    """Make a request to the Fabric REST API. Returns None on non-retryable errors."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    data = json.dumps(body).encode("utf-8") if body else None

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            content = response.read().decode("utf-8")
            return json.loads(content) if content else {}
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8") if e.fp else str(e)
        error_data = json.loads(error_body) if error_body else {}
        error_code = error_data.get("errorCode", "")
        if error_code in ("ItemDisplayNameAlreadyInUse", "WorkspaceDisplayNameAlreadyInUse"):
            print(f"    Already exists, skipping.")
            return None
        print(f"API Error [{e.code}]: {error_body}", file=sys.stderr)
        return None


def wait_for_long_running_operation(
    token: str,
    operation_url: str,
    max_wait_seconds: int = 300,
) -> dict:
    """Poll a long-running operation until completion."""
    deadline = time.time() + max_wait_seconds
    while time.time() < deadline:
        status = fabric_request("GET", operation_url, token)
        if status.get("status") in ("Succeeded", "Failed", "Cancelled"):
            return status
        time.sleep(5)
    raise TimeoutError(f"Operation timed out after {max_wait_seconds}s")


class FabricProvisioner:
    """Provision a complete Fabric workspace for the ER pipeline."""

    def __init__(self, token: str, subscription_id: str, resource_group: str):
        self.token = token
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.base_url = "https://api.fabric.microsoft.com/v1"

    # ------------------------------------------------------------------
    # Workspace
    # ------------------------------------------------------------------

    def create_workspace(
        self,
        name: str,
        capacity_id: str,
        description: str = "Entity Resolution Maturity Journey Pipeline",
    ) -> dict:
        """Create a Fabric workspace."""
        print(f"Creating workspace: {name}...")
        body = {
            "displayName": name,
            "description": description,
            "capacityId": capacity_id,
        }
        result = fabric_request("POST", f"{self.base_url}/workspaces", self.token, body)
        if result:
            print(f"  Workspace created: {result.get('id')}")
        return result if result else {}

    def get_workspace(self, workspace_id: str) -> dict:
        """Get workspace details."""
        return fabric_request("GET", f"{self.base_url}/workspaces/{workspace_id}", self.token)

    # ------------------------------------------------------------------
    # Lakehouse
    # ------------------------------------------------------------------

    def create_lakehouse(
        self,
        workspace_id: str,
        name: str,
        description: str = "",
    ) -> dict:
        """Create a Lakehouse in a workspace."""
        print(f"  Creating Lakehouse: {name}...")
        body = {
            "displayName": name,
            "description": description,
        }
        result = fabric_request(
            "POST",
            f"{self.base_url}/workspaces/{workspace_id}/lakehouses",
            self.token,
            body,
        )
        if result:
            print(f"    Lakehouse created: {result.get('id')}")
        return result if result else {}

    # ------------------------------------------------------------------
    # Notebooks
    # ------------------------------------------------------------------

    def create_notebook(
        self,
        workspace_id: str,
        name: str,
        lakehouse_id: str,
        notebook_content: str,
    ) -> dict:
        """Create or import a notebook."""
        print(f"  Creating notebook: {name}...")

        # Base64 encode the notebook content for API
        import base64
        content_b64 = base64.b64encode(notebook_content.encode("utf-8")).decode("utf-8")

        body = {
            "displayName": name,
            "definition": {
                "format": "ipynb",
                "parts": [
                    {
                        "path": "notebookContent.ipynb",
                        "payload": content_b64,
                        "payloadType": "InlineBase64",
                    }
                ],
            },
            "lakehouseId": lakehouse_id,
        }
        result = fabric_request(
            "POST",
            f"{self.base_url}/workspaces/{workspace_id}/notebooks",
            self.token,
            body,
        )
        if result:
            print(f"    Notebook created: {result.get('id')}")
        return result if result else {}

    # ------------------------------------------------------------------
    # Spark Environment
    # ------------------------------------------------------------------

    def create_spark_environment(
        self,
        workspace_id: str,
        name: str,
        runtime_version: str = "1.3",
    ) -> dict:
        """Create a Fabric Spark environment with custom libraries."""
        print(f"  Creating Spark environment: {name}...")

        body = {
            "displayName": name,
            "runtimeVersion": runtime_version,
            "driverCores": 4,
            "driverMemory": "28g",
            "executorCores": 4,
            "executorMemory": "28g",
            "dynamicExecutorAllocation": {
                "enabled": True,
                "minExecutors": 1,
                "maxExecutors": 10,
            },
            "sparkProperties": {
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.sql.shuffle.partitions": "200",
            },
            "publicLibraries": [
                "great-expectations==1.0.0",
                "xgboost==2.1.0",
                "sentence-transformers==2.7.0",
                "faiss-cpu==1.8.0",
                "jellyfish==1.0.4",
                "litellm==1.40.0",
                "langchain==0.3.0",
                "fastapi==0.115.0",
                "uvicorn==0.30.0",
                "azure-eventhub==5.12.0",
            ],
        }
        result = fabric_request(
            "POST",
            f"{self.base_url}/workspaces/{workspace_id}/environments",
            self.token,
            body,
        )
        if result:
            print(f"    Environment created: {result.get('id')}")
        return result if result else {}

    # ------------------------------------------------------------------
    # Data Pipeline
    # ------------------------------------------------------------------

    def create_data_pipeline(
        self,
        workspace_id: str,
        name: str,
        notebook_ids: Dict[str, str],
    ) -> dict:
        """Create a Fabric Data Pipeline that orchestrates all phase notebooks."""
        print(f"  Creating Data Pipeline: {name}...")

        activities = []
        for i in range(1, 16):
            notebook_ref = notebook_ids.get(f"phase_{i:02d}")
            if not notebook_ref:
                continue
            activity = {
                "name": f"Phase-{i:02d}",
                "type": "TridentNotebook",
                "dependsOn": [] if i == 1 else [
                    {"activity": f"Phase-{i-1:02d}"}
                ],
                "typeProperties": {
                    "notebookId": notebook_ref,
                    "sparkPool": "StarterPool",
                },
            }
            activities.append(activity)

        body = {
            "displayName": name,
            "activities": activities,
        }
        result = fabric_request(
            "POST",
            f"{self.base_url}/workspaces/{workspace_id}/dataPipelines",
            self.token,
            body,
        )
        if result:
            print(f"    Pipeline created: {result.get('id')}")
        return result if result else {}

    # ------------------------------------------------------------------
    # OneLake Shortcuts
    # ------------------------------------------------------------------

    def create_shortcut(
        self,
        workspace_id: str,
        lakehouse_id: str,
        shortcut_name: str,
        target_path: str,
    ) -> dict:
        """Create a OneLake shortcut to reference data across layers."""
        body = {
            "path": "Tables",
            "name": shortcut_name,
            "target": {
                "oneLake": {
                    "workspaceId": workspace_id,
                    "itemId": lakehouse_id,
                    "path": target_path,
                },
            },
        }
        result = fabric_request(
            "POST",
            f"{self.base_url}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/shortcuts",
            self.token,
            body,
        )
        return result


# ---------------------------------------------------------------------------
# Main provisioning flow
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Provision a Microsoft Fabric workspace for the ER pipeline"
    )
    parser.add_argument("--workspace-name", required=True,
                        help="Name for the Fabric workspace")
    parser.add_argument("--capacity-name", required=True,
                        help="Name of the Fabric capacity to assign")
    parser.add_argument("--capacity-id", default=None,
                        help="Fabric capacity UUID (if using trial/managed capacity)")
    parser.add_argument("--subscription-id", required=True,
                        help="Azure subscription ID")
    parser.add_argument("--resource-group", required=True,
                        help="Azure resource group name")
    parser.add_argument("--location", default="eastus2",
                        help="Azure region (default: eastus2)")
    parser.add_argument("--environment", default="dev",
                        choices=["dev", "staging", "prod"],
                        help="Environment type (default: dev)")
    parser.add_argument("--notebooks-dir", default="examples/notebooks",
                        help="Path to notebook files")

    args = parser.parse_args()

    print("=" * 60)
    print("Microsoft Fabric Workspace Provisioning")
    print("Entity Resolution Maturity Journey")
    print("=" * 60)

    # Get access token
    print("\nAuthenticating...")
    token = get_access_token()
    print("  Authenticated")

    if args.capacity_id:
        capacity_id = args.capacity_id
    else:
        capacity_id = (
            f"/subscriptions/{args.subscription_id}/resourceGroups/{args.resource_group}"
            f"/providers/Microsoft.Fabric/capacities/{args.capacity_name}"
        )

    provisioner = FabricProvisioner(token, args.subscription_id, args.resource_group)

    # Step 1: Create or find workspace
    ws = provisioner.create_workspace(
        name=args.workspace_name,
        capacity_id=capacity_id,
        description="Entity Resolution Maturity Journey - 15-phase pipeline",
    )
    if ws.get("id"):
        workspace_id = ws["id"]
    else:
        # Look up existing workspace by name
        print(f"  Looking up existing workspace: {args.workspace_name}...")
        list_result = fabric_request("GET",
            f"{provisioner.base_url}/workspaces", token)
        existing = [w for w in list_result.get("value", [])
                    if w.get("displayName") == args.workspace_name]
        if existing:
            workspace_id = existing[0]["id"]
            print(f"  Found existing workspace: {workspace_id}")
        else:
            raise RuntimeError(f"Failed to create or find workspace: {args.workspace_name}")

    # Step 2: Create lakehouses for medallion layers
    print("\nCreating medallion lakehouses...")
    bronze_lh = provisioner.create_lakehouse(
        workspace_id, "er_bronze",
        "Raw ingested data, schema validation, quality checks"
    )
    silver_lh = provisioner.create_lakehouse(
        workspace_id, "er_silver",
        "Standardized, enriched, deduplicated entity data"
    )
    gold_lh = provisioner.create_lakehouse(
        workspace_id, "er_gold",
        "Golden records, match scores, stewardship, MDM distribution"
    )
    gold_lh_id = gold_lh.get("id")

    # Step 3: Create Spark environment
    print("\nConfiguring Spark environment...")
    env = provisioner.create_spark_environment(
        workspace_id,
        f"er-spark-env-{args.environment}",
        runtime_version="1.3",
    )

    # Step 4: Upload notebooks
    print("\nImporting notebooks...")
    notebook_ids = {}
    phase_notebooks = [
        ("01", "01-Ingestion"),
        ("02", "02-Schema-Validation"),
        ("03", "03-Data-Quality"),
        ("04", "04-Standardization"),
        ("05", "05-Enrichment"),
        ("06", "06-Exact-Dedup"),
        ("07", "07-Fuzzy-Matching"),
        ("08", "08-Record-Blocking"),
        ("09", "09-Feature-Engineering"),
        ("10", "10-Probabilistic-Matching"),
        ("11", "11-LLM-Semantic"),
        ("12", "12-Embedding-Matching"),
        ("13", "13-Golden-Records"),
        ("14", "14-Stewardship"),
        ("15", "15-MDM-Distribution"),
    ]

    # Upload master orchestrator notebook
    orchestrator_path = os.path.join(args.notebooks_dir, "00-master-orchestrator.ipynb")
    if os.path.exists(orchestrator_path):
        with open(orchestrator_path, "r") as f:
            notebook_content = f.read()
        provisioner.create_notebook(
            workspace_id, "00-Master-Orchestrator",
            gold_lh_id, notebook_content,
        )

    # Create individual phase notebooks (placeholder if files don't exist)
    for phase_num, phase_name in phase_notebooks:
        notebook_path = os.path.join(args.notebooks_dir, f"{phase_num}-{phase_name}.ipynb")
        if os.path.exists(notebook_path):
            with open(notebook_path, "r") as f:
                content = f.read()
        else:
            content = _generate_phase_notebook(phase_num, phase_name, args.environment)

        nb = provisioner.create_notebook(
            workspace_id, f"{phase_num}-{phase_name}",
            gold_lh_id, content,
        )
        if nb:
            notebook_ids[f"phase_{phase_num}"] = nb.get("id")

    # Step 5: Create orchestration pipeline
    print("\nCreating orchestration pipeline...")
    provisioner.create_data_pipeline(
        workspace_id,
        f"er-pipeline-{args.environment}",
        notebook_ids,
    )

    # Step 6: Print summary
    print("\n" + "=" * 60)
    print("PROVISIONING COMPLETE")
    print("=" * 60)
    print(f"""
Workspace: {args.workspace_name}
Workspace ID: {workspace_id}
Bronze Lakehouse: {bronze_lh.get('id')}
Silver Lakehouse: {silver_lh.get('id')}
Gold Lakehouse: {gold_lh.get('id')}
Spark Environment: {env.get('id')}

Next steps:
1. Open https://app.fabric.microsoft.com/
2. Navigate to workspace: {args.workspace_name}
3. Upload config: examples/config/pipeline-config.yaml
4. Run notebook: 00-Master-Orchestrator
""")


def _generate_phase_notebook(phase_num: str, phase_name: str, env: str) -> str:
    """Generate a placeholder phase notebook if no file exists."""
    phase_module = f"phase_{phase_num}"

    name_map = {
        "01": "ingestion", "02": "schema_validation", "03": "data_quality",
        "04": "standardization", "05": "enrichment", "06": "exact_dedup",
        "07": "fuzzy_matching", "08": "record_blocking", "09": "feature_engineering",
        "10": "probabilistic_matching", "11": "semantic_matching_llm",
        "12": "embedding_matching", "13": "golden_record",
        "14": "stewardship", "15": "mdm_distribution",
    }
    module_name = name_map.get(phase_num, phase_name.lower().replace("-", "_"))

    return json.dumps({
        "cells": [
            {
                "cell_type": "markdown",
                "source": [
                    f"# Phase {phase_num}: {phase_name}\n",
                    f"Auto-generated notebook for the {phase_name} phase.\n",
                ],
            },
            {
                "cell_type": "code",
                "source": [
                    f"import {phase_module} as phase\n",
                    "from utils.spark_session import get_or_create_spark_session\n",
                    "spark = get_or_create_spark_session('{phase_name}')\n",
                    f"# Load data from previous phase and run\n",
                ],
            },
        ],
        "metadata": {
            "kernelspec": {"display_name": "PySpark", "language": "python", "name": "python3"},
        },
        "nbformat": 4,
        "nbformat_minor": 4,
    }, indent=2)


if __name__ == "__main__":
    main()
