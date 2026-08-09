# Microsoft Fabric Setup Guide

A practical guide to setting up Microsoft Fabric for the entity resolution maturity pipeline.

---

## Overview

Microsoft Fabric provides a unified SaaS analytics platform with all the services needed to run the full 15-phase maturity model:

| Fabric Workload | Phases | Role |
|-----------------|--------|------|
| **Data Factory** | 1 | Ingestion pipelines from source systems |
| **Data Engineering** | 1–10, 13 | PySpark notebooks, Delta Lake tables, lakehouse management |
| **Data Science** | 9–12, 14 | ML experiments, model training, embeddings, LLM calls |
| **Real-Time Intelligence** | 15 | Event streams for CDC and MDM distribution |
| **Power BI** | 3, 14 | Quality dashboards, stewardship metrics |

---

## Step-by-Step Setup

### 1. Create a Fabric Workspace

A workspace is the container for all Fabric resources (Lakehouses, Notebooks, Pipelines).

1. Navigate to [app.fabric.microsoft.com](https://app.fabric.microsoft.com/)
2. Select **Workspaces** > **New Workspace**
3. Name: `entity-resolution-prod`
4. Assign the workspace to a Fabric capacity (F2 minimum)

### 2. Provision a Lakehouse

The Lakehouse combines OneLake storage with a managed Spark runtime.

1. Inside the workspace, select **New** > **Lakehouse**
2. Name: `er_lakehouse`
3. Once created, the Lakehouse appears with two default folders:
   - `Tables/` — for managed Delta Lake tables
   - `Files/` — for unstructured files and raw data

### 3. Configure Spark Runtime

1. Go to **Workspace Settings** > **Data Engineering/Science** > **Spark Settings**
2. Select **Fabric Runtime 1.3** (Spark 3.5)
3. Set **Node Size**: Small (4 vCores, 32 GB) for development; Medium (8 vCores, 64 GB) for production
4. Enable **Autoscale**: Min 1, Max based on workload
5. Set **Auto-termination**: 15 minutes (development), 5 minutes (production batch jobs)

### 4. Create the Medallion Layers

Follow the medallion architecture directly in Fabric:

```sql
-- Run in a Fabric Notebook or SQL Endpoint
CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;
```

Alternatively, create separate Lakehouses for each layer and use **OneLake shortcuts** to reference data across them:

```
Workspace: er-bronze (Lakehouse: bronze_lakehouse)
Workspace: er-silver (Lakehouse: silver_lakehouse, shortcut → er-bronze/Tables)
Workspace: er-gold  (Lakehouse: gold_lakehouse, shortcut → er-silver/Tables)
```

This approach enforces access control per layer while avoiding data duplication.

### 5. Import Notebooks and Set Up Code

1. Create a Notebook for each phase: `01-Ingestion`, `02-Schema-Validation`, etc.
2. Attach each notebook to the appropriate Lakehouse.
3. Configure the Spark session at the top of each notebook:

```python
# %%configure magic (first cell in each notebook)
# Required for Fabric Runtime
from pyspark.sql import SparkSession

# OneLake path pattern:
# abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables/<table>
BRONZE_PATH = "Tables/bronze/"
SILVER_PATH = "Tables/silver/"
GOLD_PATH = "Tables/gold/"

spark = SparkSession.builder \
    .appName("EntityResolution") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
```

**Note:** Fabric Runtime automatically includes Delta Lake and common ML libraries. No additional JARs or `--packages` flags needed.

### 6. Set Up MLflow

Fabric natively supports MLflow — no installation required:

```python
import mlflow

# Experiments are auto-created in the workspace
mlflow.set_experiment("/Workspace/Users/your-user/entity-resolution-matching")

with mlflow.start_run():
    mlflow.log_params(best_params)
    mlflow.log_metrics({"f1": f1_score, "precision": precision, "recall": recall})
    mlflow.spark.log_model(model, "matching_model")
```

Models appear in the **Workspace > Data Science > ML Models** view.

### 7. Schedule Pipelines

Use **Fabric Data Pipelines** (Data Factory) instead of external orchestrators:

1. Select **New** > **Data Pipeline**
2. Add a **Notebook** activity
3. Select the phase notebook to execute
4. Configure retry policy (3 attempts, 30-second delay)
5. Set schedule: **Schedule** > **New Trigger** (e.g., daily at 02:00 UTC)

For complex DAGs with dependencies (Phase 4 depends on Phase 3 completion):

```
Data Pipeline: nightly-er-pipeline
├── Activity: 01-Ingestion
├── Activity: 02-Schema-Validation (depends on 01)
├── Activity: 03-Data-Quality (depends on 02)
├── ...
└── Activity: 13-Golden-Records (depends on 12)
```

### 8. Monitor and Alert

1. **Spark metrics**: Workspace > Monitor Hub > Historical Runs
2. **Pipeline status**: Data Pipeline > Run History
3. **Power BI dashboards**: Connect to `er_lakehouse` via Direct Lake mode for live quality metrics, match rates, and stewardship SLAs
4. **Capacity metrics**: Fabric Capacity Metrics app (included with F-SKU) to track CU consumption and prevent throttling

---

## Library Management

### Built-in Libraries (Fabric Runtime 1.3)

| Library | Version | Used In |
|---------|---------|---------|
| PySpark | 3.5.x | All phases |
| Delta Lake | 3.x | All phases |
| scikit-learn | 1.5+ | Phases 9, 10 |
| MLflow | 2.x | Phases 10, 14 |

### Custom Libraries

For libraries not included in the runtime, add them via **Fabric Environment**:

1. Create an Environment from the workspace
2. Add libraries under **Public Libraries**: `great-expectations`, `xgboost`, `sentence-transformers`, `faiss-cpu`, `langchain`, `litellm`
3. Publish the environment
4. Attach notebooks to the environment

This ensures consistent library versions across all notebooks and pipeline runs.

---

## Cost Management

| Environment | F-SKU | Approx. Monthly | Notes |
|-------------|-------|-----------------|-------|
| Development | F2 | ~$263 | Pause nights/weekends |
| Small production | F8 | ~$1,051 | 8 CU, suitable for < 10M records |
| Medium production | F16 | ~$2,102 | 16 CU, suitable for 10M–100M records |
| Enterprise | F64+ | ~$5,003+ | 64 CU, reserved pricing |

- **Pause capacity** outside working hours from Fabric Admin Portal
- **Reserved pricing** (1-year) saves ~40% vs. pay-as-you-go
- **Monitor CU consumption** in Fabric Capacity Metrics to right-size your SKU

---

## Next Steps

- [Architecture Overview](../architecture.md)
- [Phase 1: Basic Data Ingestion](../phases/phase-01-basic-data-ingestion.md)
- [Technology Stack](./tech-stack.md)
- [Fabric vs Databricks Comparison](./fabric-vs-databricks.md)
