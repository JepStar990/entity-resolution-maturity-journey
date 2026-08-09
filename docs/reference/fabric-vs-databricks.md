# Fabric vs Databricks Comparison

A detailed comparison of Microsoft Fabric and Databricks for entity resolution workloads, to help teams evaluate which platform fits their context.

---

## Platform Philosophy

| Dimension | Microsoft Fabric | Databricks |
|-----------|-----------------|------------|
| **Type** | SaaS (fully managed) | PaaS/SaaS hybrid |
| **Primary interface** | Browser-based workspace with notebooks, pipelines, Power BI | Browser-based workspace with notebooks, SQL, workflows |
| **Storage** | OneLake (SaaS data lake, Delta native) | Cloud object store + Delta Lake (customer-managed) |
| **Spark** | Managed Fabric Runtime (Microsoft-curated) | Databricks Runtime (Databricks-curated) |
| **Billing** | Capacity-based (F-SKUs, predictable monthly) | Consumption-based (DBUs + cloud infra, variable) |

---

## Entity Resolution Pipeline Mapping

| Phase | Fabric Service | Databricks Service |
|-------|---------------|-------------------|
| 1 — Ingestion | Data Factory (Copy Data) | Auto Loader / COPY INTO |
| 2 — Schema Validation | Fabric Notebook (PySpark) | Databricks Notebook (PySpark) |
| 3 — Data Quality | Fabric Notebook + Great Expectations | Databricks Notebook + Great Expectations |
| 4 — Standardization | Fabric Notebook (PySpark) | Databricks Notebook (PySpark) |
| 5 — Enrichment | Fabric Notebook + Shortcuts | Databricks Notebook + Unity Catalog |
| 6 — Exact Dedup | Fabric Notebook (Delta Lake) | Databricks Notebook (Delta Lake) |
| 7 — Fuzzy Matching | Fabric Notebook | Databricks Notebook |
| 8 — Blocking | Fabric Notebook | Databricks Notebook |
| 9 — Feature Engineering | Fabric Notebook + MLflow | Databricks Notebook + Feature Store |
| 10 — ML Matching | Fabric Notebook + MLflow | Databricks Notebook + MLflow + AutoML |
| 11 — LLM Semantic | Fabric Notebook + LiteLLM | Databricks Notebook + Model Serving |
| 12 — Embeddings | Fabric Notebook + FAISS | Databricks Notebook + Vector Search |
| 13 — Golden Records | Fabric Notebook (Delta MERGE) | Databricks Notebook (Delta MERGE) |
| 14 — Stewardship | Fabric Notebook + Custom UI | Databricks Notebook + Custom UI |
| 15 — MDM Distribution | Real-Time Intelligence + Power BI | Delta Sharing + REST APIs |

---

## Strengths by Scenario

### Fabric Wins When

- **Power BI is the primary consumer.** Direct Lake mode enables live query on Delta tables without data movement or refresh schedules.
- **Cost predictability matters.** Capacity-based pricing means you know your monthly bill upfront.
- **Microsoft 365 ecosystem.** Teams already using Azure, Power BI, Purview, and Entra ID get seamless integration.
- **Operational simplicity is prioritized.** No JAR management, no cloud infrastructure provisioning, no separate service wiring.
- **Development can pause.** Capacity can be paused to $0 — ideal for dev/test environments that don't run 24/7.
- **Data sharing across workspaces.** OneLake shortcuts share Delta tables without duplication.

### Databricks Wins When

- **Extreme Spark performance.** Databricks Runtime includes proprietary optimizations (Photon engine, vectorized execution) that Fabric's open-source Spark doesn't match.
- **ML/AI depth.** AutoML, Feature Store, Model Serving endpoints, Unity Catalog lineage — more mature ML ecosystem.
- **Multi-cloud strategy.** Databricks runs identical on AWS, Azure, and GCP. No vendor lock-in.
- **Very large clusters.** Proven at petabyte scale with 1000+ node clusters.
- **Open-source leverage.** Feature Store, Unity Catalog, and Delta Sharing are open-sourced — no platform lock-in for those components.
- **Granular cost control.** Pay-per-second DBU billing rewards disciplined cluster management.

---

## Cost Comparison (Entity Resolution Workload)

Estimate for a mid-size pipeline: 10M records/day, 8 phases running daily, plus ML training and inference:

| Item | Fabric (F16, PAYG) | Databricks (AWS Premium) |
|------|-------------------|--------------------------|
| Batch ETL (Phases 1–8) | Included in capacity | ~$1,200/mo (Jobs Compute) |
| ML training (Phase 10) | Included in capacity | ~$500/mo |
| LLM calls (Phase 11) | External API cost | External API cost |
| Embeddings (Phase 12) | Included in capacity | ~$300/mo |
| SQL queries (Dashboards) | Included in capacity | ~$800/mo (SQL Serverless) |
| Power BI Pro licenses | ~$10/user/mo | ~$10/user/mo + SQL warehouse |
| **Monthly total** | **~$2,102 + API costs** | **~$2,800 + API costs + cloud infra** |

**Note:** Fabric costs are fixed (F16 PAYG). Databricks costs are estimates that vary with workload patterns. At high utilization (>80%), Fabric is typically cheaper. At low utilization (<30%), Databricks pay-per-second can be cheaper.

---

## Migration Considerations

### From Databricks to Fabric

| Component | Migration Effort | Notes |
|-----------|-----------------|-------|
| PySpark code | Low | Spark API is identical; `spark.read` paths change to OneLake |
| Delta Lake tables | Low | Copy Parquet/Delta files to OneLake Files; register as tables |
| MLflow models | Low | Export from Databricks MLflow; import to Fabric MLflow |
| Workflows | Medium | Rewrite Databricks Workflows as Fabric Data Pipelines |
| Unity Catalog | Medium | Map to Fabric workspace permissions + Purview |
| Auto Loader | Medium | Replace with Fabric Data Factory pipelines |

### Key Differences to Expect

1. **OneLake paths** use `abfss://` instead of `dbfs:/` or `s3://`.
2. **Fabric Runtime** may have slightly older library versions than Databricks Runtime.
3. **No Photon engine** — compute-heavy operations may run slower on equivalent node sizes.
4. **Notebook %%configure magic** instead of cluster configuration UI.
5. **Workspace instead of Repos** — Git integration exists but differs in workflow.

---

## Recommendation

**Choose Fabric** if you are: in the Microsoft Azure ecosystem, value cost predictability, need tight Power BI integration, and want minimal operational overhead.

**Choose Databricks** if you need: maximum Spark performance, advanced ML/AI features (Feature Store, AutoML, Model Serving), multi-cloud portability, or are already running Databricks in production.

**This guide targets Microsoft Fabric** as the reference platform. The 15-phase maturity model concepts apply equally to both platforms.

---

## Next Steps

- [Fabric Setup Guide](./fabric-setup.md)
- [Technology Stack](./tech-stack.md)
- [Decision Log: Platform Choice](./decision-log.md#adr-008-microsoft-fabric-as-primary-platform)
