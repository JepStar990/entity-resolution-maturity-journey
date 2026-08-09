# Architecture

This page describes the end-to-end architecture of the entity resolution and MDM pipeline, the technology choices, and the design principles that guide the maturity model.

---

## System Context

```mermaid
--8<-- "diagrams/data-flow-architecture.mmd"
```

### Data Flows

1. **Source Systems** push or are pulled into the **Bronze layer**, where data is preserved exactly as received with metadata enrichment (load timestamp, source system, batch ID).

2. **Validation and quality gates** ensure only well-formed data progresses to **Silver**, where it is standardized, enriched, and deduplicated.

3. **Matching engines** (exact, fuzzy, ML, LLM, embeddings) operate in the **Gold layer**, producing matched entity clusters and golden records.

4. **Data stewards** review uncertain matches via a review queue; their decisions feed back into model retraining.

5. The **MDM hub** distributes trusted golden records to downstream consumers: analytics, AI applications, operational systems, and data warehouses.

---

## Medallion Architecture

The pipeline follows the **Bronze → Silver → Gold** medallion architecture pattern, implemented natively in Microsoft Fabric and popularized by Databricks.

### Bronze Layer (Phases 1–3)

| Attribute | Value |
|-----------|-------|
| **Purpose** | Preserve source data fidelity |
| **Data State** | Raw, as-received |
| **Mutability** | Append-only, immutable |
| **Retention** | Configurable (typically 30–90 days raw, then archived) |
| **Access Pattern** | Read by validation/quality jobs only |

**Contents**: Raw ingested data, validated-but-uncleaned records, quality check results.

### Silver Layer (Phases 4–8)

| Attribute | Value |
|-----------|-------|
| **Purpose** | Clean, standardize, and deduplicate |
| **Data State** | Cleansed, consistent, deduplicated |
| **Mutability** | Overwrite or merge (SCD Type 1 or 2) |
| **Retention** | Full history (until Gold confirms) |
| **Access Pattern** | Read by matching engines and analysts |

**Contents**: Standardized entities, enriched records, deduplicated datasets, blocked record groups.

### Gold Layer (Phases 9–15)

| Attribute | Value |
|-----------|-------|
| **Purpose** | Business-ready, governed, trusted |
| **Data State** | Matched entities, golden records |
| **Mutability** | Managed (merge + version history) |
| **Retention** | Permanent (compliance-grade) |
| **Access Pattern** | APIs, event streams, direct query by all consumers |

**Contents**: Feature vectors, match probabilities, golden records, stewardship audit logs.

[:material-book-open-variant: Full medallion architecture reference](reference/medallion-architecture.md)

---

## Technology Stack

| Technology | Role | Phases | Rationale |
|------------|------|--------|-----------|
| **Python 3.10+** | Primary language | All | Ecosystem depth for data, ML, and LLM |
| **PySpark 3.5+** | Distributed processing | 1–10, 13 | Horizontal scalability, DataFrame API, MLlib |
| **Delta Lake 3.x** | Storage layer | All | ACID transactions, time travel, schema enforcement |
| **Great Expectations 1.x** | Data quality | 3 | Declarative expectations, data docs, profiling |
| **scikit-learn** | ML matching | 9–10 | Feature preprocessing, baseline models |
| **XGBoost** | ML matching | 10 | State-of-the-art tabular matching |
| **MLflow** | Model registry | 10, 14 | Experiment tracking, model versioning |
| **FAISS / Milvus** | Vector database | 12 | Approximate nearest neighbor search at scale |
| **Sentence Transformers** | Embeddings | 12 | Pre-trained models for entity text encoding |
| **LangChain / LiteLLM** | LLM orchestration | 11 | Provider-agnostic LLM integration |
| **FastAPI** | MDM API | 15 | High-performance REST/GraphQL serving |

[:material-book-open-variant: Detailed technology reference](reference/tech-stack.md)

---

## Design Principles

### 1. Progressive Maturity

Each phase builds on the previous. You cannot skip from exact matching to embeddings without understanding blocking and feature engineering. The model is designed to be followed sequentially, but organizations already at a certain maturity can enter at the appropriate phase.

### 2. Technology Agnostic Where Possible

Phases describe patterns, not products. While the reference implementation uses Python + PySpark + Delta Lake, the concepts apply to any modern data stack (e.g., dbt + Snowflake, Beam + BigQuery, Spark + Iceberg).

### 3. Config-Driven Logic

Matching rules, quality thresholds, blocking keys, survivorship rules — all are externalized from code as YAML or JSON configuration. This enables business users to tune matching behavior without code changes.

### 4. Feedback Loops

Stewardship decisions (Phase 14) feed back into:
- **Model retraining** (Phase 10): Human-labeled pairs become training data.
- **Survivorship rules** (Phase 13): Override patterns refined from steward actions.
- **Quality rules** (Phase 3): Previously unknown data issues surfaced by stewards.

### 5. Observable at Every Stage

Each phase emits metrics:
- **Phase 1**: Ingestion volume, latency, source completeness.
- **Phase 3**: Pass/fail rates per expectation, data quality scores.
- **Phases 7–12**: Match rates, precision/recall, false positive rates.
- **Phase 14**: Steward resolution time, agreement rates, training data growth.

---

## Scaling Characteristics

| Data Volume | Recommended Configuration |
|-------------|--------------------------|
| < 100K records | Single-node Python (pandas, DuckDB) — Phases 1–12 without Spark |
| 100K–10M records | PySpark standalone or small cluster (4–8 workers) |
| 10M–100M records | PySpark on YARN/Kubernetes (16–64 workers). Blocking (Phase 8) is critical. |
| > 100M records | Distributed Spark + vector DB + LLM batching. Full blocking, incremental processing, partition pruning. |

---

## Next Steps

- [:material-arrow-right: Browse the 15 phases](phases/overview.md)
- [:material-arrow-right: Get started with prerequisites](getting-started.md)
- [:material-arrow-right: Review technology choices](reference/tech-stack.md)
