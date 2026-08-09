# Decision Log

An architecture decision record (ADR) log for key design choices in the entity resolution maturity pipeline.

---

## ADR-001: PySpark over Pandas for Entity Resolution

**Status**: Accepted
**Date**: 2026-05-31

### Context

Entity resolution workloads range from thousands to hundreds of millions of records. The processing framework must scale from a single machine to a distributed cluster without code changes.

### Decision

Use PySpark as the primary processing framework.

### Rationale

- **Horizontal scalability**: PySpark scales from laptop to 1000-node cluster. Pandas requires a complete rewrite for distributed processing.
- **DataFrame API**: Expressive, SQL-compatible, and optimized by Catalyst optimizer.
- **MLlib integration**: Native distributed ML training (Phase 10) without moving data.
- **Streaming**: Structured Streaming for real-time ingestion (Phase 1) and CDC (Phase 15) from the same codebase.
- **Ecosystem**: Delta Lake, GraphFrames, and Great Expectations all have first-class PySpark support.

### Consequences

- Higher operational complexity than single-node solutions for small datasets.
- JVM dependency (Java 11+ required).
- Python UDFs have performance overhead — critical path algorithms may need Scala or Pandas UDF optimization.

---

## ADR-002: Delta Lake over Parquet for Storage

**Status**: Accepted
**Date**: 2026-05-31

### Context

The pipeline requires ACID guarantees for quality gates (Phase 3), exact deduplication (Phase 6), golden record merges (Phase 13), and audit logging (Phase 14).

### Decision

Use Delta Lake as the storage format for all layers (Bronze, Silver, Gold).

### Rationale

- **ACID transactions**: Atomic writes prevent partial results. A quality gate failure rolls back cleanly.
- **Time travel**: Query data as-of any point. Essential for audit, reprocessing, and debugging.
- **Schema enforcement**: Prevents schema drift from corrupting downstream tables.
- **Efficient merges**: `MERGE` operation for upserts (SCD, dedup, golden record creation).
- **Open-source**: Apache 2.0 license, compatible with the rest of the stack.

### Consequences

- Requires Delta Lake JAR in the Spark classpath.
- `OPTIMIZE` and `VACUUM` operations needed for performance and storage management.
- Not all Spark versions support the latest Delta features — version compatibility must be managed.

---

## ADR-003: Great Expectations over Deequ for Data Quality

**Status**: Accepted
**Date**: 2026-05-31

### Context

Phase 3 requires a declarative data quality framework. Two options: Great Expectations (Python-native) and Deequ (Spark-native, Scala/Java).

### Decision

Use Great Expectations as the primary data quality framework, with Deequ as a fallback for very large Spark-only deployments.

### Rationale

- **Python ecosystem alignment**: The entire tech stack is Python. Deequ requires Scala/Java for expectation definition.
- **Data Docs**: Auto-generated, shareable HTML documentation of data quality — built into GE, requires custom work in Deequ.
- **Spark backend**: GE supports a Spark backend for distributed validation.
- **Community**: Larger Python community, more tutorials, more integrations.

### Consequences

- GE's Spark integration is newer and less battle-tested than its Pandas backend.
- For very large datasets (>100M records per validation), Deequ may be faster due to native Spark optimization.
- Teams already invested in the Deequ/Scala ecosystem should consider Deequ instead.

---

## ADR-004: XGBoost over Deep Learning for Probabilistic Matching

**Status**: Accepted
**Date**: 2026-05-31

### Context

Phase 10 requires a supervised ML model for predicting match probabilities from feature vectors. Options: XGBoost, Random Forest, Logistic Regression, or deep learning (TensorFlow/PyTorch).

### Decision

Use XGBoost as the primary model, with Logistic Regression as the interpretable baseline and Random Forest as an ensemble alternative.

### Rationale

- **Tabular data performance**: XGBoost consistently outperforms deep learning on tabular feature vectors with < 100 features.
- **Label efficiency**: Gradient boosting performs well with small-to-medium training sets (500–5000 labeled pairs). Deep learning typically requires more data.
- **Handles missing values**: Native support for null features without imputation.
- **Interpretability**: Feature importance scores and SHAP values explain model decisions (important for stewardship).
- **Spark integration**: `xgboost4j-spark` enables distributed training on large datasets.

### Consequences

- Deep learning may outperform XGBoost if training data exceeds 100K labeled pairs.
- XGBoost requires careful hyperparameter tuning (max_depth, eta, subsample) to avoid overfitting on small datasets.

---

## ADR-005: Multi-Stage Matching Pipeline (Exact → ML → LLM → Embeddings)

**Status**: Accepted
**Date**: 2026-05-31

### Context

Entity matching can be done with any single technique, but different techniques have different cost/accuracy profiles. A phased approach optimizes both.

### Decision

Apply matching techniques in increasing order of computational cost and sophistication:

1. **Exact matching** (Phase 6): O(n), near-zero cost. Catches identical records.
2. **Fuzzy matching** (Phase 7): O(n * b) with blocking. Catches typos and formatting differences.
3. **ML matching** (Phase 10): O(features * pairs). Handles complex feature interactions.
4. **LLM semantic** (Phase 11): High cost per pair. Resolves the hardest cases (abbreviations, context-dependent).
5. **Embedding search** (Phase 12): Upfront cost to embed all records. Enables sub-second similarity search at ingestion time.

### Rationale

- **Cost optimization**: 99% of pairs are resolved by Phases 6–10 at low cost. Only 1% reach the expensive LLM phase.
- **Accuracy optimization**: Each phase handles the cases it's best at. Exact matching for identical records, fuzzy for typos, ML for feature interactions, LLM for semantic understanding.
- **Independent scaling**: Phases can be scaled independently. Add LLM capacity without affecting ML throughput.

### Consequences

- System complexity: 5 matching stages means 5 places where things can go wrong.
- Latency: A record pair might pass through multiple stages before resolution. Total latency = sum of stage latencies.
- Inconsistency risk: Different stages might produce different decisions for similar pairs if thresholds aren't calibrated consistently.

---

## ADR-006: Feedback-Driven Continuous Improvement

**Status**: Accepted
**Date**: 2026-05-31

### Context

A one-time matching pipeline degrades over time as data distributions shift. The system must improve with use.

### Decision

Implement a closed-loop feedback system:

1. **Steward decisions** (Phase 14) → **Training labels** → **Model retraining** (Phase 10)
2. **Steward patterns** (Phase 14) → **Survivorship rule updates** (Phase 13)
3. **Quality issues** (Phase 3) → **Source system fixes** (Phase 1)

### Rationale

- **Active learning**: Stewards review the most informative pairs, maximizing label value.
- **Continuous improvement**: Each quarter, the model should be better than the previous quarter.
- **Adaptation**: The pipeline adapts to new data patterns, new source systems, and changing business definitions.

### Consequences

- Requires a steward team with ongoing commitment.
- Model retraining must be automated (triggered when enough new labels accumulate).
- Rollback capability needed (if a retrained model performs worse).

---

## ADR-007: MkDocs + Material over Docusaurus for Documentation

**Status**: Accepted
**Date**: 2026-05-31

### Context

The maturity model documentation requires Mermaid diagram rendering, code syntax highlighting, navigation depth, and search.

### Decision

Use MkDocs with the Material for MkDocs theme.

### Rationale

- **Python ecosystem**: Contributors already have Python. No Node.js or Ruby dependency.
- **Mermaid support**: `pymdownx.superfences` renders Mermaid natively with zero configuration.
- **One-command deploy**: `mkdocs gh-deploy` deploys to GitHub Pages.
- **Search**: Built-in, server-side, no API key or external service needed.

### Consequences

- Limited to Markdown (no MDX/React components). This is acceptable for documentation-focused content.
- Material theme is free for open-source but requires a license for commercial internal use.

---

## ADR-008: Microsoft Fabric as Primary Platform

**Status**: Accepted
**Date**: 2026-08-09

### Context

The entity resolution maturity model was initially designed platform-agnostically with a Databricks-leaning reference stack. For production implementation, a specific platform must be selected. Options: Databricks, Microsoft Fabric, AWS EMR, or self-managed Spark on Kubernetes.

### Decision

Use Microsoft Fabric as the primary reference platform for production implementations.

### Rationale

- **Unified SaaS platform**: All workloads (Data Factory, Data Engineering, Data Science, Real-Time Intelligence, Power BI) operate on a single capacity pool with OneLake as the unified storage layer — no stitching together separate services.
- **Native Delta Lake**: Delta is Fabric's native table format. All ACID transactions, time travel, schema enforcement, and merges work without additional JARs or configuration.
- **Built-in MLflow**: Fabric natively supports MLflow for experiment tracking and model registry — no separate infrastructure to provision or maintain.
- **OneLake shortcuts**: Delta tables can be shared across workspaces without data duplication, enabling a clean medallion architecture where Bronze/Silver/Gold layers in separate workspaces reference the same physical data.
- **Cost predictability**: Capacity-based pricing (F-SKUs) with the ability to pause when idle provides predictable costs for development and production.
- **Power BI integration**: Matching results, quality dashboards, and stewardship metrics are natively accessible in Power BI via Direct Lake mode without data movement.
- **Managed Spark runtime**: Fabric Runtime provides pre-configured PySpark environments with library management, reducing operational overhead compared to self-managed clusters.

### Consequences

- Platform lock-in to the Microsoft ecosystem (Azure, OneLake, Fabric SKUs).
- Fabric Spark features may lag behind the latest open-source Apache Spark releases.
- Some open-source libraries (e.g., certain FAISS GPU configurations) may require additional setup within Fabric's managed environment.
- Teams already invested in the Databricks ecosystem may prefer to stay; the maturity model concepts remain applicable to both platforms.
