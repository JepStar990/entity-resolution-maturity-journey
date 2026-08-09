# Entity Resolution Maturity Journey

A documented 15-phase maturity model for building enterprise-grade entity resolution and master data management (MDM) pipelines using **Python and PySpark**.

---

## The Journey at a Glance

```mermaid
--8<-- "diagrams/master-pipeline.mmd"
```

Most organizations do not start with AI-powered entity resolution. They first build a reliable data engineering foundation, then progressively add data quality, standardization, and finally intelligent matching. This model captures that journey across three acts:

### Act I: Foundation (Phases 1–5) — *Getting Data Under Control*

Before you can deduplicate, you must trust your data. These phases establish reliable ingestion, validation, quality checks, standardization, and enrichment.

[:material-arrow-right: Start at Phase 1](phases/phase-01-basic-data-ingestion.md)

### Act II: Matching Mastery (Phases 6–12) — *Finding the Same Entity*

Progressively sophisticated matching — from deterministic exact matches to probabilistic ML models, LLM-powered semantic understanding, and embedding-based vector search.

[:material-arrow-right: Start at Phase 6](phases/phase-06-exact-deduplication.md)

### Act III: Operationalization (Phases 13–15) — *Making It Real*

From matches to trusted golden records, through human stewardship, to enterprise-wide MDM distribution.

[:material-arrow-right: Start at Phase 13](phases/phase-13-golden-record-creation.md)

---

## Who Is This For?

<div class="grid cards" markdown>

-   :material-database-cog:{ .lg .middle } **Data Engineers**

    ---

    Build reliable ingestion, validation, and quality pipelines.

    [:octicons-arrow-right-24: Recommended path: Phases 1–8, 13–15](getting-started.md#data-engineer)

-   :material-brain:{ .lg .middle } **ML Engineers**

    ---

    Design and train matching models from exact to embeddings.

    [:octicons-arrow-right-24: Recommended path: Phases 6–12](getting-started.md#ml-engineer)

-   :material-account-check:{ .lg .middle } **Data Stewards**

    ---

    Manage review queues and survivorship rules.

    [:octicons-arrow-right-24: Recommended path: Phases 3, 13–14](getting-started.md#data-steward)

-   :material-architect:{ .lg .middle } **Architects**

    ---

    Design enterprise MDM systems with proven patterns.

    [:octicons-arrow-right-24: Recommended path: Full journey](getting-started.md#architect)

</div>

---

## Maturity Curve

```mermaid
--8<-- "diagrams/maturity-curve.mmd"
```

Three capability dimensions improve across the journey:

| Dimension | What It Measures |
|-----------|-----------------|
| **Data Quality Maturity** | Completeness, validity, consistency, and freshness of data |
| **Entity Resolution Sophistication** | Matching accuracy from exact rules to AI-powered semantic matching |
| **Operational Readiness** | Production hardening, monitoring, stewardship, and SLAs |

---

## Medallion Architecture

The 15 phases map naturally to the Bronze → Silver → Gold medallion architecture:

```mermaid
--8<-- "diagrams/bronze-silver-gold.mmd"
```

| Layer | Phases | Characteristics |
|-------|--------|----------------|
| **Bronze** | 1–3 | Raw, immutable, append-only. Source fidelity preserved. |
| **Silver** | 4–8 | Cleansed, validated, standardized, deduplicated. |
| **Gold** | 9–15 | Business-ready, governed, trusted. Ready for consumption. |

[:material-book-open-variant: Deep dive on medallion architecture](reference/medallion-architecture.md)

[:material-target: Real-world use cases by industry](reference/use-cases.md)

[:material-road-variant: Planned features and roadmap](reference/feature-roadmap.md)

[:material-robot: AI-assisted development guide](reference/ai-assisted-development.md)

---

## Quick Start

```bash
# Clone the repo
git clone https://github.com/JepStar990/entity-resolution-maturity-journey.git
cd entity-resolution-maturity-journey

# Install docs dependencies
pip install -r requirements-docs.txt

# Serve documentation locally
mkdocs serve
```

Visit `http://127.0.0.1:8000` to browse the full documentation.

[:material-rocket-launch: Full Getting Started guide](getting-started.md)
