# Phase Overview

This page provides a high-level map of all 15 phases in the entity resolution maturity journey.

---

## Summary Table

| # | Phase | Input | Output | Layer | Key Technology |
|---|-------|-------|--------|-------|----------------|
| 1 | Basic Data Ingestion | Source Systems | Bronze Tables | Bronze | PySpark, Delta Lake |
| 2 | Schema Validation | Bronze Tables | Validated Bronze | Bronze | PySpark, Schema Registry |
| 3 | Data Quality Rules | Validated Bronze | Silver | Bronze | Great Expectations |
| 4 | Standardization | Silver | Silver Standardized | Silver | PySpark UDFs, Regex |
| 5 | Data Enrichment | Silver Standardized | Enriched Silver | Silver | Reference APIs, SCD |
| 6 | Exact Deduplication | Enriched Silver | Deduplicated Silver | Silver | Window Functions |
| 7 | Fuzzy Matching | Deduplicated Silver | Candidate Matches | Silver | Levenshtein, Jaro-Winkler |
| 8 | Record Blocking | Deduplicated Silver | Blocked Candidates | Silver | Sorted Neighborhood |
| 9 | Feature Engineering | Candidate Pairs | Feature Vectors | Gold | PySpark ML |
| 10 | Probabilistic Matching | Feature Vectors | Match Probabilities | Gold | XGBoost, scikit-learn |
| 11 | LLM Semantic Matching | Low-Confidence Pairs | Semantic Scores | Gold | LLM APIs, LangChain |
| 12 | Embedding-Based Matching | Golden Candidates | Vector Matches | Gold | FAISS, Sentence Transformers |
| 13 | Golden Record Creation | Matched Records | Golden Records | Gold | Survivorship Rules |
| 14 | Data Stewardship | Uncertain Matches | Reviewed Decisions | Gold | Review Queue, Feedback Loop |
| 15 | MDM Distribution | Golden Records | Enterprise Consumption | Gold | FastAPI, Event Streams |

---

## Maturity Curve

```mermaid
--8<-- "diagrams/maturity-curve.mmd"
```

The curve shows three capability dimensions evolving across the 15 phases:

1. **Data Quality Maturity** — Rises rapidly in Phases 1–5 (foundation), plateaus during matching phases, and climbs again in Phases 13–15 (governance, stewardship).

2. **Entity Resolution Sophistication** — Starts near zero, accelerates through Phases 6–12 (matching techniques), and asymptotes at Phase 15.

3. **Operational Readiness** — Grows steadily, with inflection points at Phase 3 (first quality gate), Phase 8 (scalability), and Phase 14–15 (production MDM).

---

## Medallion Architecture Mapping

```mermaid
--8<-- "diagrams/bronze-silver-gold.mmd"
```

### Bronze Layer — Foundation (Phases 1–3)

**Goal**: Get data into the platform reliably, validate its structure, and measure quality.

- **Phase 1**: Ingest from diverse sources. Preserve raw data. Add metadata.
- **Phase 2**: Enforce schema contracts. Separate passing and failing records.
- **Phase 3**: Apply quality rules. Generate metrics. Gate progression to Silver.

### Silver Layer — Cleaning & Matching (Phases 4–8)

**Goal**: Clean, standardize, enrich, and deduplicate data.

- **Phase 4**: Normalize names, phones, addresses, dates to consistent formats.
- **Phase 5**: Join reference data (postal codes, industry codes, firmographics).
- **Phase 6**: Remove exact duplicates with SQL and window functions.
- **Phase 7**: Catch near-duplicates with string distance algorithms.
- **Phase 8**: Scale matching with blocking strategies (sorted neighborhood, canopy clustering).

### Gold Layer — Intelligence & Trust (Phases 9–15)

**Goal**: Apply AI matching, create golden records, enable human review, and distribute trusted data.

- **Phase 9**: Build similarity feature vectors for ML models.
- **Phase 10**: Train probabilistic models (Logistic Regression, XGBoost).
- **Phase 11**: Use LLMs for semantic understanding of entity names and abbreviations.
- **Phase 12**: Leverage embedding models and vector databases for deep similarity search.
- **Phase 13**: Merge matched records into golden records with survivorship rules.
- **Phase 14**: Enable human review of uncertain matches; feed decisions back into models.
- **Phase 15**: Distribute trusted golden records to enterprise consumers via APIs and events.

---

## Reading Guide by Persona

| Persona | Recommended Phases | Skip / Skim |
|---------|-------------------|-------------|
| **Data Engineer** | 1–8, 13–15 | 9–12 (read summaries for context) |
| **ML Engineer** | 6–12 | 1–5 (skim for prerequisites) |
| **Data Steward** | 3, 13–14 | 4–12 (read overview only) |
| **Architect** | All 15 | None — read the full journey |

---

## How to Use This Documentation

### If You're Building a Pipeline

1. Assess your current maturity: which phase describes your current state?
2. Start at that phase and work forward sequentially.
3. Each phase document includes a **"When to Advance"** section with concrete signals.
4. Adapt the patterns to your technology stack — the principles are technology-agnostic.

### If You're Evaluating Technology

1. Read the [Technology Stack](reference/tech-stack.md) reference.
2. Browse the [Decision Log](reference/decision-log.md) for architecture rationale.
3. Each phase lists key technologies with justification.

### If You're Designing an MDM Strategy

1. Read the [Architecture](architecture.md) page for system context.
2. Study the master pipeline diagram for end-to-end flow.
3. Deep-dive into Phases 13–15 for MDM-specific patterns.
4. Review the [Medallion Architecture](reference/medallion-architecture.md) for storage design.

---

## Next: Phase 1

Start the journey with [:material-arrow-right: Phase 1: Basic Data Ingestion](phase-01-basic-data-ingestion.md)
