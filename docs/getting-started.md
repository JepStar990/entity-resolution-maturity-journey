# Getting Started

This guide covers prerequisites, setup, and recommended reading paths for different roles.

---

## Prerequisites

### For Reading the Documentation

- Familiarity with data engineering concepts (ETL/ELT, data warehouses, data lakes)
- Basic understanding of Python and SQL
- No prior entity resolution or MDM experience required

### For Running Code Examples

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.10+ | Primary language |
| Apache Spark | 3.5+ | Distributed processing |
| Java | 11+ | Spark runtime requirement |
| Delta Lake | 3.x | Storage layer |
| Great Expectations | 1.x | Data quality framework |

### Local Setup

```bash
# 1. Clone the repository
git clone https://github.com/JepStar990/entity-resolution-maturity-journey.git
cd entity-resolution-maturity-journey

# 2. Create a virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
# .venv\Scripts\activate   # Windows

# 3. Install documentation dependencies
pip install -r requirements-docs.txt

# 4. Serve the documentation locally
mkdocs serve
```

Open `http://127.0.0.1:8000` in your browser.

### Running Code Examples

The `examples/` directory contains annotated PySpark code scaffolding. These are **illustrative, not production-ready**. Each module demonstrates the core pattern for its phase.

```bash
# Install PySpark (if running locally)
pip install pyspark>=3.5

# Optional: Delta Lake support
pip install delta-spark>=3.0

# Run an example (from the repo root)
python examples/src/phase_01_ingestion.py
```

---

## Reading Paths by Persona

### Data Engineer

**Focus**: Build reliable pipelines. Get data under control before matching.

| Step | Document | Why |
|------|----------|-----|
| 1 | [Architecture](architecture.md) | Understand the big picture |
| 2 | [Phase 1: Ingestion](phases/phase-01-basic-data-ingestion.md) | Foundation of all data work |
| 3 | [Phase 2: Schema Validation](phases/phase-02-schema-validation.md) | Enforce data contracts |
| 4 | [Phase 3: Data Quality](phases/phase-03-data-quality-rules.md) | Measure and enforce quality |
| 5 | [Phase 4: Standardization](phases/phase-04-standardization.md) | Consistent data formats |
| 6 | [Phase 5: Enrichment](phases/phase-05-data-enrichment.md) | Add business context |
| 7 | [Phases 6–8: Deduplication](phases/phase-06-exact-deduplication.md) | Remove duplicates at scale |
| 8 | [Phases 13–15: MDM](phases/phase-13-golden-record-creation.md) | Productionize and distribute |

### ML Engineer

**Focus**: Design and train matching models. Progress from deterministic to AI-driven.

| Step | Document | Why |
|------|----------|-----|
| 1 | [Architecture](architecture.md) | Understand the pipeline context |
| 2 | [Phases 1–5 (skim)](phases/phase-01-basic-data-ingestion.md) | Understand data quality prerequisites |
| 3 | [Phase 6: Exact Dedup](phases/phase-06-exact-deduplication.md) | Baseline matching |
| 4 | [Phase 7: Fuzzy Matching](phases/phase-07-fuzzy-matching.md) | String distance algorithms |
| 5 | [Phase 8: Blocking](phases/phase-08-record-blocking.md) | Scale matching to millions |
| 6 | [Phase 9: Feature Engineering](phases/phase-09-feature-engineering.md) | Build similarity vectors |
| 7 | [Phase 10: ML Matching](phases/phase-10-probabilistic-matching.md) | Train probabilistic models |
| 8 | [Phase 11: LLM Semantic](phases/phase-11-semantic-matching-llm.md) | AI-powered matching |
| 9 | [Phase 12: Embeddings](phases/phase-12-embedding-matching.md) | Vector-based search |

### Data Steward

**Focus**: Review matches, manage survivorship, provide feedback.

| Step | Document | Why |
|------|----------|-----|
| 1 | [Phase 3: Data Quality](phases/phase-03-data-quality-rules.md) | Understand quality metrics |
| 2 | [Phase Overview](phases/overview.md) | See the full journey |
| 3 | [Phase 13: Golden Records](phases/phase-13-golden-record-creation.md) | How records are merged |
| 4 | [Phase 14: Stewardship](phases/phase-14-data-stewardship.md) | Your primary workflow |
| 5 | [Phase 15: MDM](phases/phase-15-master-data-management.md) | How trusted data is consumed |

### Architect

**Focus**: Design enterprise MDM systems. Understand all patterns and trade-offs.

| Step | Document | Why |
|------|----------|-----|
| 1 | [Architecture](architecture.md) | System context and design principles |
| 2 | [Phase Overview](phases/overview.md) | Full maturity model summary |
| 3 | All 15 phases | Deep understanding of each stage |
| 4 | [Medallion Architecture](reference/medallion-architecture.md) | Storage layer design |
| 5 | [Technology Stack](reference/tech-stack.md) | Tooling rationale |
| 6 | [Decision Log](reference/decision-log.md) | Key architectural decisions |
| 7 | [Bibliography](reference/bibliography.md) | Foundational papers and resources |

---

## Repository Structure

```
entity-resolution-maturity-journey/
│
├── docs/
│   ├── index.md                     # Landing page
│   ├── architecture.md              # System context & design principles
│   ├── getting-started.md           # This page
│   │
│   ├── phases/                      # 15 phase documentation files
│   │   ├── overview.md              # Summary table & maturity curve
│   │   └── phase-01 through 15      # Detailed per-phase docs
│   │
│   ├── diagrams/                    # Mermaid diagram source files
│   │   ├── master-pipeline.mmd      # Full 15-phase pipeline
│   │   ├── data-flow-architecture.mmd
│   │   ├── maturity-curve.mmd
│   │   ├── bronze-silver-gold.mmd
│   │   └── phase-01 through 15.mmd  # Per-phase diagrams
│   │
│   ├── reference/                   # Supplementary reference material
│   │   ├── glossary.md
│   │   ├── tech-stack.md
│   │   ├── bibliography.md
│   │   ├── medallion-architecture.md
│   │   └── decision-log.md
│   │
│   ├── assets/images/
│   └── stylesheets/extra.css
│
├── examples/                        # Annotated PySpark code scaffolding
│   ├── README.md
│   ├── config/                      # YAML configuration stubs
│   ├── src/                         # Python modules (one per phase)
│   └── notebooks/                   # Jupyter notebooks (one per phase)
│
├── mkdocs.yml                       # MkDocs + Material configuration
├── requirements-docs.txt            # Python doc dependencies
└── README.md                        # GitHub repository README
```

---

## Conventions

### Diagrams

All diagrams use [Mermaid](https://mermaid.js.org/) and render natively in both GitHub and MkDocs Material.

- **Direction**: Top-down (`flowchart TD`) for process flows, left-to-right (`flowchart LR`) for data pipelines.
- **Color coding**: Bronze nodes (`#cd7f32`), Silver nodes (`#a8a8a8`), Gold nodes (`#ffd700`).

### Code Examples

- Type hints on all functions (`def run(spark: SparkSession, df: DataFrame) -> DataFrame`)
- Google-style docstrings
- DataFrame API over RDD (performance, readability)
- No `.collect()` in examples (demonstrates production-safe patterns)

---

## Next Steps

Start your journey:

- [:material-arrow-right: Phase Overview & Maturity Curve](phases/overview.md)
- [:material-arrow-right: Phase 1: Basic Data Ingestion](phases/phase-01-basic-data-ingestion.md)
