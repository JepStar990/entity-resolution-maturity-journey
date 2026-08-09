# Examples

This directory contains the reference implementation for the 15-phase entity resolution maturity model. All code targets Microsoft Fabric with PySpark and Delta Lake.

## Structure

```
examples/
├── README.md                           # This file
├── config/                             # YAML configuration files
│   ├── pipeline-config.yaml            # Master pipeline configuration
│   ├── fabric-dev.yaml                 # Development environment overrides
│   ├── fabric-prod.yaml                # Production environment overrides
│   ├── entity-customer.yaml            # Customer entity configuration
│   ├── entity-company.yaml             # Company entity configuration
│   └── entity-product.yaml             # Product entity configuration
├── src/                                # Python phase modules
│   ├── __init__.py
│   ├── utils/                          # Shared utilities
│   │   ├── __init__.py
│   │   ├── spark_session.py            # Spark session builder
│   │   ├── delta_helpers.py            # Delta Lake operations
│   │   ├── metrics.py                  # Metrics collection
│   │   └── logging_config.py           # Structured logging
│   ├── phase_01_ingestion.py           # Multi-source ingestion
│   ├── phase_02_schema_validation.py   # Schema enforcement
│   ├── phase_03_data_quality.py        # Quality rules engine
│   ├── phase_04_standardization.py     # Name/address/phone normalization
│   ├── phase_05_enrichment.py          # Reference data enrichment
│   ├── phase_06_exact_dedup.py         # Exact deduplication
│   ├── phase_07_fuzzy_matching.py      # Fuzzy string matching
│   ├── phase_08_record_blocking.py     # Scalable blocking strategies
│   ├── phase_09_feature_engineering.py # ML feature construction
│   ├── phase_10_probabilistic_matching.py # ML model training
│   ├── phase_11_semantic_matching_llm.py # LLM-based matching
│   ├── phase_12_embedding_matching.py  # Vector embedding matching
│   ├── phase_13_golden_record.py       # Survivorship and golden records
│   ├── phase_14_stewardship.py         # Human review workflow
│   └── phase_15_mdm_distribution.py    # API and event distribution
└── notebooks/                          # Fabric notebooks
    └── 00-master-orchestrator.ipynb    # Full pipeline execution notebook
```

## Quick Start

### Local Development

```bash
pip install pyspark>=3.5 delta-spark>=3.0 pyyaml

python examples/src/phase_01_ingestion.py
```

### Fabric Deployment

1. Upload notebooks to your Fabric workspace
2. Attach notebooks to your Lakehouse
3. Configure `%%configure` magic at the top of each notebook
4. Run the master orchestrator notebook

## Configuration

All pipeline behavior is driven by YAML configuration files in `examples/config/`. The master `pipeline-config.yaml` defines defaults for all 15 phases. Environment-specific files (`fabric-dev.yaml`, `fabric-prod.yaml`) override settings for each deployment.

## Phase Interface

Every phase module exposes a `run()` function with this signature:

```python
def run(
    spark: SparkSession,
    df: DataFrame,           # Input DataFrame from previous phase
    entity_type: str,        # Entity type: customer, company, product
    config: Optional[dict],  # Phase-specific configuration
    *paths: str,             # Medallion layer paths
    workspace: Optional[str],# Fabric workspace name
    metrics: Optional[MetricsCollector],  # Metrics instrumentation
) -> DataFrame:              # Output DataFrame for next phase
    ...
```
