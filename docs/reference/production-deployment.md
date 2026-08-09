# Production Deployment Record

## Entity Resolution Maturity Journey — Microsoft Fabric

Date: 2026-08-09
Account: `eradmin@zwiswamuridili990gmail.onmicrosoft.com`
Tenant: `zwiswamuridili990gmail.onmicrosoft.com`
Subscription: Pay as you go SUB (`0043c10a-402e-442e-bcbc-763e26aca8df`)

---

## 1. Azure Infrastructure

### Resources Deployed (eastus2)

| Resource | Name | Purpose |
|----------|------|---------|
| Resource Group | `rg-er-prod` | All production resources |
| Key Vault | `kv-er-prod-uw3ubhx2kgs3o` | Secrets: LLM API keys, connection strings |
| Log Analytics | `log-er-prod` | Pipeline monitoring, diagnostics |
| Application Insights | `appi-er-prod` | Telemetry, performance tracking |

### Fabric Capacity

| Detail | Value |
|--------|-------|
| Type | Trial (60-day free) |
| SKU | FTL4 (F64 equivalent) |
| Region | South Africa North |
| Capacity ID | `1bbe784b-83c0-4590-9daf-c8a86d609517` |
| Expiry | 2026-10-08 |

**Note:** Paid Fabric capacity (F8) deployment was blocked by zero regional quota. The trial bypasses this. A quota increase request or trial-to-paid upgrade is needed before expiry.

---

## 2. Fabric Workspace

### Workspace: `entity-resolution-prod`

| Item | Name | ID |
|------|------|----|
| Workspace | entity-resolution-prod | `70ef7f84-7a5a-4343-97e3-f5fb8e9eb9a9` |

### Medallion Lakehouses

| Layer | Name | ID | Purpose |
|-------|------|----|---------|
| Bronze | `er_bronze` | `0980f690-663a-40c4-a3a8-abc3ce29d41e` | Raw ingestion, schema validation, quality checks |
| Silver | `er_silver` | `d37f72c3-a7e4-4c4a-a781-c3d726eb70f2` | Standardized, enriched, deduplicated data |
| Gold | `er_gold` | `839522f0-3f0a-4dc4-a471-5fae58bfa601` | Golden records, match scores, stewardship, MDM |

### Spark Environment

| Detail | Value |
|--------|-------|
| Name | `er-spark-env-prod` |
| ID | `cdf805f6-96a5-466e-82ed-7713091c8270` |
| Runtime | Fabric Runtime 1.3 |
| State | Published (Success) |
| Custom Wheels | `er_pipeline-1.0.0-py3-none-any.whl` |
| Public Libraries | great-expectations, xgboost, sentence-transformers, faiss-cpu, jellyfish, litellm, langchain, fastapi, uvicorn, azure-eventhub |

### Notebooks

| # | Name | ID | Purpose |
|---|------|----|---------|
| 0 | `00-Master-Orchestrator` | `9e22b8ca-9278-41df-befa-3eb7e001b192` | Config-driven 15-phase pipeline |
| 1 | `01-Load-Kaggle-Dataset` | `f67ea99c-5e17-4a74-a2b5-91d24d32e46a` | Kaggle dataset download |
| 2-16 | `01-Ingestion` through `15-MDM-Distribution` | (auto-generated) | Individual phase placeholders |

### Data Pipeline

| Name | ID |
|------|----|
| `er-pipeline-prod` | `c5ac2466-52a5-4f6c-a41c-d4067f4b711c` |

### Lakehouse Files (er_bronze)

```
Files/
  config/
    pipeline-config.yaml       # Master pipeline configuration
    fabric-prod.yaml           # Production environment overrides
    fabric-dev.yaml            # Development overrides
    entity-customer.yaml       # Customer entity config
    entity-company.yaml        # Company entity config
    entity-product.yaml        # Product entity config
  examples/src/
    __init__.py
    utils/                     # spark_session, logging_config, metrics, delta_helpers
    engine/                    # data_profiler, kaggle_data_loader, pipeline_engine, etc.
    phase_01_ingestion.py      # Phase modules (01-15)
    ...
  customer_raw/
    sample_customers.csv       # 375 records (300 unique + 75 duplicates)
```

---

## 3. Code Architecture

### er-pipeline Wheel (v1.0.0)

```
er_pipeline/
  __init__.py
  phases/
    phase_01_ingestion.py          # Multi-source data ingestion
    phase_02_schema_validation.py  # Schema enforcement and validation
    phase_03_data_quality.py       # Quality rules engine
    phase_04_standardization.py    # Name/address/phone normalization
    phase_05_enrichment.py         # Reference data enrichment
    phase_06_exact_dedup.py        # Exact deduplication
    phase_07_fuzzy_matching.py     # Fuzzy string matching (Levenshtein, Jaro-Winkler)
    phase_08_record_blocking.py    # Scalable blocking strategies
    phase_09_feature_engineering.py # ML feature construction
    phase_10_probabilistic_matching.py # XGBoost/scikit-learn matching
    phase_11_semantic_matching_llm.py  # LLM-based semantic matching
    phase_12_embedding_matching.py  # FAISS vector embedding matching
    phase_13_golden_record.py      # Survivorship and golden records
    phase_14_stewardship.py        # Human review workflow
    phase_15_mdm_distribution.py   # API and event distribution
  utils/
    spark_session.py    # Fabric-aware Spark session builder
    logging_config.py   # Structured logging with context
    metrics.py          # Metrics collection per phase
    delta_helpers.py    # Delta Lake operations
  engine/
    data_profiler.py         # Automatic data profiling
    kaggle_data_loader.py    # Kaggle dataset download + 12 curated datasets
    pipeline_engine.py       # Full pipeline orchestration engine
    production_hardening.py  # Idempotency, retries, validation
    sample_data_generator.py # Synthetic data generation
    test_runner.py           # Phase-level integration tests
```

### Orchestrator Flow

```
pipeline-config.yaml
       |
       v
  Resolve entity type (customer/company/product)
       |
       v
  Load entity-{type}.yaml (schema, matching rules)
       |
       v
  P1  -> P2  -> P3  -> P4  -> P5    (Foundation: Bronze->Silver)
  P6  -> P7  -> P8                 (Matching: Exact, Fuzzy, Blocking)
  P9  -> P10 -> P11 -> P12         (AI Matching: Features, ML, LLM, Embeddings)
  P13 -> P14 -> P15                (Operationalize: Goldens, Stewards, MDM)
```

Each phase exposes `run(spark, df, config) -> df`. The orchestrator chains them
dynamically. Phases 9-12 degrade gracefully when prerequisites are unavailable
(no candidate pairs, no GPU, no LLM key).

---

## 4. Configuration System

### pipeline-config.yaml Structure

```yaml
pipeline:
  name: entity-resolution-maturity-journey
  version: "1.0.0"
  default_entity: customer          # <- Change to switch entities

environment:
  fabric_workspace: entity-resolution-prod
  fabric_lakehouse: er_bronze
  spark_runtime: "Fabric Runtime 1.3"

medallion:
  bronze_path: Tables/bronze/
  silver_path: Tables/silver/
  gold_path: Tables/gold/

ingestion:
  sources:
    - type: csv
      path: Files/customer_raw/
      table_name: customer_raw

# Per-phase configuration: exact_dedup, fuzzy_matching, record_blocking,
# feature_engineering, probabilistic_matching, llm_semantic,
# embedding_matching, golden_record, stewardship, mdm_distribution
```

### Entity Config Structure (entity-customer.yaml)

```yaml
entity: customer
schema:
  columns: [id, first_name, last_name, email, phone, address, city, state, postal_code, country]
  required: [id, first_name, last_name, email]
matching:
  blocking_keys: [state, last_name_first_letter]
  fuzzy_fields: [first_name, last_name, email]
  match_threshold: 0.85
survivorship:
  priority_fields: [email, phone]
```

---

## 5. Git Workflow

| Step | Detail |
|------|--------|
| Issue | [#2](https://github.com/JepStar990/entity-resolution-maturity-journey/issues/2) — feat: production Fabric workspace provisioning |
| Branch | `feat/production-fabric-provisioning` |
| Commit | `00b14b7` — 41 files, 11,824 insertions |
| PR | [#3](https://github.com/JepStar990/entity-resolution-maturity-journey/pull/3) — open, awaiting review |
| Remote | `https://github.com/JepStar990/entity-resolution-maturity-journey` |

### Changes in PR #3

- Fixed Bicep API version: `2023-07-01` -> `2023-11-01` for Fabric capacity
- Fixed Fabric capacity naming: lowercase alphanumeric only (no hyphens)
- Added Python 3.8 compatibility to provision-fabric.py
- Added `--capacity-id` parameter for trial/managed capacity support
- Made provisioning script idempotent
- Added CI/CD pipeline (`.github/workflows/ci-cd.yml`)
- Added full example source code, configs, and notebook scaffolding

---

## 6. Key Technical Decisions

1. **Wheel packaging over lakehouse imports:** Fabric lakehouses use OneLake (object storage), not a local filesystem. Python's `sys.path` and `importlib` cannot resolve `/lakehouse/default/Files/` paths. Solution: package phase modules as a pip wheel, upload to Spark environment as a custom library.

2. **Config via Spark text read:** Config files stored in lakehouse Files section cannot be opened with `open()`. Solution: `spark.read.format("text").load("Files/config/...")` then parse.

3. **Native tenant account required:** Microsoft Fabric rejects guest (`#EXT#`) accounts for capacity creation and API access. Created `eradmin@zwiswamuridili990gmail.onmicrosoft.com` as a native member with Global Administrator role.

4. **Trial capacity over paid SKU:** New subscriptions have zero Fabric capacity quota. The 60-day Fabric trial (FTL4, F64 equivalent) bypasses quota entirely. A permanent F8 capacity requires a quota increase request.

5. **Fabric capacity naming:** The Fabric RP enforces `^[a-z][a-z0-9]*$` — lowercase alphanumeric only, must start with a letter. Hyphens are rejected.

6. **Admin principals format:** Fabric Bicep templates accept UPN strings (`user@domain.com`), not AAD object IDs, for the `administration.members` array.

---

## 7. Gaps and Remaining Work

### Critical

| Gap | Impact | Resolution |
|-----|--------|------------|
| **Fabric trial expiry (60 days)** | Workspace becomes inaccessible on 2026-10-08 | Request Fabric capacity quota increase in Azure Portal, deploy permanent F8 capacity, or upgrade trial to paid |
| **No real source data** | Pipeline runs on 375 synthetic records only | Use `01-Load-Kaggle-Dataset` notebook to pull real data; connect to actual CRM/ERP sources |
| **LLM API key not configured** | Phase 11 (LLM Semantic) will skip | Add OpenAI/Anthropic API keys to Key Vault `kv-er-prod-uw3ubhx2kgs3o`; reference in pipeline config |
| **PR #3 not merged** | Code changes not on master branch | Review and merge https://github.com/JepStar990/entity-resolution-maturity-journey/pull/3 |

### Important

| Gap | Impact | Resolution |
|-----|--------|------------|
| **Kaggle not in Spark environment** | `01-Load-Kaggle-Dataset` uses `%pip install` at runtime (slower first run) | Create new environment with kaggle pre-installed, or add via `environmentYml` |
| **No CI/CD pipeline for Fabric** | Manual notebook deployment | `.github/workflows/ci-cd.yml` exists but needs Fabric deployment steps added |
| **Phase notebooks are placeholders** | Individual phase debugging not possible | Populate auto-generated notebooks with real code from the wheel modules |
| **No monitoring/alerting** | Pipeline failures go unnoticed | Configure Log Analytics alerts; wire App Insights to the pipeline metrics |
| **Data pipeline not wired to orchestrator** | `er-pipeline-prod` references placeholder notebook IDs | Update pipeline to chain proper phase notebooks or run orchestrator directly |

### Nice to Have

| Gap | Resolution |
|-----|------------|
| **No entity-company/product configs validated** | Create sample data for company/product entities and test end-to-end |
| **No performance benchmarking** | Run with Kaggle stress-test datasets (5M flights, 1M taxi) on higher SKU |
| **No automated testing** | Wire `test_runner.py` into CI/CD; add Fabric notebook test execution |
| **MLflow not configured** | Enable MLflow tracking in Spark environment for experiment management |
| **No RBAC beyond admin** | Add data steward, analyst, and engineer security groups to workspace |
| **Bicep template hardcoded for prod** | Refactor `infra/main.bicep` resource names to accept environment parameter |
| **OneLake file shortcuts not configured** | Create shortcuts between medallion layers for cross-lakehouse access |

---

## 8. Quick Reference: Running the Pipeline

```bash
# Portal
https://app.fabric.microsoft.com/
  -> entity-resolution-prod
  -> 00-Master-Orchestrator
  -> Attach lakehouse: er_bronze
  -> Spark environment: er-spark-env-prod
  -> Run All

# API (get token first)
az login --use-device-code
az account get-access-token --resource https://api.fabric.microsoft.com
```

### Environment Variables for Key Vault

| Secret | Purpose |
|--------|---------|
| `llm-api-key` | OpenAI/Anthropic API key for Phase 11 |
| `kaggle-username` | Kaggle API username |
| `kaggle-key` | Kaggle API key |
| `mdm-api-key` | MDM distribution API auth (Phase 15) |
