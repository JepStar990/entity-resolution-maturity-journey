# Technology Stack

This reference details each technology in the entity resolution maturity pipeline, its role, version recommendations, and alternatives.

---

## Core Pipeline

### Apache Spark (PySpark)

| Attribute | Value |
|-----------|-------|
| **Version** | 3.5+ |
| **Role** | Distributed data processing engine |
| **Phases** | 1–10, 13 |
| **Why** | Unified API for batch and streaming; DataFrame API is expressive and optimized; MLlib provides distributed ML training; scales horizontally from laptop to 1000+ node clusters. |

**Alternatives**: Apache Beam (portable, but more complex), Dask (Python-native, smaller ecosystem), Polars (fast single-node, no distributed).

### Delta Lake

| Attribute | Value |
|-----------|-------|
| **Version** | 3.x |
| **Role** | Storage layer with ACID transactions |
| **Phases** | All |
| **Why** | ACID transactions on data lake storage; time travel enables point-in-time queries and rollback; schema enforcement prevents data corruption; efficient upserts/merges for SCD and dedup. |

**Alternatives**: Apache Iceberg (broader ecosystem, Hive/Trino/Flink support), Apache Hudi (better for streaming upserts). Delta Lake chosen for deepest Spark integration.

---

## Data Quality

### Great Expectations

| Attribute | Value |
|-----------|-------|
| **Version** | 1.x |
| **Role** | Data quality expectations framework |
| **Phases** | 3 |
| **Why** | Declarative expectation definitions; auto-generated data docs; Spark and Pandas backends; integrates with CI/CD pipelines for quality gating. |

**Alternatives**: Deequ (Spark-native, Scala/Java, better for large Spark workloads but smaller community), Soda (SQL-based, simpler learning curve, cloud-native).

---

## String Matching

### jellyfish

| Attribute | Value |
|-----------|-------|
| **Version** | 1.x |
| **Role** | Phonetic and string distance algorithms |
| **Phases** | 7, 9 |
| **Why** | Pure Python implementation of Soundex, Metaphone, NYSIIS, Match Rating Approach; stable API; no compiled dependencies. |

### textdistance

| Attribute | Value |
|-----------|-------|
| **Version** | 4.x |
| **Role** | Comprehensive string distance library |
| **Phases** | 7, 9 |
| **Why** | 30+ algorithms (Levenshtein, Damerau-Levenshtein, Jaro-Winkler, Hamming, etc.); normalized and non-normalized variants; pure Python. |

**Alternative**: rapidfuzz (C++ backend, 10–100x faster, recommended for production workloads).

---

## Machine Learning

### scikit-learn

| Attribute | Value |
|-----------|-------|
| **Version** | 1.5+ |
| **Role** | Baseline ML models, preprocessing, evaluation |
| **Phases** | 9, 10 |
| **Why** | Gold-standard ML library; excellent API consistency; comprehensive metrics and preprocessing; best for small-to-medium training datasets. |

### XGBoost (xgboost4j-spark)

| Attribute | Value |
|-----------|-------|
| **Version** | 2.x |
| **Role** | Gradient-boosted tree classifier for matching |
| **Phases** | 10 |
| **Why** | State-of-the-art performance on tabular data; handles missing values natively; Spark integration for distributed training; built-in regularization prevents overfitting on small label sets. |

### MLflow

| Attribute | Value |
|-----------|-------|
| **Version** | 2.x |
| **Role** | ML lifecycle management |
| **Phases** | 10, 14 |
| **Why** | Experiment tracking, model versioning, model registry; Spark and scikit-learn integration; promotes models from staging to production with governance. |

---

## Vector Search & Embeddings

### Sentence Transformers

| Attribute | Value |
|-----------|-------|
| **Version** | 2.x |
| **Role** | Text embedding generation |
| **Phases** | 12 |
| **Why** | State-of-the-art pre-trained models (BGE, E5, all-MiniLM); easy `model.encode()` API; multi-lingual models available; runs locally (no API costs). |

### FAISS (Facebook AI Similarity Search)

| Attribute | Value |
|-----------|-------|
| **Version** | 1.7+ |
| **Role** | Vector similarity search |
| **Phases** | 12 |
| **Why** | GPU-accelerated; billions of vectors; multiple index types for different scale/precision trade-offs; C++ backend with Python bindings. |

**Alternatives**: Milvus (distributed, cloud-native, managed), Pinecone (fully managed, no ops), Weaviate (open-source, GraphQL-native).

---

## LLM Integration

### LiteLLM

| Attribute | Value |
|-----------|-------|
| **Version** | 1.x |
| **Role** | Multi-provider LLM proxy |
| **Phases** | 11 |
| **Why** | Unified API across OpenAI, Anthropic, Azure, AWS Bedrock, and open-source models; built-in cost tracking, rate limiting, and retry logic. |

### LangChain

| Attribute | Value |
|-----------|-------|
| **Version** | 0.3+ |
| **Role** | LLM orchestration framework |
| **Phases** | 11 |
| **Why** | Prompt templating, chain-of-thought, structured output parsing; extensive model integrations; active community. Use only the components you need (avoid the overly abstract "chains" for simple LLM calls). |

---

## APIs & Distribution

### FastAPI

| Attribute | Value |
|-----------|-------|
| **Version** | 0.110+ |
| **Role** | MDM REST API |
| **Phases** | 15 |
| **Why** | High-performance async Python web framework; auto-generated OpenAPI/Swagger docs; Pydantic-based validation; benchmarked as one of the fastest Python frameworks. |

### Apache Kafka

| Attribute | Value |
|-----------|-------|
| **Version** | 3.x |
| **Role** | Event streaming for MDM distribution |
| **Phases** | 15 |
| **Why** | Industry standard for durable, replayable event streams; exactly-once semantics; broad ecosystem (Kafka Connect, ksqlDB, Schema Registry). |

---

## Orchestration & Monitoring

### Apache Airflow

| Attribute | Value |
|-----------|-------|
| **Version** | 2.9+ |
| **Role** | Workflow orchestration |
| **Phases** | 1, 10, 14, 15 |
| **Why** | Python-native DAG definitions; rich scheduling (cron, sensors, dependencies); massive community and operator ecosystem. |

**Alternative**: Dagster (modern, asset-based, better for data pipelines specifically).

### Prometheus + Grafana

| Attribute | Value |
|-----------|-------|
| **Version** | Prometheus 2.x, Grafana 10.x |
| **Role** | Metrics collection and visualization |
| **Phases** | 15 |
| **Why** | Industry standard for infrastructure and application monitoring; Prometheus for metrics collection and alerting; Grafana for dashboards. |

---

## Version Compatibility Matrix

| Component | Min Version | Max Version | Notes |
|-----------|-------------|-------------|-------|
| Python | 3.10 | 3.12 | 3.13 not yet fully supported by PySpark |
| PySpark | 3.5.0 | 3.5.x | Major version stability |
| Delta Lake | 3.0.0 | 3.x | Must match Spark version |
| Java | 11 | 17 | Spark 3.5 requires Java 8/11/17 |
| Great Expectations | 1.0 | 1.x | Major API stability |
| XGBoost | 2.0 | 2.x | Spark classifier in xgboost4j-spark |
| FAISS | 1.7.0 | 1.7.x | GPU support optional |
| FastAPI | 0.100.0 | 0.115.x | Active development |
