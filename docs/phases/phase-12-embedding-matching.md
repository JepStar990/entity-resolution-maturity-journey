# Phase 12: Embedding-Based Matching

## Phase Overview

Convert entity records into dense vector embeddings that capture semantic meaning, then use vector databases and approximate nearest neighbor (ANN) search to find similar entities. Embeddings excel when text differs significantly in wording but is semantically equivalent — cases where even LLMs struggle without explicit comparison.

---

## Business Context

### Why This Phase Matters

LLM-based matching (Phase 11) requires comparing pairs explicitly: "Is Record A the same as Record B?" This works well but has a fundamental limitation — you must know which pairs to compare.

Embedding-based matching inverts the problem:

1. **Embed every record** into a fixed-dimensional vector.
2. **Index all vectors** in a vector database.
3. **Query**: "Find the 10 most similar records to this new record."
4. No explicit pair generation needed — the vector database does it.

This is particularly powerful for:

- **Fuzzy search**: "Show me all records similar to 'Acme South Africa'" without knowing what variations exist.
- **Deduplication at ingestion time**: A new record arrives; instantly check if it already exists.
- **Cross-lingual matching**: Embeddings from multilingual models map "Beijing" and "北京" to nearby vectors.

### Capability Unlocked

Sub-second similarity search across millions of records without explicit pair generation.

---

## Input Data State

| Attribute | Description |
|-----------|-------------|
| **Source** | Golden record candidates from Phase 10/11 |
| **Records** | Clean, enriched, standardized entity records |
| **Volume** | Potentially millions of records |

---

## Processing Logic

### Step 1: Choose and Load an Embedding Model

```python
from sentence_transformers import SentenceTransformer

# Option A: Local model (no API costs, data stays in-house)
model = SentenceTransformer("BAAI/bge-large-en-v1.5")  # 1024 dimensions
# model = SentenceTransformer("intfloat/e5-large-v2")   # 1024 dimensions
# model = SentenceTransformer("all-MiniLM-L6-v2")       # 384 dimensions (fast)

# Option B: API-based (higher quality, per-token cost)
# from openai import OpenAI
# client = OpenAI()
# def get_embedding(text):
#     return client.embeddings.create(
#         model="text-embedding-3-large",
#         input=text
#     ).data[0].embedding
```

### Step 2: Construct Embedding Text

The quality of embeddings depends on how you construct the input text. Entity records should be flattened into a descriptive string:

```python
def record_to_text(record):
    """Convert an entity record into embedding-ready text."""
    parts = [
        f"Company: {record.get('company_name', '')}",
        f"Address: {record.get('address', '')}",
        f"City: {record.get('city', '')}",
        f"Province: {record.get('province', '')}",
        f"Country: {record.get('country', '')}",
        f"Industry: {record.get('industry', '')}",
        f"Phone: {record.get('phone', '')}",
    ]
    return " | ".join(parts)

texts = [record_to_text(r) for r in records]
```

### Step 3: Generate Embeddings

```python
import numpy as np

# Batch encode (much faster than one-at-a-time)
batch_size = 256
embeddings = model.encode(
    texts,
    batch_size=batch_size,
    show_progress_bar=True,
    normalize_embeddings=True  # Enables cosine similarity via dot product
)

# Each record now has a dense vector
# embeddings.shape = (num_records, 1024)
```

### Step 4: Index in Vector Database

```python
import faiss

dimension = embeddings.shape[1]  # 1024

# Create a FAISS index
# IndexFlatIP = Inner Product (cosine similarity when vectors are normalized)
# For large datasets (>1M), use IndexIVFPQ for compression
index = faiss.IndexFlatIP(dimension)

# Add vectors with record IDs
index.add(embeddings.astype('float32'))

# Save index to disk
faiss.write_index(index, "/vector_db/entity_index.faiss")

# Store a mapping from FAISS internal ID → record ID
id_mapping = {i: record_id for i, record_id in enumerate(record_ids)}
```

### Step 5: Query for Similar Entities

```python
def find_similar(query_record, k=10, threshold=0.85):
    """Find the k most similar records to a query record."""
    query_text = record_to_text(query_record)
    query_vector = model.encode(
        [query_text],
        normalize_embeddings=True
    ).astype('float32')

    # ANN search
    scores, indices = index.search(query_vector, k)

    results = []
    for score, idx in zip(scores[0], indices[0]):
        if score >= threshold:
            results.append({
                "record_id": id_mapping[idx],
                "similarity": float(score),
                "record": records[id_mapping[idx]]
            })

    return results

# Example
query = {"company_name": "Acme SA", "city": "Johannesburg"}
matches = find_similar(query, k=5, threshold=0.85)
# Returns: "Acme South Africa Pty Ltd" (0.97), "ACME Corp" (0.72), ...
```

### Step 6: Hybrid Search (Vector + Keyword)

```python
def hybrid_search(query_record, k=10):
    """Combine vector similarity with exact keyword matches."""
    # Vector search (semantic similarity)
    vector_results = find_similar(query_record, k=k*2, threshold=0.70)

    # Keyword boost: exact match on critical fields
    for result in vector_results:
        record = result["record"]
        # Boost score if critical fields match exactly
        if record.get("phone") == query_record.get("phone"):
            result["similarity"] = min(1.0, result["similarity"] + 0.1)
        if record.get("email") == query_record.get("email"):
            result["similarity"] = min(1.0, result["similarity"] + 0.2)

    # Re-rank and return top k
    vector_results.sort(key=lambda r: r["similarity"], reverse=True)
    return vector_results[:k]
```

---

## Output Data State

| Attribute | Description |
|-----------|-------------|
| **Vector Index** | FAISS/Milvus index of all entity embeddings |
| **Embedding Vectors** | 384–1536 dimensional normalized vectors per record |
| **Similarity Scores** | Cosine similarity (0–1) between query and matched records |

---

## Key Technologies

| Technology | Role | Rationale |
|------------|------|-----------|
| **Sentence Transformers (BGE, E5)** | Embedding generation | State-of-the-art, open-source, multi-lingual |
| **FAISS** (Facebook AI Similarity Search) | Vector indexing | Fast, GPU-accelerated, battle-tested |
| **Milvus** (alternative) | Vector database | Distributed, cloud-native, managed option |
| **Pinecone / Weaviate** (alternative) | Managed vector DB | No infrastructure to manage |
| **OpenAI Embeddings** (alternative) | API-based embeddings | High quality, per-token cost |
| **Cohere Embed** (alternative) | API-based embeddings | Multi-lingual focus |

---

## Diagram

```mermaid
--8<-- "diagrams/phase-12-embedding-search.mmd"
```

---

## Example Code Reference

- [:material-code-tags: `examples/src/phase_12_embedding_search.py`](../../examples/README.md)
- [:material-notebook: `examples/notebooks/12-embedding-matching.ipynb`](../../examples/README.md)

---

## Quality Metrics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Recall@10 | ≥ 98% | True duplicate in top 10 results |
| Mean Reciprocal Rank (MRR) | ≥ 0.90 | Reciprocal rank of first correct match |
| Index query latency | < 100ms | Time from query to results (p99) |
| Index build time | < 1 hour (10M records) | Time to embed + index the full dataset |

---

## When to Advance

Move to Phase 13 when:

- [ ] Embeddings are generated for all entity records.
- [ ] A vector index is built and queryable.
- [ ] Similarity thresholds for matching are calibrated.
- [ ] The system supports incremental indexing (new records added without full rebuild).
- [ ] Query latency meets the application's SLA.

---

## Common Pitfalls

### 1. Embedding Without Entity Context

**Problem**: Embedding only `company_name` produces vectors that don't distinguish "Apple Inc. (technology)" from "Apple Farms (agriculture)." The names are identical but the entities are not.

**Fix**: Include multiple fields in the embedding text (name + address + city + industry). The more context in the embedding, the more discriminating it is. Use a consistent template: `"Field: value | Field: value"`.

### 2. The Wrong Similarity Metric

**Problem**: Using Euclidean distance on non-normalized embeddings. Two records with verbose descriptions have larger vector magnitudes and appear "farther" from everything, even when semantically similar.

**Fix**: Normalize embeddings (`normalize_embeddings=True`) and use cosine similarity (dot product on normalized vectors). This measures direction, not magnitude — a verbose and a terse description of the same entity point in the same direction.

### 3. Full Rebuild on Every Update

**Problem**: Adding 100 new records triggers a full index rebuild on 10 million records, taking hours.

**Fix**: Use incremental indexing. FAISS `IndexIVFPQ` supports adding vectors after initial training. For FAISS `IndexFlatIP`, append new vectors and re-save. For managed vector databases (Milvus, Pinecone), inserts are natively supported.

### 4. Single-Model Dependency

**Problem**: All embeddings use a single model trained on English web text. Non-English entity names (Chinese, Arabic, Cyrillic) embed poorly and produce random matches.

**Fix**: Use multi-lingual embedding models (BGE-M3, E5-multilingual, LaBSE). Test embedding quality on representative samples from each language/script in your data. If your data is dominantly non-English, consider fine-tuning an embedding model on your domain.

---

## Further Reading

- [BGE Embeddings (BAAI)](https://huggingface.co/BAAI/bge-large-en-v1.5)
- [FAISS: A Library for Efficient Similarity Search](https://engineering.fb.com/2017/03/29/data-infrastructure/faiss-a-library-for-efficient-similarity-search/)
- [Sentence Transformers Documentation](https://www.sbert.net/)
- [ANN Benchmarks](https://ann-benchmarks.com/)

---

[:material-arrow-left: Previous: Phase 11](phase-11-semantic-matching-llm.md) &nbsp;|&nbsp; [:material-arrow-right: Next: Phase 13](phase-13-golden-record-creation.md)
