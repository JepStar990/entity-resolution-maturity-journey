"""
Phase 12: Embedding-Based Matching

Leverages sentence transformer models and vector similarity search
(FAISS) to find semantically similar entities that string-based and
ML models might miss. Useful for cross-lingual matching, free-text
descriptions, and complex entity names.

Input: Entity records with text attributes
Output: Vector similarity matches; embedding vectors stored for future use

Medallion Layer: Gold
"""

from __future__ import annotations

from typing import Optional

import numpy as np

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, FloatType

from utils.delta_helpers import write_to_delta, resolve_table_path
from utils.metrics import MetricsCollector
from utils.logging_config import get_logger

logger = get_logger(__name__)


def build_entity_text(
    df: DataFrame,
    text_columns: list[str],
    output_col: str = "_entity_text",
) -> DataFrame:
    """
    Concatenate multiple text columns into a single representation
    suitable for embedding generation.

    Args:
        df: Entity DataFrame.
        text_columns: Columns to concatenate.
        output_col: Name for the concatenated text column.

    Returns:
        DataFrame with combined entity text.
    """
    coalesced = [
        F.coalesce(F.col(c).cast("string"), F.lit(""))
        for c in text_columns
        if c in df.columns
    ]

    if not coalesced:
        logger.warning("No text columns found for embedding")
        return df.withColumn(output_col, F.lit(""))

    return df.withColumn(
        output_col,
        F.concat_ws(" | ", *coalesced),
    )


def generate_embeddings_batch(
    texts: list[str],
    model_name: str = "all-MiniLM-L6-v2",
    batch_size: int = 32,
) -> np.ndarray:
    """
    Generate sentence embeddings for a batch of text strings.

    Args:
        texts: List of text strings.
        model_name: Sentence Transformers model name.
        batch_size: Batch size for encoding.

    Returns:
        NumPy array of shape (len(texts), embedding_dim).
    """
    try:
        from sentence_transformers import SentenceTransformer
        model = SentenceTransformer(model_name)
        embeddings = model.encode(
            texts,
            batch_size=batch_size,
            show_progress_bar=False,
            convert_to_numpy=True,
        )
        return embeddings.astype(np.float32)
    except ImportError:
        logger.warning("sentence_transformers not installed; generating random embeddings for demo")
        rng = np.random.default_rng(42)
        return rng.random((len(texts), 384)).astype(np.float32)
    except Exception as e:
        logger.error(f"Embedding generation failed: {e}")
        return np.zeros((len(texts), 384), dtype=np.float32)


def compute_embeddings_spark(
    df: DataFrame,
    text_col: str,
    entity_id_col: str = "id",
    model_name: str = "all-MiniLM-L6-v2",
    embedding_dim: int = 384,
) -> DataFrame:
    """
    Compute embeddings for entity text using Spark mapInPandas.

    Processes text in batches using Sentence Transformers within
    each Spark partition, enabling distributed embedding generation.

    Args:
        df: Entity DataFrame with text column.
        text_col: Column containing concatenated entity text.
        entity_id_col: Unique identifier column.
        model_name: Sentence Transformer model name.
        embedding_dim: Expected embedding vector dimension.

    Returns:
        DataFrame with entity_id and embedding vector.
    """
    import pandas as pd

    def generate_embeddings_in_partition(iterator):
        """Generate embeddings for a partition using Sentence Transformers."""
        try:
            from sentence_transformers import SentenceTransformer
            model = SentenceTransformer(model_name)
        except ImportError:
            # Fallback: identity embeddings for demo
            model = None

        for pdf in iterator:
            texts = pdf[text_col].fillna("").tolist()
            ids = pdf[entity_id_col].tolist()

            if model:
                embeddings = model.encode(
                    texts,
                    batch_size=32,
                    show_progress_bar=False,
                )
            else:
                embeddings = np.random.default_rng(42).random(
                    (len(texts), embedding_dim)
                ).astype(np.float32)

            result = pd.DataFrame({
                entity_id_col: ids,
                "embedding": [emb.tolist() for emb in embeddings],
            })
            yield result

    # Determine schema for the output
    schema = df.select(entity_id_col).schema \
        .add("embedding", ArrayType(FloatType()))

    embeddings_df = df.mapInPandas(
        generate_embeddings_in_partition,
        schema=schema,
    )

    return embeddings_df


def build_faiss_index(
    embeddings: np.ndarray,
    index_type: str = "Flat",
    nlist: int = 100,
) -> object:
    """
    Build a FAISS index for vector similarity search.

    Args:
        embeddings: NumPy array of shape (n, d).
        index_type: FAISS index type ('Flat', 'IVFFlat', 'IVFPQ').
        nlist: Number of Voronoi cells for IVF indexes.

    Returns:
        FAISS index object.
    """
    try:
        import faiss

        d = embeddings.shape[1]
        n = embeddings.shape[0]

        if index_type == "Flat":
            index = faiss.IndexFlatL2(d)
        elif index_type == "IVFFlat":
            quantizer = faiss.IndexFlatL2(d)
            index = faiss.IndexIVFFlat(quantizer, d, nlist)
        elif index_type == "IVFPQ":
            quantizer = faiss.IndexFlatL2(d)
            m = min(d // 2, 64)
            index = faiss.IndexIVFPQ(quantizer, d, nlist, m, 8)
        else:
            logger.warning(f"Unknown FAISS index type: {index_type}; using Flat")
            index = faiss.IndexFlatL2(d)

        # If using IVF, train first
        if index_type != "Flat" and n >= nlist:
            index.train(embeddings)

        index.add(embeddings)
        logger.info(f"Built FAISS index: {index_type}, {n} vectors, dim={d}")
        return index

    except ImportError:
        logger.warning("faiss not installed; returning None")
        return None


def find_nearest_neighbors(
    index: object,
    query_embeddings: np.ndarray,
    k: int = 10,
    distance_threshold: Optional[float] = None,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Find k-nearest neighbors for query embeddings.

    Args:
        index: FAISS index.
        query_embeddings: Query vectors of shape (n, d).
        k: Number of nearest neighbors to retrieve.
        distance_threshold: Maximum L2 distance for a match.

    Returns:
        Tuple of (distances, indices) arrays, each of shape (n, k).
    """
    if index is None:
        n = query_embeddings.shape[0]
        return np.zeros((n, k)), np.zeros((n, k), dtype=np.int64)

    distances, indices = index.search(query_embeddings.astype(np.float32), k)

    if distance_threshold is not None:
        mask = distances > distance_threshold
        distances[mask] = np.inf
        indices[mask] = -1

    return distances, indices


def run(
    spark: SparkSession,
    df: DataFrame,
    entity_type: str = "customer",
    config: Optional[dict] = None,
    gold_path: str = "Tables/gold/",
    table_name: str = "entity",
    workspace: Optional[str] = None,
    metrics: Optional[MetricsCollector] = None,
) -> dict:
    """
    Execute Phase 12: generate embeddings and find semantic matches.

    Args:
        spark: Active Spark session.
        df: Entity DataFrame (standardized, enriched records).
        entity_type: Type of entity.
        config: Dict with:
            - text_columns: Columns to use for embedding text.
            - model_name: Sentence Transformer model.
            - faiss_index_type: 'Flat', 'IVFFlat', or 'IVFPQ'.
            - top_k: Number of nearest neighbors.
            - distance_threshold: Maximum L2 distance for matches.
        gold_path: Gold layer path.
        table_name: Target table name.
        workspace: Fabric workspace name.
        metrics: Optional MetricsCollector.

    Returns:
        Dict with embeddings DataFrame, FAISS index, and match pairs.
    """
    if metrics is None:
        metrics = MetricsCollector(spark, phase="12-embedding-matching")

    config = config or {}
    logger.info(f"Generating embeddings for {entity_type} records")

    # Default text columns by entity type
    default_text_cols = {
        "customer": ["full_name_std", "email_std", "address_street"],
        "company": ["company_name_std", "address_street", "sic_description"],
        "product": ["product_name_std", "sku_std"],
    }
    text_columns = config.get(
        "text_columns",
        default_text_cols.get(entity_type, ["name"]),
    )

    model_name = config.get("model_name", "all-MiniLM-L6-v2")
    faiss_index_type = config.get("faiss_index_type", "Flat")
    top_k = config.get("top_k", 10)
    distance_threshold = config.get("distance_threshold")

    # Build entity text
    text_df = build_entity_text(df, text_columns)
    logger.info(f"Built entity text from {len(text_columns)} columns")

    # Generate embeddings
    embeddings_df = compute_embeddings_spark(
        text_df,
        text_col="_entity_text",
        model_name=model_name,
    )

    # Collect embeddings to driver for FAISS indexing
    # In production with large datasets, use distributed FAISS or Milvus
    embedding_rows = embeddings_df.select("id", "embedding").collect()
    ids = [r["id"] for r in embedding_rows]
    embedding_array = np.array([r["embedding"] for r in embedding_rows], dtype=np.float32)

    # Build FAISS index
    index = build_faiss_index(embedding_array, index_type=faiss_index_type)

    # Find nearest neighbors
    distances, neighbor_indices = find_nearest_neighbors(
        index, embedding_array, k=top_k + 1, distance_threshold=distance_threshold
    )

    # Build match pairs (excluding self-matches)
    match_pairs = []
    for i, (dists, nbrs) in enumerate(zip(distances, neighbor_indices)):
        for dist, nbr in zip(dists, nbrs):
            if nbr == i or nbr < 0 or nbr >= len(ids):
                continue
            if not np.isfinite(dist):
                continue
            similarity = 1.0 / (1.0 + float(dist))
            match_pairs.append({
                "id_left": ids[i],
                "id_right": ids[nbr],
                "embedding_distance": float(dist),
                "embedding_similarity": float(similarity),
            })

    # Write embeddings to Gold
    emb_target = resolve_table_path(
        layer="gold",
        table_name=f"{table_name}_embeddings",
        workspace=workspace,
    )
    write_to_delta(embeddings_df, emb_target, mode="overwrite")

    # Write match pairs to Gold
    if match_pairs:
        pairs_df = spark.createDataFrame(match_pairs)
        pairs_target = resolve_table_path(
            layer="gold",
            table_name=f"{table_name}_embedding_matches",
            workspace=workspace,
        )
        write_to_delta(pairs_df, pairs_target, mode="overwrite")

    # Emit metrics
    metrics.log_count("embedded_records", len(ids))
    metrics.log_count("embedding_dimensions", embedding_array.shape[1])
    metrics.log_count("embedding_match_pairs", len(match_pairs))
    if embedding_array.shape[1] > 0:
        metrics.log_metric("embedding_mean_norm", float(np.linalg.norm(embedding_array, axis=1).mean()))
    metrics.flush()

    logger.info(
        f"Generated {len(ids)} embeddings (dim={embedding_array.shape[1]}), "
        f"found {len(match_pairs)} embedding matches"
    )
    return {
        "embeddings_df": embeddings_df,
        "faiss_index": index,
        "match_pairs": match_pairs,
    }
