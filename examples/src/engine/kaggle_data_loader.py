"""
Kaggle Dataset Loader

Integrates real-world datasets from Kaggle into the entity resolution
pipeline for testing and benchmarking. Datasets are loaded via the Kaggle
API, cached locally, and converted to Spark DataFrames with automatic
schema detection.

Supported datasets cover diverse entity types, formats, and quality levels:
- Customer/contact datasets (names, addresses, emails, phones)
- Product catalogs (SKUs, descriptions, categories, prices)
- Company/business datasets (firmographics, industries, financials)
- Dirty/duplicate-heavy datasets for stress-testing matching phases

Usage:
    from engine.kaggle_data_loader import KaggleDataLoader

    loader = KaggleDataLoader(spark, cache_dir="./data/kaggle")
    df = loader.load("zynicide/wine-reviews")  # Any Kaggle dataset path
    # or load a recommended dataset for a specific phase:
    df = loader.load_for_phase(phase=7, entity_type="customer")
"""

from __future__ import annotations

import os
import json
import glob
import hashlib
from pathlib import Path
from typing import Optional
from dataclasses import dataclass

from pyspark.sql import SparkSession, DataFrame

from utils.logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class KaggleDataset:
    """Metadata for a Kaggle dataset used in testing."""
    path: str           # Kaggle dataset path (owner/dataset-name)
    entity_type: str    # customer, product, company, etc.
    file_pattern: str   # File to load within the dataset
    phase_coverage: list[int]  # Which phases this dataset exercises
    record_count: int   # Approximate number of records
    quality_level: str  # clean, typical, dirty
    description: str
    has_duplicates: bool
    has_missing_values: bool


# ---------------------------------------------------------------------------
# Curated Kaggle datasets for entity resolution testing
# ---------------------------------------------------------------------------
CURATED_DATASETS: dict[str, KaggleDataset] = {
    # Customer/Contact datasets
    "customer_us_companies": KaggleDataset(
        path="peopledatalabssf/federal-contributors-2022",
        entity_type="customer",
        file_pattern="*.csv",
        phase_coverage=[1, 2, 3, 4, 6, 7, 8],
        record_count=50000,
        quality_level="typical",
        description="US company contact records with names, addresses, employers",
        has_duplicates=True,
        has_missing_values=True,
    ),
    "customer_wine_reviews": KaggleDataset(
        path="zynicide/wine-reviews",
        entity_type="customer",
        file_pattern="*.csv",
        phase_coverage=[1, 2, 4, 6, 7],
        record_count=150000,
        quality_level="clean",
        description="Wine reviews with reviewer names, winery, variety, price",
        has_duplicates=False,
        has_missing_values=True,
    ),
    "customer_amazon_reviews": KaggleDataset(
        path="arhamrumi/amazon-product-reviews",
        entity_type="customer",
        file_pattern="*.csv",
        phase_coverage=[1, 2, 3, 4, 6, 7, 8, 12],
        record_count=500000,
        quality_level="typical",
        description="Amazon product reviews with reviewer names, IDs, ratings",
        has_duplicates=True,
        has_missing_values=True,
    ),
    # Product datasets
    "product_ecommerce": KaggleDataset(
        path="carrie1/ecommerce-data",
        entity_type="product",
        file_pattern="*.csv",
        phase_coverage=[1, 2, 3, 4, 5, 6, 7, 8],
        record_count=500000,
        quality_level="typical",
        description="Online retail transactions with product descriptions, prices, quantities",
        has_duplicates=True,
        has_missing_values=True,
    ),
    "product_amazon_metadata": KaggleDataset(
        path="beridzeg45/amazon-meta-data",
        entity_type="product",
        file_pattern="*.json",
        phase_coverage=[1, 2, 4, 6, 7, 8, 12],
        record_count=120000,
        quality_level="typical",
        description="Amazon product metadata with titles, descriptions, categories",
        has_duplicates=True,
        has_missing_values=True,
    ),
    # Company/Business datasets
    "company_fortune500": KaggleDataset(
        path="mikhail0503/fortune500-dataset",
        entity_type="company",
        file_pattern="*.csv",
        phase_coverage=[1, 2, 4, 5, 6, 7, 8],
        record_count=500,
        quality_level="clean",
        description="Fortune 500 companies with revenue, profit, employees, industry",
        has_duplicates=False,
        has_missing_values=False,
    ),
    "company_unicorn": KaggleDataset(
        path="niekvanderzwaag/unicorn-companies",
        entity_type="company",
        file_pattern="*.csv",
        phase_coverage=[1, 2, 4, 6, 7, 8],
        record_count=1000,
        quality_level="clean",
        description="Unicorn startups with valuation, industry, country, investors",
        has_duplicates=False,
        has_missing_values=True,
    ),
    # Dirty/dedup challenge datasets
    "dirty_citations": KaggleDataset(
        path="rhtsingh/citation-data-set-for-record-linkage",
        entity_type="generic",
        file_pattern="*.csv",
        phase_coverage=[1, 2, 3, 4, 6, 7, 8, 9, 10, 11],
        record_count=2500,
        quality_level="dirty",
        description="Citation records with intentional duplicates, abbreviations, typos",
        has_duplicates=True,
        has_missing_values=True,
    ),
    "dirty_restaurants": KaggleDataset(
        path="zynicide/fodors-and-zagats-restaurants",
        entity_type="generic",
        file_pattern="*.csv",
        phase_coverage=[6, 7, 8, 9, 10],
        record_count=850,
        quality_level="dirty",
        description="Restaurant records from two sources with duplicates and variations",
        has_duplicates=True,
        has_missing_values=True,
    ),
    "dirty_music": KaggleDataset(
        path="nikdavis/steam-store-reviews",
        entity_type="generic",
        file_pattern="*.csv",
        phase_coverage=[1, 2, 3, 4, 6, 7, 8, 9, 10, 12],
        record_count=100000,
        quality_level="typical",
        description="Steam game reviews with reviewer names, game titles, text reviews",
        has_duplicates=True,
        has_missing_values=True,
    ),
    # Large-scale stress test datasets
    "large_flights": KaggleDataset(
        path="usdot/flight-delays",
        entity_type="generic",
        file_pattern="*.csv",
        phase_coverage=[1, 2, 3, 4, 8],
        record_count=5000000,
        quality_level="clean",
        description="US flight delay data — stress test for ingestion and blocking",
        has_duplicates=False,
        has_missing_values=True,
    ),
    "large_nyc_taxi": KaggleDataset(
        path="elem84/nyc-taxi-trip-data",
        entity_type="generic",
        file_pattern="*.csv",
        phase_coverage=[1, 2, 3, 4, 8],
        record_count=1000000,
        quality_level="clean",
        description="NYC taxi trip data — stress test for large-scale ingestion",
        has_duplicates=False,
        has_missing_values=True,
    ),
}

# Phase-specific dataset recommendations
PHASE_DATASET_MAP: dict[int, list[str]] = {
    1: ["customer_wine_reviews", "product_ecommerce", "large_flights"],
    2: ["customer_wine_reviews", "company_fortune500", "product_ecommerce"],
    3: ["dirty_citations", "dirty_restaurants", "customer_amazon_reviews"],
    4: ["customer_us_companies", "dirty_citations", "product_ecommerce"],
    5: ["company_fortune500", "company_unicorn"],
    6: ["customer_us_companies", "dirty_citations", "dirty_restaurants"],
    7: ["dirty_citations", "dirty_restaurants", "dirty_music"],
    8: ["large_flights", "large_nyc_taxi", "customer_amazon_reviews"],
    9: ["dirty_citations", "dirty_restaurants"],
    10: ["dirty_citations", "dirty_restaurants"],
    11: ["dirty_citations", "dirty_music"],
    12: ["product_amazon_metadata", "customer_amazon_reviews", "dirty_music"],
    13: ["customer_us_companies", "product_ecommerce"],
    14: ["dirty_citations", "dirty_restaurants"],
    15: ["customer_us_companies", "company_fortune500"],
}


class KaggleDataLoader:
    """
    Loads real Kaggle datasets for entity resolution pipeline testing.

    Handles:
    - Kaggle API authentication
    - Dataset download and caching
    - Auto-detection of file format and schema
    - Conversion to Spark DataFrame
    - Dataset recommendations for specific phases and entity types
    """

    def __init__(
        self,
        spark: SparkSession,
        cache_dir: str = "./data/kaggle",
        kaggle_username: Optional[str] = None,
        kaggle_key: Optional[str] = None,
    ):
        """
        Initialize the Kaggle data loader.

        Args:
            spark: Spark session.
            cache_dir: Directory to cache downloaded datasets.
            kaggle_username: Kaggle API username (or from KAGGLE_USERNAME env var).
            kaggle_key: Kaggle API key (or from KAGGLE_KEY env var).
        """
        self.spark = spark
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        self.kaggle_username = kaggle_username or os.environ.get("KAGGLE_USERNAME")
        self.kaggle_key = kaggle_key or os.environ.get("KAGGLE_KEY")

        self._kaggle_api = None
        self._downloaded: set[str] = set()

    @property
    def kaggle_api(self):
        """Lazy-initialize Kaggle API client."""
        if self._kaggle_api is None:
            try:
                from kaggle.api.kaggle_api_extended import KaggleApi
                api = KaggleApi()
                api.authenticate()
                self._kaggle_api = api
            except ImportError:
                logger.warning(
                    "kaggle package not installed. Install with: "
                    "pip install kaggle"
                )
                return None
            except Exception as e:
                logger.warning(f"Kaggle authentication failed: {e}")
                return None
        return self._kaggle_api

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(
        self,
        dataset_path: str,
        file_pattern: str = "*.csv",
        sample_ratio: Optional[float] = None,
        force_download: bool = False,
    ) -> DataFrame:
        """
        Load any Kaggle dataset as a Spark DataFrame.

        Args:
            dataset_path: Kaggle dataset path (e.g., 'zynicide/wine-reviews').
            file_pattern: Glob pattern for files to load within the dataset.
            sample_ratio: Optional fraction of data to sample (0.0-1.0).
            force_download: Re-download even if cached.

        Returns:
            Spark DataFrame.
        """
        local_path = self._ensure_downloaded(dataset_path, force_download)

        # Find matching files
        matching = sorted(glob.glob(str(local_path / "**" / file_pattern), recursive=True))
        if not matching:
            # Try broader patterns
            for pat in ["*.csv", "*.json", "*.jsonl", "*.parquet", "*.tsv"]:
                matching = sorted(glob.glob(str(local_path / "**" / pat), recursive=True))
                if matching:
                    break

        if not matching:
            raise FileNotFoundError(
                f"No files matching '{file_pattern}' found in {local_path}. "
                f"Available: {list(local_path.rglob('*'))[:20]}"
            )

        # Load the largest file (usually the main data file)
        target_file = max(matching, key=os.path.getsize)
        logger.info(f"Loading {target_file} ({os.path.getsize(target_file) / 1e6:.1f} MB)")

        df = self._auto_read(target_file)

        if sample_ratio and 0.0 < sample_ratio < 1.0:
            df = df.sample(fraction=sample_ratio, seed=42)
            logger.info(f"Sampled {sample_ratio:.1%}: {df.count()} rows")

        return df

    def load_curated(self, dataset_key: str, **kwargs) -> DataFrame:
        """
        Load one of the curated datasets by key.

        Args:
            dataset_key: Key from CURATED_DATASETS (e.g., 'customer_wine_reviews').
            **kwargs: Passed to load().

        Returns:
            Spark DataFrame.
        """
        if dataset_key not in CURATED_DATASETS:
            available = "\n  ".join(CURATED_DATASETS.keys())
            raise ValueError(
                f"Unknown dataset key: '{dataset_key}'. Available:\n  {available}"
            )

        ds = CURATED_DATASETS[dataset_key]
        return self.load(ds.path, ds.file_pattern, **kwargs)

    def load_for_phase(
        self,
        phase: int,
        entity_type: Optional[str] = None,
        **kwargs,
    ) -> DataFrame:
        """
        Load the best dataset for testing a specific phase.

        Args:
            phase: Phase number (1-15).
            entity_type: Optional entity type filter.
            **kwargs: Passed to load().

        Returns:
            Spark DataFrame.
        """
        candidates = PHASE_DATASET_MAP.get(phase, ["customer_wine_reviews"])

        # Filter by entity type if specified
        if entity_type:
            candidates = [
                k for k in candidates
                if CURATED_DATASETS[k].entity_type == entity_type
                or CURATED_DATASETS[k].entity_type == "generic"
            ]

        if not candidates:
            candidates = PHASE_DATASET_MAP.get(phase, ["customer_wine_reviews"])

        dataset_key = candidates[0]
        logger.info(f"Phase {phase}: loading '{dataset_key}' ({CURATED_DATASETS[dataset_key].description})")
        return self.load_curated(dataset_key, **kwargs)

    def load_test_suite(
        self,
        entity_types: Optional[list[str]] = None,
        phases: Optional[list[int]] = None,
    ) -> dict[str, DataFrame]:
        """
        Load a comprehensive test suite with datasets for all phases.

        Args:
            entity_types: Entity types to include (default: all).
            phases: Phases to include (default: all 15).

        Returns:
            Dict mapping dataset_key -> DataFrame.
        """
        if entity_types is None:
            entity_types = ["customer", "product", "company"]

        if phases is None:
            phases = list(range(1, 16))

        datasets = {}
        for phase in phases:
            for entity_type in entity_types:
                candidates = [
                    k for k in PHASE_DATASET_MAP.get(phase, [])
                    if CURATED_DATASETS[k].entity_type in (entity_type, "generic")
                ]
                if candidates:
                    key = candidates[0]
                    if key not in datasets:
                        try:
                            datasets[key] = self.load_curated(key)
                        except Exception as e:
                            logger.warning(f"Failed to load {key}: {e}")
                            continue

        return datasets

    def list_datasets(self) -> list[KaggleDataset]:
        """List all available curated datasets with metadata."""
        return list(CURATED_DATASETS.values())

    def print_dataset_catalog(self) -> None:
        """Print a formatted catalog of all curated datasets."""
        print(f"\n{'='*80}")
        print("KAGGLE DATASET CATALOG — Entity Resolution Testing")
        print(f"{'='*80}")
        for key, ds in CURATED_DATASETS.items():
            print(f"\n  [{ds.entity_type.upper()}] {key}")
            print(f"    Path:        {ds.path}")
            print(f"    Records:     {ds.record_count:,}")
            print(f"    Quality:     {ds.quality_level}")
            print(f"    Phases:      {ds.phase_coverage}")
            print(f"    Duplicates:  {ds.has_duplicates}")
            print(f"    Missing:     {ds.has_missing_values}")
            print(f"    {ds.description}")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _ensure_downloaded(self, dataset_path: str, force: bool = False) -> Path:
        """Download a Kaggle dataset to the cache directory."""
        dataset_dir = self.cache_dir / dataset_path.replace("/", "_")
        cache_marker = dataset_dir / ".download_complete"

        if not force and cache_marker.exists():
            logger.debug(f"Using cached dataset: {dataset_dir}")
            return dataset_dir

        api = self.kaggle_api
        if api is None:
            if dataset_dir.exists() and any(dataset_dir.iterdir()):
                logger.warning("Kaggle API unavailable; using cached data if available")
                return dataset_dir
            raise RuntimeError(
                "Kaggle API not available. Install kaggle package and set "
                "KAGGLE_USERNAME and KAGGLE_KEY environment variables."
            )

        owner, name = dataset_path.split("/")
        logger.info(f"Downloading {dataset_path} to {dataset_dir}...")

        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            api.dataset_download_files(
                dataset_path,
                path=tmp,
                unzip=True,
                quiet=False,
            )
            # Move files to cache
            dataset_dir.mkdir(parents=True, exist_ok=True)
            for f in Path(tmp).rglob("*"):
                if f.is_file():
                    dest = dataset_dir / f.name
                    f.rename(dest)

        cache_marker.touch()
        logger.info(f"Downloaded to {dataset_dir}")
        return dataset_dir

    def _auto_read(self, path: str) -> DataFrame:
        """Auto-detect format and read file into Spark DataFrame."""
        path_lower = path.lower()

        if path_lower.endswith(".csv"):
            return self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("escape", "\"") \
                .option("multiLine", "true") \
                .option("mode", "PERMISSIVE") \
                .csv(path)

        elif path_lower.endswith(".tsv") or path_lower.endswith(".tab"):
            return self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("sep", "\t") \
                .csv(path)

        elif path_lower.endswith(".json") or path_lower.endswith(".jsonl"):
            return self.spark.read \
                .option("multiline", "true") \
                .option("mode", "PERMISSIVE") \
                .json(path)

        elif path_lower.endswith(".parquet"):
            return self.spark.read.parquet(path)

        elif path_lower.endswith(".orc"):
            return self.spark.read.orc(path)

        else:
            # Try CSV first, then JSON
            try:
                return self.spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .csv(path)
            except Exception:
                return self.spark.read \
                    .option("multiline", "true") \
                    .json(path)
