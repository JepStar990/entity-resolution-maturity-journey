# Glossary

## A

**Active Learning**
: A machine learning approach where the model selects the most informative unlabeled examples for human labeling, maximizing the value of each labeled instance. Used in Phase 14 to prioritize steward review tasks.

**ANN (Approximate Nearest Neighbor)**
: A search algorithm that finds approximate (not exact) nearest neighbors in a vector space, trading a small amount of accuracy for orders-of-magnitude speed improvements. Used in Phase 12 with FAISS.

## B

**Blocking**
: The process of grouping records into subsets (blocks) and only comparing records within the same block. Reduces pairwise comparisons from O(n^2) to O(n * b). Phase 8.

**Blocking Key**
: The attribute(s) used to partition records into blocks. Example: postal code, first letter of name, Soundex code. A good blocking key has high coverage and produces blocks of manageable size (100–1000 records).

**Bronze Layer**
: The first layer of the medallion architecture. Stores raw, immutable data exactly as received from source systems. Phases 1–3.

## C

**Candidate Pair**
: Two records that might represent the same entity, selected for comparison by blocking or fuzzy matching. Not all candidate pairs are actual matches.

**Canopy Clustering**
: A blocking technique that uses a cheap distance metric to create overlapping clusters (canopies). Records within the same canopy are compared with expensive metrics. Phase 8.

**CDC (Change Data Capture)**
: A technique for capturing changes made to a database and delivering them to downstream systems in real time. Used in Phase 15 for event-driven MDM distribution.

**Composite Key**
: A business key composed of multiple columns (e.g., email + phone) to uniquely identify an entity. More reliable than using a single column. Phase 6.

**Connected Components**
: A graph algorithm that finds clusters of connected nodes. In entity resolution, matched record pairs form a graph; connected components represent entity clusters. Phase 8 (transitive closure).

**Cosine Similarity**
: A measure of similarity between two vectors, calculated as the cosine of the angle between them. Range: [-1, 1]. Used in Phase 12 for embedding comparison. On normalized vectors, cosine similarity equals dot product.

## D

**Data Contract**
: A formal agreement between a data producer and data consumer specifying the schema, freshness, completeness, and availability SLAs. Phase 15.

**Data Quality Score**
: A numeric measure of a record's trustworthiness, used as a tiebreaker in survivorship rules (prefer records with higher scores). Generated in Phase 3.

**Data Steward**
: A business user who reviews uncertain matches, resolves conflicts, and provides the human judgment that algorithms cannot. Phase 14.

**Delta Lake**
: An open-source storage layer that brings ACID transactions to data lakes. Supports time travel, schema enforcement, and efficient upserts/merges. The recommended storage format for all pipeline layers.

## E

**E.164**
: The international standard for phone number format. Example: `+27821234567`. Standardization in Phase 4 should target E.164 compliance.

**Embedding**
: A dense vector representation of text (or other data) that captures semantic meaning. Similar texts produce nearby vectors. Phase 12.

**Entity Resolution (ER)**
: The process of identifying which records refer to the same real-world entity across different data sources. Also called record linkage, deduplication, or matching.

**Exact Deduplication**
: Removing records that are exactly identical or identical on a defined business key. The simplest form of deduplication. Phase 6.

**Expectation (Great Expectations)**
: A declarative statement about what data should look like (e.g., "column `age` should contain values between 0 and 120"). Used in Phase 3.

## F

**False Positive (in ER)**
: Two records incorrectly identified as the same entity. The most costly error type — merging unrelated entities corrupts the golden record.

**False Negative (in ER)**
: Two records that represent the same entity but were not matched. Results in duplicate golden records.

**Feature Vector**
: An array of numeric values representing the similarity between two records across multiple dimensions. Input to ML models in Phase 10.

**Fellegi-Sunter Model**
: The foundational statistical framework for record linkage (1969). Estimates match probability based on agreement/disagreement patterns across multiple fields.

**FAISS (Facebook AI Similarity Search)**
: An open-source library for efficient similarity search and clustering of dense vectors. Used in Phase 12.

## G

**Golden Record**
: The single, trusted representation of an entity created by merging matched records and applying survivorship rules. The "single version of truth." Phase 13.

**Great Expectations**
: An open-source Python library for defining, running, and documenting data quality expectations. Used in Phase 3.

## J

**Jaccard Similarity**
: The size of the intersection divided by the size of the union of two sets. Used for token-based address comparison in Phase 7.

**Jaro-Winkler Similarity**
: A string similarity metric that measures the edit distance between two strings, with a bias toward strings that match from the beginning. Range: [0, 1]. Excellent for personal names. Phase 7.

## L

**Levenshtein Distance**
: The minimum number of single-character edits (insertions, deletions, substitutions) required to change one string into another. Phase 7.

**LLM (Large Language Model)**
: A deep learning model trained on massive text corpora that can understand and generate human-like text. Used in Phase 11 for semantic entity matching.

## M

**Master Data Management (MDM)**
: The discipline of creating and maintaining a single, trusted view of critical business entities (customers, suppliers, products, locations) across the enterprise. Phase 15.

**Medallion Architecture**
: A data organization pattern (Bronze → Silver → Gold) where data quality and structure improve at each layer. Implemented by Databricks and Microsoft Fabric.

**MLflow**
: An open-source platform for managing the ML lifecycle, including experiment tracking, model registry, and deployment. Natively supported in Microsoft Fabric. Used in Phase 10.

**OneLake**
: Microsoft Fabric's built-in SaaS data lake. A single, unified storage layer for all Fabric workloads — Lakehouses, Warehouses, and KQL Databases. Delta Lake tables stored in OneLake are automatically accessible across workspaces via shortcuts without data duplication.

## N

**NFKC Normalization**
: A Unicode normalization form that decomposes characters and applies compatibility composition. Converts ligatures (ﬁ → fi), fullwidth characters, and special symbols to their canonical forms. Phase 4.

## P

**Precision**
: True positives / (True positives + False positives). The fraction of predicted matches that are actual matches. High precision = few false positives.

**Probability Calibration**
: Adjusting model-predicted probabilities so they match empirical frequencies. A well-calibrated model's "90% confidence" predictions are correct 90% of the time.

## R

**Recall**
: True positives / (True positives + False negatives). The fraction of actual matches that were found. High recall = few missed matches.

**Record Linkage**
: See Entity Resolution.

## S

**SCD (Slowly Changing Dimension)**
: A dimension (reference data) whose attributes change slowly over time. SCD Type 1 overwrites old values; Type 2 preserves history. Phase 5.

**Silver Layer**
: The second layer of the medallion architecture. Stores cleansed, validated, standardized, and deduplicated data. Phases 4–8.

**Sorted Neighborhood Method**
: A blocking technique that sorts records by a key, then slides a fixed-size window over the sorted list. Only records within the same window are compared. Phase 8.

**Soundex**
: A phonetic algorithm that encodes words by how they sound. `Smith` and `Smyth` produce the same Soundex code. Phase 7.

**Survivorship**
: The rules that determine which value is selected when matched records disagree on a field. Strategies: most recent, most frequent, most trusted source. Phase 13.

## T

**Transitive Closure**
: The logical inference that if A matches B, and B matches C, then A matches C (even if A and C were never directly compared). Implemented via connected components on a match graph. Phase 8.

## V

**Vector Database**
: A database optimized for storing and searching vector embeddings. Supports ANN queries for finding similar vectors. Examples: FAISS, Milvus, Pinecone, Weaviate. Phase 12.
