# Bibliography

A curated collection of foundational papers, books, and articles that inform the entity resolution maturity model.

---

## Foundational Papers

### Record Linkage Theory

- **Fellegi, I. P., & Sunter, A. B. (1969).** "A Theory for Record Linkage." *Journal of the American Statistical Association*, 64(328), 1183–1210.
  - The foundational paper that established the probabilistic framework for record linkage. Introduces the concept of matching weights and decision thresholds. Still the theoretical basis for most modern ER systems.

- **Newcombe, H. B., et al. (1959).** "Automatic Linkage of Vital Records." *Science*, 130(3381), 954–959.
  - One of the earliest applications of computerized record linkage, linking birth and marriage records using probabilistic methods.

### Blocking and Scalability

- **Hernández, M. A., & Stolfo, S. J. (1995).** "The Merge/Purge Problem for Large Databases." *ACM SIGMOD Record*, 24(2), 127–138.
  - Introduces the Sorted Neighborhood Method for blocking. The first paper to address the O(n^2) scalability problem systematically.

- **Christen, P. (2012).** "A Survey of Indexing Techniques for Scalable Record Linkage and Deduplication." *IEEE Transactions on Knowledge and Data Engineering*, 24(9), 1537–1555.
  - Comprehensive survey of blocking and indexing techniques, including sorted neighborhood, canopy clustering, and suffix-array-based methods.

- **McCallum, A., Nigam, K., & Ungar, L. H. (2000).** "Efficient Clustering of High-Dimensional Data Sets with Application to Reference Matching." *ACM SIGKDD*, 169–178.
  - Introduces Canopy Clustering as a pre-clustering method for reducing pairwise comparisons in high-dimensional spaces.

### Machine Learning for Entity Resolution

- **Christen, P. (2008).** "Automatic Record Linkage using Seeded Nearest Neighbour and Support Vector Machine Classification." *ACM SIGKDD*, 151–159.
  - One of the first papers to apply SVM to record linkage, with an active learning approach for generating training data.

- **Bilenko, M., & Mooney, R. J. (2003).** "Adaptive Duplicate Detection Using Learnable String Similarity Measures." *ACM SIGKDD*, 39–48.
  - Introduces learnable similarity measures that combine multiple string metrics using SVM, trained on labeled duplicate/non-duplicate pairs.

### Deep Learning and LLMs for ER

- **Mudgal, S., et al. (2018).** "Deep Learning for Entity Matching: A Design Space Exploration." *ACM SIGMOD*, 19–34.
  - Comprehensive exploration of deep learning architectures (RNN, CNN, hybrid) for entity matching, comparing them to traditional methods.

- **Li, Y., et al. (2020).** "Deep Entity Matching with Pre-Trained Language Models." *VLDB*, 13(12), 2457–2470.
  - Applies BERT and other pre-trained language models to entity matching, showing significant improvements on benchmark datasets.

- **Narayan, A., et al. (2023).** "Can Foundation Models Wrangle Your Data?" *VLDB*, 16(4), 738–746.
  - Evaluates GPT-3.5 and GPT-4 on data wrangling tasks including entity matching. Finds LLMs competitive with supervised methods, particularly in low-label regimes.

### Embeddings for Entity Resolution

- **Ebraheem, M., et al. (2018).** "DeepER — Deep Entity Resolution." *arXiv:1710.00597*.
  - Proposes using RNN/LSTM to embed entity records into a shared vector space, then using cosine similarity for matching. Early work on embedding-based ER.

- **Reimers, N., & Gurevych, I. (2019).** "Sentence-BERT: Sentence Embeddings using Siamese BERT-Networks." *EMNLP-IJCNLP*, 3982–3992.
  - Introduces Sentence-BERT, enabling efficient semantic similarity search with BERT embeddings. The foundation for modern embedding-based ER (Phase 12).

---

## Books

- **Christen, P. (2012).** *Data Matching: Concepts and Techniques for Record Linkage, Entity Resolution, and Duplicate Detection.* Springer.
  - The definitive textbook on entity resolution. Covers all phases: data preparation, blocking, string comparison, classification, clustering, and evaluation.

- **Berson, A., & Dubov, L. (2011).** *Master Data Management and Data Governance.* McGraw-Hill.
  - Comprehensive guide to MDM strategy, governance, and implementation in the enterprise.

- **Loshin, D. (2010).** *Master Data Management.* Morgan Kaufmann.
  - Practitioner-focused guide covering MDM architecture, data quality, identity resolution, and organizational adoption.

- **Kimball, R., & Ross, M. (2013).** *The Data Warehouse Toolkit: The Definitive Guide to Dimensional Modeling.* Wiley.
  - The foundational text on dimensional modeling. Covers slowly changing dimensions (SCDs) used in Phase 5 enrichment.

---

## Articles and Blog Posts

### Architecture

- **Databricks. (2021).** "The Medallion Architecture." [databricks.com/glossary/medallion-architecture](https://www.databricks.com/glossary/medallion-architecture)
  - Introduces and explains the Bronze → Silver → Gold data organization pattern used throughout this maturity model.

- **Microsoft. (2024).** "Microsoft Fabric Lakehouse Architecture." [learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-architecture](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-architecture)
  - Microsoft Fabric's native implementation of the medallion architecture with OneLake as the unified storage layer.

- **Microsoft. (2024).** "What is Microsoft Fabric?" [learn.microsoft.com/en-us/fabric/get-started/microsoft-fabric-overview](https://learn.microsoft.com/en-us/fabric/get-started/microsoft-fabric-overview)
  - Overview of Microsoft Fabric's SaaS analytics platform, covering Lakehouse, Data Factory, Data Engineering, and Data Science workloads.

- **Microsoft. (2024).** "Microsoft Fabric Decision Guide." [learn.microsoft.com/en-us/fabric/get-started/fabric-decision-guide](https://learn.microsoft.com/en-us/fabric/get-started/fabric-decision-guide)
  - Decision guidance for choosing the right Fabric workload and architecture pattern for your use case.

- **Armbrust, M., et al. (2020).** "Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores." *VLDB*, 13(12), 3411–3424.
  - The Delta Lake paper. Covers the design, implementation, and performance characteristics of the storage layer recommended for all pipeline phases.

### Data Quality

- **Great Expectations Documentation.** [docs.greatexpectations.io](https://docs.greatexpectations.io/)
  - Definitive guide to the Great Expectations data quality framework used in Phase 3.

- **Schelter, S., et al. (2018).** "Automating Large-Scale Data Quality Verification." *VLDB*, 11(12), 1781–1794.
  - Presents Deequ, an alternative data quality framework for Spark. Good comparison point for understanding Great Expectations' design trade-offs.

### Entity Resolution Benchmarks

- **Köpcke, H., Thor, A., & Rahm, E. (2010).** "Evaluation of Entity Resolution Approaches on Real-World Match Problems." *VLDB*, 3(1-2), 484–493.
  - Empirical comparison of entity resolution approaches on real-world datasets. Valuable for understanding which techniques work in which contexts.

---

## Recommended Reading Order

### For Practitioners (Builders)

1. Christen (2012) — *Data Matching* — Chapters 1–5 (data preparation, blocking, string comparison)
2. Hernández & Stolfo (1995) — The Merge/Purge Problem
3. Fellegi & Sunter (1969) — Theory for Record Linkage
4. Mudgal et al. (2018) — Deep Learning for Entity Matching

### For Architects (Designers)

1. Berson & Dubov (2011) — *Master Data Management and Data Governance*
2. Databricks Medallion Architecture
3. Microsoft Fabric Lakehouse Architecture
4. Delta Lake Paper
5. Christen (2012) — *Data Matching* — Chapters 8–10 (clustering, evaluation, privacy)

### For Researchers

1. Fellegi & Sunter (1969) — Theory for Record Linkage
2. Christen (2012) — *Data Matching* book (complete)
3. Li et al. (2020) — Deep Entity Matching with Pre-Trained Language Models
4. Narayan et al. (2023) — Can Foundation Models Wrangle Your Data?
