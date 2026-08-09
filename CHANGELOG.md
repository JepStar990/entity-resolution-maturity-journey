# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-08-09

### Changed

- Adapted reference platform from Databricks to Microsoft Fabric throughout all documentation
- Updated architecture docs to reference Fabric Lakehouse as primary platform
- Updated medallion architecture references to include Fabric's OneLake implementation
- Updated glossary: added OneLake entry, revised Databricks/MLflow entries for Fabric context
- Updated technology stack: added Fabric Runtime, OneLake, Fabric Data Pipelines context
- Updated decision log: added ADR-008 (Microsoft Fabric as Primary Platform)
- Updated phase docs (1, 2, 6, 9, 10, 13, 14): replaced Databricks-specific links with Fabric + Delta Lake equivalents
- Updated getting started guide with Fabric workspace prerequisites and Spark configuration
- Updated Mermaid diagram (phase 10): replaced "MLflow Registry" with platform-neutral "Model Registry"

### Added

- `docs/reference/fabric-setup.md` — step-by-step Fabric workspace, Lakehouse, and notebook setup guide
- `docs/reference/fabric-vs-databricks.md` — platform comparison for entity resolution workloads
- Microsoft Fabric documentation links to bibliography and phase "Further Reading" sections

## [0.1.0] - 2026-05-31

### Added

- Initial repository structure and documentation framework
- 15-phase entity resolution maturity model documentation
- Cross-cutting Mermaid diagrams:
  - Master pipeline (full 15-phase flow with medallion architecture)
  - Data flow architecture (system context)
  - Maturity curve (capability progression across three dimensions)
  - Bronze/Silver/Gold overlay (medallion architecture mapping)
  - Technology-to-phase matrix
- Per-phase Mermaid diagrams for all 15 phases
- Per-phase documentation following 12-section template
- Reference section: glossary, technology stack, medallion architecture, bibliography, decision log
- MkDocs + Material for MkDocs configuration
- Contribution guide
- Code of conduct
- Apache 2.0 license
