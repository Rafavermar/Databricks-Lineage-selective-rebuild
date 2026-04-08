# Databricks Free Edition Selective Rebuild POC

This repository contains a reproducible Databricks Free Edition POC that shows
how to rebuild only the minimum upstream lineage branch required to regenerate a
single Gold consumer view.

Guides:

- `docs/guia_poc_databricks_free_es.md`
- `docs/guide_poc_databricks_free_en.md`
- `docs/technical_flow_reference_en.md`

Main implementation areas:

- `notebooks/` Databricks source notebooks
- `src/lineage_poc/` reusable Python helpers for lineage, quality, and YAML generation
- `resources/jobs/` Declarative Automation Bundle job definitions
- `databricks.yml` bundle entry point
