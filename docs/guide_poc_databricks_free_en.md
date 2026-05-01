# Databricks Free POC: selective rebuild with lineage BFS

## 1. POC goal

This POC proves end to end that, when `gold.v_sales_summary` has a data quality
defect, we can fix the upstream data or transformation and rebuild only the
minimum lineage branch required to regenerate that Gold view, without
recomputing `gold.v_inventory_summary`.

What this POC demonstrates:

1. Only the branch required for the selected Gold target is executed.
2. The inventory Gold view is not recomputed even though both Gold views share
   `core.dim_date`.
3. Lineage can be resolved through:
   - the system lineage table if it exists
   - `workspace.audit.lineage_edges` as a fallback
4. Gold quality remains visible through tables and audit outputs even when DLT
   expectations are not available.

## 2. Recommended structure

```text
.
|-- databricks.yml
|-- resources/jobs/
|-- src/lineage_poc/
|-- notebooks/
|   |-- _common/
|   |-- 00_admin/
|   |-- ops/
|   |-- raw/dml/
|   |-- stage/dml/
|   |-- core/dml/
|   |-- curated/dml/
|   `-- gold/dml/
`-- docs/
```

## 3. Prerequisites

1. Access to a Databricks Free Edition workspace.
2. A recent Databricks CLI version with bundle support.
3. A PAT or valid Databricks CLI authentication.
4. Work from the `develop` branch.

## 4. CLI setup

```bash
databricks configure --profile DEFAULT
```

If you want to reproduce the scaffolding from an empty folder:

```bash
databricks bundle init default-python
```

This repository is already initialized, so `init` is only needed if you want to
recreate the project from scratch in another directory.

## 5. Practical POC assumptions

1. Default catalog: `workspace`
2. Schemas created by bootstrap:
   - `workspace.raw`
   - `workspace.stage`
   - `workspace.core`
   - `workspace.curated`
   - `workspace.gold`
   - `workspace.audit`
3. Preferred volume:
   - `workspace.audit.poc_volume`
4. Preferred landing paths:
   - `/Volumes/workspace/audit/poc_volume/landing/raw/sales_orders/`
   - `/Volumes/workspace/audit/poc_volume/landing/raw/inventory_moves/`
5. Fallback when the volume is not available:
   - `dbfs:/tmp/databricks_lineage_poc/...`
6. Additional ingestion fallback:
   - `workspace.audit.source_seed_records`

## 6. Exact manual execution order

1. `notebooks/00_admin/00_bootstrap_poc.py`
2. `notebooks/00_admin/01_generate_source_data.py`
3. `notebooks/raw/dml/SALES/ORDERS/sales_orders.py`
4. `notebooks/raw/dml/INVENTORY/MOVES/inventory_moves.py`
5. `notebooks/stage/dml/SALES/ORDERS/sales_orders_clean.py`
6. `notebooks/stage/dml/INVENTORY/MOVES/inventory_moves_clean.py`
7. `notebooks/core/dml/SHARED/CALENDAR/dim_date.py`
8. `notebooks/core/dml/SALES/FACT/fact_sales.py`
9. `notebooks/core/dml/INVENTORY/FACT/fact_inventory.py`
10. `notebooks/curated/dml/kpi/sales_kpis.py`
11. `notebooks/curated/dml/kpi/inventory_kpis.py`
12. `notebooks/gold/dml/consumer/v_sales_summary.py`
13. `notebooks/gold/dml/consumer/v_inventory_summary.py`
14. `notebooks/00_admin/06_run_sanity_checks.py`

## 7. Bundle and Workflows

Validate and deploy:

```bash
databricks bundle validate -t dev --profile DEFAULT
databricks bundle deploy -t dev --profile DEFAULT
```

Run through the CLI:

```bash
databricks bundle run generate_lineage_yaml -t dev --profile DEFAULT
databricks bundle run load_lineage_sales_summary -t dev --profile DEFAULT
```

You can also open the Workflows UI and run manually:

1. `Generate_Lineage_YAML`
2. `Load_Lineage_Sales_Summary`
3. `POC_End_To_End_Demo`

Recommended usage:

1. Use the manual notebook flow when you want clean screenshots and step-by-step
   evidence.
2. Use `POC_End_To_End_Demo` when you want one reproducible workflow run that
   covers baseline pass, defect injection, failing quality, fix, lineage YAML
   generation, and selective sales rebuild.

## 8. First cycle with the defect

1. Run `01_generate_source_data.py` with `INJECT_DEFECTS=true`.
2. Run the full base pipeline.
3. Query `workspace.audit.quality_results`.
4. You should see failures in the sales branch and no anomalies in inventory.

## 9. Upstream fix and selective rebuild

1. Run `05_fix_sales_source.py`.
2. Run `90_generate_lineage_yaml.py` with:
   - `TARGET_TABLE_NAME = gold.v_sales_summary`
   - `MAX_HOPS = 6`
   - `NOTEBOOK_BASE = notebooks`
   - `TEST_RANKS =`
   - `TEST_LIMIT_PER_RANK = 0`
3. Run the `Load_Lineage_Sales_Summary` job.
4. Review:
   - `workspace.audit.run_log`
   - `workspace.audit.quality_results`
   - `workspace.audit.lineage_bfs_results`
   - `workspace.audit.lineage_job_candidates`

For fast POC runs, keep `LINEAGE_SOURCE = audit`. Use `LINEAGE_SOURCE = system`
only when you explicitly want to validate Databricks system lineage tables and
can tolerate extra latency.

Expected result:

1. `gold.v_sales_summary` is corrected.
2. No inventory tasks appear in `audit.run_log` for that run.
3. `gold.v_inventory_summary` is not recomputed.

## 10. Orchestrator smoke test

Run `90_generate_lineage_yaml.py` with:

1. `TEST_RANKS = 3,4`
2. `TEST_LIMIT_PER_RANK = 1`

In smoke mode the generator limits tasks by rank and does not append the final
target task or the quality validation task, which keeps the execution coherent.

## 11. End-to-end demo job sequence

The `POC_End_To_End_Demo` workflow runs these phases in order:

1. bootstrap the POC objects
2. generate a clean baseline source
3. execute the full raw to gold pipeline for sales and inventory
4. validate the clean sales baseline
5. inject the sales source defect
6. rebuild only the sales branch to reproduce the failure
7. validate the failing sales quality state
8. fix the sales source
9. generate the selective rebuild YAML
10. execute the selective sales rebuild branch only
11. validate the repaired sales quality state

It is safe to add because it does not replace the existing jobs or change the
underlying notebooks. It only orchestrates them in a longer sequence.

## 12. Validation queries

### Edge count by hop

```sql
SELECT hop, COUNT(*) AS edges
FROM workspace.audit.lineage_bfs_results
GROUP BY hop
ORDER BY hop;
```

### Candidates by rank

```sql
SELECT from_rank, COUNT(*) AS candidates
FROM workspace.audit.lineage_job_candidates
GROUP BY from_rank
ORDER BY from_rank;
```

### Tasks actually executed

```sql
SELECT event_time, task_name, materialized_object, status
FROM workspace.audit.run_log
WHERE target_table = 'workspace.gold.v_sales_summary'
ORDER BY event_time;
```

### Prove inventory did not run

```sql
SELECT *
FROM workspace.audit.run_log
WHERE target_table = 'workspace.gold.v_sales_summary'
  AND task_name LIKE '%inventory%';
```

This query must return zero rows for the selective sales rebuild.

## 13. Implemented fallbacks

1. Lineage:
   - Default for the POC: `workspace.audit.lineage_edges`
   - Optional: `system.access.table_lineage` with `LINEAGE_SOURCE = system`
2. Ingestion:
   - Preferred: Auto Loader
   - Fallback: batch JSON
   - Last fallback: inline seed records from `workspace.audit.source_seed_records`
3. Gold quality:
   - Optional: DLT or Lakeflow expectations notebook
   - Default first-time-works path: notebook framework backed by
     `workspace.audit.quality_results`

## 14. Verified official references

1. Free Edition limitations:
   https://learn.microsoft.com/azure/databricks/getting-started/free-edition-limitations
2. System lineage table:
   https://docs.databricks.com/aws/en/admin/system-tables/lineage
3. Workspace files:
   https://docs.databricks.com/aws/en/files/workspace
4. Bundle examples:
   https://docs.databricks.com/aws/en/dev-tools/bundles/examples
5. Bundle job tutorial:
   https://docs.databricks.com/aws/en/dev-tools/bundles/jobs-tutorial
6. Lakeflow expectations:
   https://docs.databricks.com/aws/en/ldp/expectations
