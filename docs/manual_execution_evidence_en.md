# Manual Execution And Evidence Checklist

## Goal

Run the POC manually once to capture clean evidence: defect introduced upstream,
Gold quality failure, BFS lineage selection, selective rebuild, final quality
pass, and no inventory recomputation.

## Before starting

Use `notebooks/00_admin/99_reset_poc` only when you want a clean demo state.
Set `CONFIRM_RESET = RESET_POC`.

The reset notebook drops known POC tables/views and recreates empty audit tables.
It keeps the catalog, schemas, and volume.

If you do not reset, rerunning the POC still works, but audit tables such as
`run_log`, `quality_results`, and `lineage_edges` will contain historical rows.

## Manual flow

1. Run `00_bootstrap_poc`.
   Expected: schemas and audit tables exist, storage mode is shown.
   Capture: bootstrap output and Catalog Explorer schemas.

2. Run `01_generate_source_data` with `INJECT_DEFECTS = true`.
   Expected: `sales_bad_v1`, `injected_defects = true`.
   Capture: `source_seed_records` showing bad sales and clean inventory.

3. Run raw notebooks for sales and inventory.
   Expected: raw tables exist, `source_mode` is populated.
   Capture: `raw.sales_orders` showing `amount = -50`, `customer_id = null`,
   or `currency = BTC`.

4. Run stage notebooks for sales and inventory.
   Expected: sales defect flags are `false`; inventory is clean.
   Capture: `stage.sales_orders_clean` with invalid flags.

5. Run core notebooks.
   Expected: `core.dim_date`, `core.fact_sales`, and `core.fact_inventory`
   exist.
   Capture: `core.dim_date` as the shared node and `core.fact_sales` with flags.

6. Run curated notebooks.
   Expected: `curated.sales_kpis` has invalid counters greater than zero.
   Capture: sales KPI invalid counts and inventory KPI output.

7. Run Gold notebooks.
   Expected: `gold.v_sales_summary` exposes quality counters;
   `gold.v_inventory_summary` remains independent.
   Capture: both Gold views.

8. Run `06_run_sanity_checks`.
   Expected: latest sales quality checks fail.
   Capture: `quality_results` with `FAIL`.

9. Run `90_generate_lineage_yaml` with `LINEAGE_SOURCE = audit`.
   Expected: candidates are sales branch plus `core.dim_date`, with no inventory.
   Capture: `lineage_job_candidates`, `lineage_bfs_results`, and generated YAML.

10. Run `05_fix_sales_source`.
    Expected: `sales_fixed_v2`, `injected_defects = false`.
    Capture: fixed source seed records.

11. Run the `Load_Lineage_Sales_Summary` workflow.
    Expected: only sales tasks plus `core_dim_date` execute.
    Capture: successful workflow run graph.

12. Run `06_run_sanity_checks` again.
    Expected: latest sales quality checks pass.
    Capture: `quality_results` showing FAIL then PASS.

## Final proof queries

```sql
WITH last_start AS (
  SELECT max(event_time) AS start_time
  FROM workspace.audit.run_log
  WHERE target_table = 'workspace.gold.v_sales_summary'
    AND task_name = 'Start_Load'
    AND status = 'STARTED'
)
SELECT event_time, task_name, materialized_object, status
FROM workspace.audit.run_log
WHERE target_table = 'workspace.gold.v_sales_summary'
  AND event_time >= (SELECT start_time FROM last_start)
  AND lower(task_name) LIKE '%inventory%'
ORDER BY event_time DESC;
```

Expected: zero rows.

```sql
WITH last_start AS (
  SELECT max(event_time) AS start_time
  FROM workspace.audit.run_log
  WHERE target_table = 'workspace.gold.v_sales_summary'
    AND task_name = 'Start_Load'
    AND status = 'STARTED'
)
SELECT event_time, task_name, materialized_object, status
FROM workspace.audit.run_log
WHERE target_table = 'workspace.gold.v_sales_summary'
  AND event_time >= (SELECT start_time FROM last_start)
ORDER BY event_time;
```

Expected: sales branch tasks, `core_dim_date`, gates, validation, and completion.
