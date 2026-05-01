# Technical Flow Reference

## Purpose

The project demonstrates selective rebuild for a single Gold target in
Databricks Free Edition. The core idea is simple:

1. Materialize lineage edges while the notebooks run.
2. Run a BFS upstream from the selected Gold object.
3. Infer which notebooks correspond to the discovered upstream tables.
4. Generate a minimal job definition.
5. Execute only the notebooks needed to rebuild that Gold target.

## Project map

### Configuration and deployment

- `databricks.yml`
  Bundle entry point, variables, and target definition.
- `resources/jobs/generate_lineage.yml`
  Static workflow that runs the lineage YAML generator notebook.
- `resources/jobs/load_lineage_sales.yml`
  Static demo workflow for the sales selective rebuild path.

### Reusable runtime and orchestration logic

- `src/lineage_poc/config.py`
  Central defaults, layer ranks, catalog helpers, and config object.
- `src/lineage_poc/runtime.py`
  Widget loading, schema and volume bootstrap, audit table creation, seed
  helpers, and run logging.
- `src/lineage_poc/lineage.py`
  Lineage edge writer, system lineage fallback resolver, BFS engine, candidate
  selection, and audit persistence.
- `src/lineage_poc/path_inference.py`
  Converts `catalog.schema.table` into the relative notebook path used by the
  orchestrator.
- `src/lineage_poc/yaml_generator.py`
  Builds the dynamic job payload and renders YAML.
- `src/lineage_poc/quality.py`
  Writes Gold quality results into `workspace.audit.quality_results`.

### Notebook groups

- `notebooks/_common/00_setup.py`
  Registers the repo `src/` path inside the notebook runtime and imports the
  shared helpers.
- `notebooks/00_admin/`
  Bootstrap, source generation, fix simulation, YAML generation, optional DLT
  sample, and sanity checks.
- `notebooks/ops/`
  Start, barrier, preload, and completion control notebooks used by workflows.
- `notebooks/raw/`, `notebooks/stage/`, `notebooks/core/`,
  `notebooks/curated/`, `notebooks/gold/`
  Layer-specific transformation notebooks.

## Execution context note

This project uses a `src/` layout for reusable Python helpers. That matters in
Databricks:

1. In a Git folder, Databricks typically auto-appends the repository root to
   `sys.path`, not `src/`.
2. In this repo, importing `lineage_poc` requires `.../src` to be on
   `sys.path`.
3. The shared setup notebook therefore derives the project root from the
   current notebook path and appends the sibling `src/` directory explicitly.

This is why the setup code is path-aware instead of relying on default import
behavior.

## Data model and dependencies

### Sources

- `raw.sales_orders`
- `raw.inventory_moves`

### Stage

- `stage.sales_orders_clean` depends on `raw.sales_orders`
- `stage.inventory_moves_clean` depends on `raw.inventory_moves`

### Core

- `core.dim_date` is shared
- `core.fact_sales` depends on `stage.sales_orders_clean`
- `core.fact_inventory` depends on `stage.inventory_moves_clean`

### Curated

- `curated.sales_kpis` depends on `core.fact_sales`
- `curated.inventory_kpis` depends on `core.fact_inventory`

### Gold

- `gold.v_sales_summary` depends on `curated.sales_kpis` and `core.dim_date`
- `gold.v_inventory_summary` depends on `curated.inventory_kpis` and
  `core.dim_date`

This is the key proof point: both Gold views overlap only on `core.dim_date`.
Everything else is intentionally separated so the selective rebuild can show
that inventory-only notebooks do not run when the sales Gold target is rebuilt.

## Lineage modes

### Default mode

The default and recommended POC mode is `LINEAGE_SOURCE = audit`. The BFS reads
from `workspace.audit.lineage_edges`, which is small, deterministic, and fast for
manual demos.

### System mode

Use `LINEAGE_SOURCE = system` only when you explicitly want to validate
`system.access.table_lineage`. System lineage can be slower because it may scan a
larger system table.

### Audit edge writes

Every transformation notebook appends its own edge set to
`workspace.audit.lineage_edges`.

Example:

- `stage.sales_orders_clean` writes the edge
  `workspace.raw.sales_orders -> workspace.stage.sales_orders_clean`
- `gold.v_sales_summary` writes two edges:
  - `workspace.curated.sales_kpis -> workspace.gold.v_sales_summary`
  - `workspace.core.dim_date -> workspace.gold.v_sales_summary`

## BFS selective rebuild logic

The BFS starts from the chosen target, usually
`workspace.gold.v_sales_summary`, and walks upstream.

Inputs:

- `TARGET_TABLE_NAME`
- `MAX_HOPS`

Mechanics:

1. `frontier` starts with the target node.
2. Join `frontier` with lineage edges on `frontier.node = target_full`.
3. Emit the discovered upstream edges for the current hop.
4. Build the next frontier from `from_full`.
5. Remove previously visited nodes with `left_anti`.
6. Stop early if `next_frontier.limit(1).count() == 0`.

Outputs:

- `workspace.audit.lineage_bfs_results`
- `workspace.audit.lineage_job_candidates`

Candidate ordering:

1. `from_rank ASC`
2. `max_hop DESC`
3. `rel_notebook_path ASC`

## Notebook path inference

`path_inference.py` converts a table name into the notebook path expected by the
orchestrator.

Examples:

- `workspace.raw.sales_orders`
  -> `notebooks/raw/dml/SALES/ORDERS/sales_orders.py`
- `workspace.core.fact_sales`
  -> `notebooks/core/dml/SALES/FACT/fact_sales.py`
- `workspace.gold.v_sales_summary`
  -> `notebooks/gold/dml/consumer/v_sales_summary.py`

Safe filtering:

- Objects ending in an optional underscore plus at least six digits are ignored.
- This prevents old migrated artifacts such as `fact_sales_202401` from entering
  the generated job plan.

## Quality validation

The default quality path is notebook-based because it is the most predictable
option in Free Edition.

What it does:

1. Reads `gold.v_sales_summary`
2. Reads `stage.sales_orders_clean`
3. Computes invalid counts for:
   - negative amounts
   - null customers
   - invalid currencies
   - invalid dates
4. Writes one record per check to `workspace.audit.quality_results`

There is also an optional notebook example for DLT or Lakeflow expectations in
`notebooks/00_admin/91_dlt_sales_expectations_optional.py`, but it is not part
of the default first-time-works path.

## Orchestration model

There are two orchestration layers:

### Static workflow layer

Deployed by the Asset Bundle:

- `Generate_Lineage_YAML`
- `Load_Lineage_Sales_Summary`
- `POC_End_To_End_Demo`

### Dynamic workflow description layer

Generated by `notebooks/00_admin/90_generate_lineage_yaml.py`

This notebook:

1. Runs the BFS
2. Persists BFS outputs
3. Applies smoke-test filters when requested
4. Builds a minimal job payload
5. Renders YAML
6. Stores the YAML in:
   - `workspace.audit.generated_job_yaml`
   - the generated workspace storage path when available

## How the static sales workflow is arranged

The deployed demo workflow follows the same sequence the dynamic generator
produces:

1. `Start_Load`
2. `Preload_Run_Context`
3. rank 2 tasks
4. `Gate_R2`
5. rank 3 tasks
6. `Gate_R3`
7. rank 4 tasks
8. `Gate_R4`
9. rank 5 tasks
10. `Gate_R5`
11. target Gold task
12. quality validation
13. `Complete_Load`

The rank gates are simple synchronization barriers. They make the dependency
ordering obvious and ensure upstream layers complete before downstream layers
start.

## How the end-to-end demo workflow is arranged

The bundled `POC_End_To_End_Demo` job is a convenience workflow for repeated
demonstrations and regression checks.

Its phases are:

1. bootstrap
2. clean baseline source generation
3. full base pipeline for sales and inventory
4. baseline quality check
5. defect injection on sales
6. sales-only rebuild to reproduce the defect in Gold
7. failing quality check
8. sales source repair
9. lineage YAML generation
10. selective sales rebuild path
11. final quality validation

This job is intentionally additive. It does not change the manual notebook path
or the existing selective rebuild job.

## Runtime view: what happens when you run it

### Base full load

1. Bootstrap schemas, audit tables, and storage root
2. Generate source files, optionally with defects
3. Ingest raw sales and inventory
4. Clean stage tables
5. Build core tables
6. Build curated tables
7. Create both Gold views
8. Run sanity checks and quality checks

### Selective rebuild after the fix

1. Overwrite the bad sales source with the clean file
2. Run the lineage generator for `gold.v_sales_summary`
3. Inspect generated candidates
4. Execute the selective workflow
5. Review audit logs and quality results
6. Confirm no inventory tasks were executed

### Full automatic demonstration flow

1. Run `POC_End_To_End_Demo`
2. Confirm the baseline quality task passes
3. Confirm the failing quality task records the injected defect
4. Confirm the final selective rebuild quality task passes
5. Confirm the selective section does not execute inventory tasks

## Main audit tables to inspect

- `workspace.audit.lineage_edges`
  Fallback lineage source of truth.
- `workspace.audit.lineage_bfs_results`
  BFS-discovered upstream edges by hop.
- `workspace.audit.lineage_job_candidates`
  Distinct upstream tables selected as execution candidates.
- `workspace.audit.generated_job_yaml`
  Generated YAML text stored for inspection.
- `workspace.audit.run_log`
  Execution trace by task.
- `workspace.audit.quality_results`
  Gold quality outcomes before and after the fix.

## Suggested evidence capture

For a later article or internal documentation, the highest-value screenshots are:

1. Workflows page showing the three deployed jobs
2. Bootstrap notebook output with storage mode and landing root
3. Source generation output with `INJECT_DEFECTS=true`
4. `gold.v_sales_summary` before the fix
5. `workspace.audit.quality_results` showing failure
6. `workspace.audit.lineage_bfs_results` by hop
7. `workspace.audit.lineage_job_candidates` by rank
8. Generated YAML preview from `90_generate_lineage_yaml.py`
9. `Load_Lineage_Sales_Summary` task graph and successful run
10. `workspace.audit.run_log` filtered to prove inventory tasks did not run
11. `workspace.audit.quality_results` showing the final pass

## Execution checklist

To see the whole project in action, run in this exact order:

1. `00_bootstrap_poc.py`
2. `01_generate_source_data.py` with `INJECT_DEFECTS=true`
3. all layer notebooks from raw to gold
4. `06_run_sanity_checks.py`
5. `05_fix_sales_source.py`
6. `90_generate_lineage_yaml.py`
7. `Load_Lineage_Sales_Summary`
8. `06_run_sanity_checks.py` again

The success condition is very concrete:

1. sales quality changes from fail to pass
2. inventory tasks are absent from the sales rebuild run log
3. the BFS candidate list contains only the sales branch plus the shared
   `core.dim_date`
