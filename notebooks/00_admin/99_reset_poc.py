# Databricks notebook source
# MAGIC %run ../_common/00_setup

# COMMAND ----------

config = load_config_from_widgets(dbutils)
dbutils.widgets.text("CONFIRM_RESET", "")
confirm_reset = dbutils.widgets.get("CONFIRM_RESET")

if confirm_reset != "RESET_POC":
    raise ValueError("Set CONFIRM_RESET to RESET_POC to reset the POC objects.")

objects_to_drop = [
    ("VIEW", config.uc_table("gold", "v_sales_summary")),
    ("VIEW", config.uc_table("gold", "v_inventory_summary")),
    ("TABLE", config.uc_table("curated", "sales_kpis")),
    ("TABLE", config.uc_table("curated", "inventory_kpis")),
    ("TABLE", config.uc_table("core", "fact_sales")),
    ("TABLE", config.uc_table("core", "fact_inventory")),
    ("TABLE", config.uc_table("core", "dim_date")),
    ("TABLE", config.uc_table("stage", "sales_orders_clean")),
    ("TABLE", config.uc_table("stage", "inventory_moves_clean")),
    ("TABLE", config.uc_table("raw", "sales_orders")),
    ("TABLE", config.uc_table("raw", "inventory_moves")),
    ("TABLE", config.audit_table("autoload_sales_orders")),
    ("TABLE", config.audit_table("autoload_inventory_moves")),
]

for object_type, full_name in objects_to_drop:
    spark.sql(f"DROP {object_type} IF EXISTS {full_name}")

audit_tables_to_drop = [
    "lineage_edges",
    "run_log",
    "quality_results",
    "source_seed_records",
    "lineage_bfs_results",
    "lineage_job_candidates",
    "generated_job_yaml",
]

for table_name in audit_tables_to_drop:
    spark.sql(f"DROP TABLE IF EXISTS {config.audit_table(table_name)}")

paths = ensure_base_objects(spark, config)
reset_df = spark.createDataFrame(
    [
        (
            config.catalog_name,
            paths["storage_mode"],
            "POC objects reset. Audit tables were recreated empty.",
        )
    ],
    "catalog_name string, storage_mode string, status string",
)

safe_display(reset_df)
