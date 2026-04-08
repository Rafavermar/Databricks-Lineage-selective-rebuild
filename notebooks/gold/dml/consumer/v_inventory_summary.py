# Databricks notebook source
# MAGIC %run ../../../_common/00_setup

# COMMAND ----------

config = load_config_from_widgets(dbutils)
ensure_base_objects(spark, config)
curated_table = config.uc_table("curated", "inventory_kpis")
dim_table = config.uc_table("core", "dim_date")
target_view = config.uc_table("gold", "v_inventory_summary")

with tracked_task(
    spark,
    dbutils,
    config,
    task_name="gold_v_inventory_summary",
    materialized_object=target_view,
) as run_id:
    spark.sql(
        f"""
        CREATE OR REPLACE VIEW {target_view} AS
        SELECT
          d.calendar_date,
          d.calendar_year,
          d.calendar_month,
          d.month_start_date,
          i.movements_total,
          i.net_quantity_delta,
          i.quantity_in,
          i.quantity_out
        FROM {curated_table} AS i
        INNER JOIN {dim_table} AS d
          ON i.day_key = d.day_key
        """
    )
    append_lineage_edges(spark, config, run_id, [curated_table, dim_table], target_view)
    safe_display(spark.table(target_view))
