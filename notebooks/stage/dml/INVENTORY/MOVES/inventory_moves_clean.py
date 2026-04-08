# Databricks notebook source
# MAGIC %run ../../../../_common/00_setup

# COMMAND ----------

config = load_config_from_widgets(dbutils)
ensure_base_objects(spark, config)
source_table = config.uc_table("raw", "inventory_moves")
target_table = config.uc_table("stage", "inventory_moves_clean")

with tracked_task(
    spark,
    dbutils,
    config,
    task_name="stage_inventory_moves_clean",
    materialized_object=target_table,
) as run_id:
    cleaned_df = spark.table(source_table).select(
        "movement_id",
        F.to_date("movement_date").alias("movement_date"),
        "warehouse_id",
        "sku_id",
        F.col("quantity_delta").cast("int").alias("quantity_delta"),
        F.upper("movement_type").alias("movement_type"),
        "source_mode",
        "ingested_at",
    )
    cleaned_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        target_table
    )
    append_lineage_edges(spark, config, run_id, [source_table], target_table)
    safe_display(spark.table(target_table))
