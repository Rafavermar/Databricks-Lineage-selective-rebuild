# Databricks notebook source
# MAGIC %run ../../../../_common/00_setup

# COMMAND ----------

config = load_config_from_widgets(dbutils)
ensure_base_objects(spark, config)
source_table = config.uc_table("stage", "inventory_moves_clean")
target_table = config.uc_table("core", "fact_inventory")

with tracked_task(
    spark,
    dbutils,
    config,
    task_name="core_fact_inventory",
    materialized_object=target_table,
) as run_id:
    fact_df = (
        spark.table(source_table)
        .select(
            "movement_id",
            "movement_date",
            "warehouse_id",
            "sku_id",
            "quantity_delta",
            "movement_type",
        )
        .withColumn("day_key", F.expr("CAST(date_format(movement_date, 'yyyyMMdd') AS INT)"))
    )
    fact_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        target_table
    )
    append_lineage_edges(spark, config, run_id, [source_table], target_table)
    safe_display(spark.table(target_table))
