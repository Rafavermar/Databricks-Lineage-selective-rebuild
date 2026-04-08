# Databricks notebook source
# MAGIC %run ../../../_common/00_setup

# COMMAND ----------

config = load_config_from_widgets(dbutils)
ensure_base_objects(spark, config)
source_table = config.uc_table("core", "fact_inventory")
target_table = config.uc_table("curated", "inventory_kpis")

with tracked_task(
    spark,
    dbutils,
    config,
    task_name="curated_inventory_kpis",
    materialized_object=target_table,
) as run_id:
    curated_df = (
        spark.table(source_table)
        .groupBy("movement_date", "day_key")
        .agg(
            F.count("*").alias("movements_total"),
            F.sum("quantity_delta").alias("net_quantity_delta"),
            F.sum(F.when(F.col("movement_type") == "IN", F.col("quantity_delta")).otherwise(0)).alias(
                "quantity_in"
            ),
            F.sum(
                F.when(F.col("movement_type") == "OUT", F.abs(F.col("quantity_delta"))).otherwise(0)
            ).alias("quantity_out"),
        )
    )
    curated_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        target_table
    )
    append_lineage_edges(spark, config, run_id, [source_table], target_table)
    safe_display(spark.table(target_table))
