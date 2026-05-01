# Databricks notebook source
# MAGIC %run ../_common/00_setup

# COMMAND ----------

config = load_config_from_widgets(dbutils)
ensure_base_objects(spark, config)

with tracked_task(
    spark,
    dbutils,
    config,
    task_name="Preload_Run_Context",
    materialized_object=config.normalized_target,
):
    preview_df = bfs_upstream(
        spark,
        config,
        target_full_name=config.normalized_target,
        max_hops=config.max_hops,
    )
    safe_display(preview_df.orderBy("hop", "from_full"))
