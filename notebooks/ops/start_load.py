# Databricks notebook source
# MAGIC %run ../_common/00_setup

# COMMAND ----------

config = load_config_from_widgets(dbutils)
ensure_base_objects(spark, config)

with tracked_task(
    spark,
    dbutils,
    config,
    task_name="Start_Load",
    materialized_object=config.normalized_target,
):
    start_df = spark.createDataFrame(
        [(config.normalized_target, "load sequence started")],
        "target_full string, status string",
    )
    safe_display(start_df)
