# Databricks notebook source
# MAGIC %run ../_common/00_setup

# COMMAND ----------

config = load_config_from_widgets(dbutils)
ensure_base_objects(spark, config)

with tracked_task(
    spark,
    dbutils,
    config,
    task_name="Complete_Load",
    materialized_object=config.normalized_target,
):
    completion_df = spark.createDataFrame(
        [(config.normalized_target, "load sequence completed")],
        "target_full string, status string",
    )
    safe_display(completion_df)
