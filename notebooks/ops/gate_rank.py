# Databricks notebook source
# MAGIC %run ../_common/00_setup

# COMMAND ----------

config = load_config_from_widgets(dbutils)
ensure_base_objects(spark, config)

dbutils.widgets.text("RANK", "0")
rank = dbutils.widgets.get("RANK")

with tracked_task(
    spark,
    dbutils,
    config,
    task_name=f"Gate_R{rank}",
    materialized_object=config.normalized_target,
):
    gate_df = spark.createDataFrame(
        [(rank, config.normalized_target, "rank barrier completed")],
        "rank string, target_full string, status string",
    )
    safe_display(gate_df)
