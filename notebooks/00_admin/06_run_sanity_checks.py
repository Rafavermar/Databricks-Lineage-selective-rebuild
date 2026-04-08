# Databricks notebook source
# MAGIC %run ../_common/00_setup

# COMMAND ----------

config = load_config_from_widgets(dbutils)
ensure_base_objects(spark, config)

dbutils.widgets.text("RUN_QUALITY_ONLY", "false")
run_quality_only = dbutils.widgets.get("RUN_QUALITY_ONLY").lower() == "true"

evaluate_sales_gold_quality(spark, config)

if run_quality_only:
    safe_display(
        spark.table(config.audit_table("quality_results"))
        .where(F.col("target_full") == config.uc_table("gold", "v_sales_summary"))
        .orderBy(F.col("event_time").desc())
    )
else:
    bfs_df = spark.table(config.audit_table("lineage_bfs_results")).where(
        F.col("target_full") == config.normalized_target
    )
    candidates_df = spark.table(config.audit_table("lineage_job_candidates")).where(
        F.col("target_full") == config.normalized_target
    )
    run_log_df = spark.table(config.audit_table("run_log")).where(
        F.col("target_table") == config.normalized_target
    )
    inventory_runs_df = run_log_df.where(F.lower("task_name").contains("inventory"))
    lineage_df = spark.table(config.audit_table("lineage_edges"))
    cycle_hint_df = lineage_df.where(F.col("source_full") == F.col("target_full"))

    print("BFS edges per hop")
    safe_display(bfs_df.groupBy("hop").count().orderBy("hop"))

    print("Candidates per rank")
    safe_display(candidates_df.groupBy("from_rank").count().orderBy("from_rank"))

    print("Numeric suffix artifact filter check")
    suffix_check_df = spark.createDataFrame(
        [
            ("workspace.core.fact_sales_202401", should_exclude_numeric_suffix("workspace.core.fact_sales_202401")),
            ("workspace.core.fact_sales", should_exclude_numeric_suffix("workspace.core.fact_sales")),
        ],
        "full_name string, excluded boolean",
    )
    safe_display(suffix_check_df)

    print("Latest sales quality results")
    safe_display(
        spark.table(config.audit_table("quality_results"))
        .where(F.col("target_full") == config.uc_table("gold", "v_sales_summary"))
        .orderBy(F.col("event_time").desc())
    )

    print("Executed tasks for the target")
    safe_display(run_log_df.orderBy(F.col("event_time").desc()))

    print("Inventory tasks executed for sales target")
    safe_display(inventory_runs_df)

    print("Cycle hints")
    safe_display(cycle_hint_df)
