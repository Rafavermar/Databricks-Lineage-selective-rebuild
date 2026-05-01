# Databricks notebook source
# MAGIC %run ../../../../_common/00_setup

# COMMAND ----------

config = load_config_from_widgets(dbutils)
ensure_base_objects(spark, config)
target_table = config.uc_table("core", "dim_date")

with tracked_task(
    spark,
    dbutils,
    config,
    task_name="core_dim_date",
    materialized_object=target_table,
) as run_id:
    dim_date_df = spark.sql(
        f"""
        SELECT
          d AS calendar_date,
          year(d) AS calendar_year,
          month(d) AS calendar_month,
          day(d) AS calendar_day,
          date_trunc('month', d) AS month_start_date,
          CAST(date_format(d, 'yyyyMMdd') AS INT) AS day_key
        FROM (
          SELECT explode(sequence(
            to_date('{DEFAULT_DATE_START}'),
            to_date('{DEFAULT_DATE_END}'),
            interval 1 day
          )) AS d
        )
        """
    )
    dim_date_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        target_table
    )
    safe_display(spark.table(target_table))
