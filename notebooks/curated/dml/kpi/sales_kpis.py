# Databricks notebook source
# MAGIC %run ../../../_common/00_setup

# COMMAND ----------

config = load_config_from_widgets(dbutils)
ensure_base_objects(spark, config)
source_table = config.uc_table("core", "fact_sales")
target_table = config.uc_table("curated", "sales_kpis")

with tracked_task(
    spark,
    dbutils,
    config,
    task_name="curated_sales_kpis",
    materialized_object=target_table,
) as run_id:
    curated_df = (
        spark.table(source_table)
        .groupBy("order_date", "day_key")
        .agg(
            F.count("*").alias("orders_total"),
            F.sum("amount").alias("gross_sales_amount"),
            F.sum(F.when(~F.col("valid_record_flag"), 1).otherwise(0)).alias(
                "invalid_order_count"
            ),
            F.sum(F.when(~F.col("is_amount_valid"), 1).otherwise(0)).alias(
                "negative_amount_count"
            ),
            F.sum(F.when(~F.col("is_customer_valid"), 1).otherwise(0)).alias(
                "null_customer_count"
            ),
            F.sum(F.when(~F.col("is_currency_valid"), 1).otherwise(0)).alias(
                "invalid_currency_count"
            ),
            F.sum(F.when(~F.col("is_order_date_valid"), 1).otherwise(0)).alias(
                "invalid_date_count"
            ),
            F.sum(F.when(F.col("valid_record_flag"), F.col("amount")).otherwise(0.0)).alias(
                "valid_sales_amount"
            ),
        )
    )
    curated_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        target_table
    )
    append_lineage_edges(spark, config, run_id, [source_table], target_table)
    safe_display(spark.table(target_table))
