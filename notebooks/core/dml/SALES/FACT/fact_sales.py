# Databricks notebook source
# MAGIC %run ../../../../_common/00_setup

# COMMAND ----------

config = load_config_from_widgets(dbutils)
ensure_base_objects(spark, config)
source_table = config.uc_table("stage", "sales_orders_clean")
target_table = config.uc_table("core", "fact_sales")

with tracked_task(
    spark,
    dbutils,
    config,
    task_name="core_fact_sales",
    materialized_object=target_table,
) as run_id:
    fact_df = (
        spark.table(source_table)
        .select(
            "order_id",
            "order_date",
            "customer_id",
            "sku_id",
            "currency",
            "amount",
            "is_amount_valid",
            "is_customer_valid",
            "is_currency_valid",
            "is_order_date_valid",
            "valid_record_flag",
        )
        .withColumn("day_key", F.expr("CAST(date_format(order_date, 'yyyyMMdd') AS INT)"))
    )
    fact_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        target_table
    )
    append_lineage_edges(spark, config, run_id, [source_table], target_table)
    safe_display(spark.table(target_table))
