# Databricks notebook source
# MAGIC %run ../../../../_common/00_setup

# COMMAND ----------

config = load_config_from_widgets(dbutils)
ensure_base_objects(spark, config)
source_table = config.uc_table("raw", "sales_orders")
target_table = config.uc_table("stage", "sales_orders_clean")

with tracked_task(
    spark,
    dbutils,
    config,
    task_name="stage_sales_orders_clean",
    materialized_object=target_table,
) as run_id:
    source_df = spark.table(source_table)
    cleaned_df = (
        source_df.select(
            "order_id",
            F.to_date("order_date").alias("order_date"),
            "customer_id",
            "sku_id",
            F.upper("currency").alias("currency"),
            F.col("amount").cast("double").alias("amount"),
            "source_mode",
            "ingested_at",
        )
        .withColumn("is_amount_valid", F.col("amount") >= F.lit(0.0))
        .withColumn("is_customer_valid", F.col("customer_id").isNotNull())
        .withColumn("is_currency_valid", F.col("currency").isin("EUR", "USD"))
        .withColumn("is_order_date_valid", F.col("order_date").isNotNull())
        .withColumn(
            "valid_record_flag",
            F.col("is_amount_valid")
            & F.col("is_customer_valid")
            & F.col("is_currency_valid")
            & F.col("is_order_date_valid"),
        )
    )
    cleaned_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        target_table
    )
    append_lineage_edges(spark, config, run_id, [source_table], target_table)
    safe_display(spark.table(target_table))
