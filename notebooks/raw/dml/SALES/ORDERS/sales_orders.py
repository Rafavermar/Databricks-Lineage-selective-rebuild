# Databricks notebook source
# MAGIC %run ../../../../_common/00_setup

# COMMAND ----------

import uuid

config = load_config_from_widgets(dbutils)
paths = ensure_base_objects(spark, config)
raw_table = config.uc_table("raw", "sales_orders")
source_path = f"{paths['landing_root']}/raw/sales_orders"

sales_schema = T.StructType(
    [
        T.StructField("order_id", T.StringType(), True),
        T.StructField("order_date", T.StringType(), True),
        T.StructField("customer_id", T.StringType(), True),
        T.StructField("sku_id", T.StringType(), True),
        T.StructField("currency", T.StringType(), True),
        T.StructField("amount", T.DoubleType(), True),
    ]
)


def _load_batch_sales():
    if fs_path_exists(dbutils, source_path):
        return spark.read.schema(sales_schema).json(source_path).withColumn(
            "source_mode", F.lit("batch_json")
        )
    return read_seed_records(spark, config, "sales_orders", sales_schema).withColumn(
        "source_mode", F.lit("seed_inline")
    )


def _load_autoloader_sales():
    temp_table = config.audit_table("autoload_sales_orders")
    checkpoint_path = f"{paths['checkpoints_root']}/raw_sales_orders/{uuid.uuid4().hex}"
    spark.sql(f"DROP TABLE IF EXISTS {temp_table}")
    stream_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(sales_schema)
        .load(source_path)
    )
    query = (
        stream_df.writeStream.option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(temp_table)
    )
    query.awaitTermination()
    return spark.table(temp_table).withColumn("source_mode", F.lit("auto_loader"))


with tracked_task(
    spark,
    dbutils,
    config,
    task_name="raw_sales_orders",
    materialized_object=raw_table,
) as run_id:
    try:
        if config.ingest_mode in {"auto", "autoloader"} and fs_path_exists(dbutils, source_path):
            raw_df = _load_autoloader_sales()
        else:
            raw_df = _load_batch_sales()
    except Exception:
        raw_df = _load_batch_sales()

    raw_df = raw_df.withColumn("ingested_at", F.current_timestamp())
    raw_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(raw_table)
    safe_display(spark.table(raw_table))
