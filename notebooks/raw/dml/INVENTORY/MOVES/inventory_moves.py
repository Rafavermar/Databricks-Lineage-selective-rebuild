# Databricks notebook source
# MAGIC %run ../../../../_common/00_setup

# COMMAND ----------

import uuid

config = load_config_from_widgets(dbutils)
paths = ensure_base_objects(spark, config)
raw_table = config.uc_table("raw", "inventory_moves")
source_path = f"{paths['landing_root']}/raw/inventory_moves"

inventory_schema = T.StructType(
    [
        T.StructField("movement_id", T.StringType(), True),
        T.StructField("movement_date", T.StringType(), True),
        T.StructField("warehouse_id", T.StringType(), True),
        T.StructField("sku_id", T.StringType(), True),
        T.StructField("quantity_delta", T.IntegerType(), True),
        T.StructField("movement_type", T.StringType(), True),
    ]
)


def _load_batch_inventory():
    if fs_path_exists(dbutils, source_path):
        return spark.read.schema(inventory_schema).json(source_path).withColumn(
            "source_mode", F.lit("batch_json")
        )
    return read_seed_records(spark, config, "inventory_moves", inventory_schema).withColumn(
        "source_mode", F.lit("seed_inline")
    )


def _load_autoloader_inventory():
    temp_table = config.audit_table("autoload_inventory_moves")
    checkpoint_path = f"{paths['checkpoints_root']}/raw_inventory_moves/{uuid.uuid4().hex}"
    spark.sql(f"DROP TABLE IF EXISTS {temp_table}")
    stream_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(inventory_schema)
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
    task_name="raw_inventory_moves",
    materialized_object=raw_table,
) as run_id:
    try:
        if config.ingest_mode in {"auto", "autoloader"} and fs_path_exists(dbutils, source_path):
            raw_df = _load_autoloader_inventory()
        else:
            raw_df = _load_batch_inventory()
    except Exception:
        raw_df = _load_batch_inventory()

    raw_df = raw_df.withColumn("ingested_at", F.current_timestamp())
    raw_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(raw_table)
    safe_display(spark.table(raw_table))
