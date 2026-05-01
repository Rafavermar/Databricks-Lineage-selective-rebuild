# Databricks notebook source
# MAGIC %run ../_common/00_setup

# COMMAND ----------

config = load_config_from_widgets(dbutils)
paths = ensure_base_objects(spark, config)

bootstrap_df = spark.createDataFrame(
    [
        (
            config.catalog_name,
            config.volume_full_name,
            paths["storage_mode"],
            paths["landing_root"],
            paths["generated_root"],
        )
    ],
    "catalog_name string, volume_name string, storage_mode string, "
    "landing_root string, generated_root string",
)

safe_display(bootstrap_df)
