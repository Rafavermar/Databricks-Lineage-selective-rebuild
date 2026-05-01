from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StructType

from lineage_poc.config import AUDIT_SCHEMA, POC_SCHEMAS, PocConfig, build_poc_config


def get_widget(dbutils, name: str, default: str) -> str:
    try:
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name) or default
    except Exception:
        return default


def load_config_from_widgets(dbutils) -> PocConfig:
    return build_poc_config(
        {
            "catalog_name": get_widget(dbutils, "CATALOG_NAME", "workspace"),
            "volume_name": get_widget(dbutils, "VOLUME_NAME", "poc_volume"),
            "target_table_name": get_widget(
                dbutils, "TARGET_TABLE_NAME", "gold.v_sales_summary"
            ),
            "max_hops": get_widget(dbutils, "MAX_HOPS", "6"),
            "notebook_base": get_widget(dbutils, "NOTEBOOK_BASE", "notebooks"),
            "test_ranks": get_widget(dbutils, "TEST_RANKS", ""),
            "test_limit_per_rank": get_widget(dbutils, "TEST_LIMIT_PER_RANK", "0"),
            "ingest_mode": get_widget(dbutils, "INGEST_MODE", "auto"),
            "lineage_source": get_widget(dbutils, "LINEAGE_SOURCE", "audit"),
        }
    )


def ensure_base_objects(spark: SparkSession, config: PocConfig) -> Dict[str, str]:
    for schema_name in POC_SCHEMAS:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {config.catalog_name}.{schema_name}")

    try:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {config.volume_full_name}")
        storage_root = f"/Volumes/{config.catalog_name}/{AUDIT_SCHEMA}/{config.volume_name}"
        storage_mode = "volume"
    except Exception:
        storage_root = "dbfs:/tmp/databricks_lineage_poc"
        storage_mode = "dbfs"

    paths = {
        "storage_mode": storage_mode,
        "storage_root": storage_root,
        "landing_root": f"{storage_root}/landing",
        "generated_root": f"{storage_root}/generated",
        "checkpoints_root": f"{storage_root}/checkpoints",
    }

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {config.audit_table('lineage_edges')} (
          source_full STRING,
          target_full STRING,
          source_schema STRING,
          target_schema STRING,
          created_at TIMESTAMP,
          run_id STRING
        ) USING DELTA
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {config.audit_table('run_log')} (
          event_time TIMESTAMP,
          run_id STRING,
          target_table STRING,
          task_name STRING,
          materialized_object STRING,
          status STRING,
          message STRING
        ) USING DELTA
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {config.audit_table('quality_results')} (
          event_time TIMESTAMP,
          run_id STRING,
          target_full STRING,
          check_name STRING,
          invalid_count BIGINT,
          total_count BIGINT,
          invalid_pct DOUBLE,
          status STRING,
          sample_json STRING
        ) USING DELTA
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {config.audit_table('source_seed_records')} (
          dataset_name STRING,
          payload STRING,
          injected_defects BOOLEAN,
          version_tag STRING,
          created_at TIMESTAMP
        ) USING DELTA
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {config.audit_table('lineage_bfs_results')} (
          target_full STRING,
          hop INT,
          from_schema STRING,
          from_full STRING,
          to_schema STRING,
          to_full STRING,
          from_rank INT,
          from_alias STRING,
          generated_at TIMESTAMP
        ) USING DELTA
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {config.audit_table('lineage_job_candidates')} (
          target_full STRING,
          from_full STRING,
          from_rank INT,
          max_hop INT,
          rel_notebook_path STRING,
          generated_at TIMESTAMP
        ) USING DELTA
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {config.audit_table('generated_job_yaml')} (
          target_full STRING,
          yaml_text STRING,
          smoke_test_mode BOOLEAN,
          generated_at TIMESTAMP
        ) USING DELTA
        """
    )

    return paths


def table_exists(spark: SparkSession, full_name: str) -> bool:
    return spark.catalog.tableExists(full_name)


def fs_path_exists(dbutils, path: str) -> bool:
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False


def write_seed_records(
    spark: SparkSession,
    config: PocConfig,
    dataset_name: str,
    records: List[dict],
    injected_defects: bool,
    version_tag: str,
) -> None:
    spark.sql(
        f"DELETE FROM {config.audit_table('source_seed_records')} WHERE dataset_name = '{dataset_name}'"
    )
    rows = [
        (
            dataset_name,
            json.dumps(record, sort_keys=True),
            injected_defects,
            version_tag,
            datetime.utcnow(),
        )
        for record in records
    ]
    seed_df = spark.createDataFrame(
        rows,
        "dataset_name string, payload string, injected_defects boolean, "
        "version_tag string, created_at timestamp",
    )
    seed_df.write.mode("append").saveAsTable(config.audit_table("source_seed_records"))


def read_seed_records(
    spark: SparkSession,
    config: PocConfig,
    dataset_name: str,
    schema: StructType,
) -> DataFrame:
    payload_df = spark.table(config.audit_table("source_seed_records")).where(
        F.col("dataset_name") == dataset_name
    )
    parsed_df = payload_df.select(F.from_json("payload", schema).alias("json_payload"))
    return parsed_df.select("json_payload.*")


def log_run_event(
    spark: SparkSession,
    config: PocConfig,
    task_name: str,
    materialized_object: str,
    status: str,
    run_id: str,
    message: str = "",
) -> None:
    log_df = spark.createDataFrame(
        [
            (
                datetime.utcnow(),
                run_id,
                config.normalized_target,
                task_name,
                materialized_object,
                status,
                message,
            )
        ],
        "event_time timestamp, run_id string, target_table string, task_name string, "
        "materialized_object string, status string, message string",
    )
    log_df.write.mode("append").saveAsTable(config.audit_table("run_log"))


def build_run_id(dbutils) -> str:
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    try:
        context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        run_id = context.currentRunId().get()
        return f"run_{run_id}"
    except Exception:
        return f"manual_{timestamp}"


@contextmanager
def tracked_task(
    spark: SparkSession,
    dbutils,
    config: PocConfig,
    task_name: str,
    materialized_object: str,
):
    run_id = build_run_id(dbutils)
    log_run_event(
        spark,
        config,
        task_name=task_name,
        materialized_object=materialized_object,
        status="STARTED",
        run_id=run_id,
    )
    try:
        yield run_id
        log_run_event(
            spark,
            config,
            task_name=task_name,
            materialized_object=materialized_object,
            status="COMPLETED",
            run_id=run_id,
        )
    except Exception as exc:
        log_run_event(
            spark,
            config,
            task_name=task_name,
            materialized_object=materialized_object,
            status="FAILED",
            run_id=run_id,
            message=str(exc)[:4000],
        )
        raise


def overwrite_json_dataset(df: DataFrame, path: str) -> None:
    df.coalesce(1).write.mode("overwrite").json(path)


def safe_display(df: DataFrame, limit: int = 20) -> None:
    try:
        display(df.limit(limit))
    except Exception:
        df.show(limit, truncate=False)
