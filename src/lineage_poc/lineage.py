from __future__ import annotations

from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from lineage_poc.config import LAYER_DEFINITIONS, PocConfig, normalize_table_name
from lineage_poc.path_inference import infer_relative_notebook_path


def append_lineage_edges(
    spark: SparkSession,
    config: PocConfig,
    run_id: str,
    sources: List[str],
    target: str,
) -> None:
    target_full = normalize_table_name(target, config.catalog_name)
    rows = []
    for source in sources:
        source_full = normalize_table_name(source, config.catalog_name)
        rows.append(
            (
                source_full,
                target_full,
                source_full.split(".")[1],
                target_full.split(".")[1],
                run_id,
            )
        )
    if not rows:
        return
    lineage_df = spark.createDataFrame(
        rows,
        "source_full string, target_full string, source_schema string, "
        "target_schema string, run_id string",
    ).withColumn("created_at", F.current_timestamp())
    lineage_df.write.mode("append").saveAsTable(config.audit_table("lineage_edges"))


def get_lineage_source_df(spark: SparkSession, config: PocConfig) -> DataFrame:
    system_lineage_name = "system.access.table_lineage"
    if spark.catalog.tableExists(system_lineage_name):
        return (
            spark.table(system_lineage_name)
            .select(
                F.col("source_table_full_name").alias("source_full"),
                F.col("target_table_full_name").alias("target_full"),
            )
            .where(
                F.col("source_full").startswith(f"{config.catalog_name}.")
                & F.col("target_full").startswith(f"{config.catalog_name}.")
            )
            .withColumn("source_schema", F.split("source_full", "\\.").getItem(1))
            .withColumn("target_schema", F.split("target_full", "\\.").getItem(1))
            .dropDuplicates(["source_full", "target_full"])
        )

    return (
        spark.table(config.audit_table("lineage_edges"))
        .select("source_full", "target_full", "source_schema", "target_schema")
        .dropDuplicates(["source_full", "target_full"])
    )


def layer_definition_df(spark: SparkSession) -> DataFrame:
    schema = T.StructType(
        [
            T.StructField("schema_name", T.StringType(), False),
            T.StructField("layer_alias", T.StringType(), False),
            T.StructField("layer_rank", T.IntegerType(), False),
        ]
    )
    rows = [
        (
            str(item["schema_name"]),
            str(item["layer_alias"]),
            int(item["layer_rank"]),
        )
        for item in LAYER_DEFINITIONS
    ]
    return spark.createDataFrame(rows, schema=schema)


def bfs_upstream(
    spark: SparkSession,
    config: PocConfig,
    target_full_name: str,
    max_hops: int,
) -> DataFrame:
    edges_df = get_lineage_source_df(spark, config)
    layer_df = layer_definition_df(spark)

    frontier_df = spark.createDataFrame(
        [(normalize_table_name(target_full_name, config.catalog_name),)],
        "node string",
    )
    visited_df = frontier_df.select(F.col("node").alias("visited_node"))
    output_df: Optional[DataFrame] = None

    for hop_number in range(1, max_hops + 1):
        expanded_df = (
            frontier_df.alias("frontier")
            .join(
                edges_df.alias("edges"),
                F.col("frontier.node") == F.col("edges.target_full"),
                "inner",
            )
            .select(
                F.lit(hop_number).alias("hop"),
                F.col("edges.source_schema").alias("from_schema"),
                F.col("edges.source_full").alias("from_full"),
                F.col("edges.target_schema").alias("to_schema"),
                F.col("edges.target_full").alias("to_full"),
            )
            .dropDuplicates(["from_full", "to_full"])
        )

        output_df = expanded_df if output_df is None else output_df.unionByName(expanded_df)
        output_df = output_df.dropDuplicates(["from_full", "to_full", "hop"])

        next_frontier_df = (
            expanded_df.select(F.col("from_full").alias("node"))
            .dropDuplicates(["node"])
            .join(
                visited_df,
                F.col("node") == F.col("visited_node"),
                "left_anti",
            )
        )

        if next_frontier_df.limit(1).count() == 0:
            break

        visited_df = visited_df.unionByName(
            next_frontier_df.select(F.col("node").alias("visited_node"))
        ).dropDuplicates(["visited_node"])
        frontier_df = next_frontier_df

    if output_df is None:
        output_df = spark.createDataFrame(
            [],
            "hop int, from_schema string, from_full string, to_schema string, to_full string",
        )

    return (
        output_df.alias("edges")
        .join(
            layer_df.alias("layers"),
            F.col("edges.from_schema") == F.col("layers.schema_name"),
            "left",
        )
        .select(
            "hop",
            "from_schema",
            "from_full",
            "to_schema",
            "to_full",
            F.col("layers.layer_rank").alias("from_rank"),
            F.col("layers.layer_alias").alias("from_alias"),
        )
    )


def select_run_candidates(
    bfs_edges_df: DataFrame,
    notebook_base: str,
) -> DataFrame:
    infer_udf = F.udf(
        lambda full_name: infer_relative_notebook_path(full_name, notebook_base),
        T.StringType(),
    )
    return (
        bfs_edges_df.groupBy("from_full", "from_rank")
        .agg(F.max("hop").alias("max_hop"))
        .withColumn("rel_notebook_path", infer_udf("from_full"))
        .where(F.col("rel_notebook_path").isNotNull())
        .orderBy(F.col("from_rank").asc(), F.col("max_hop").desc(), F.col("rel_notebook_path"))
    )


def persist_bfs_outputs(
    spark: SparkSession,
    config: PocConfig,
    bfs_edges_df: DataFrame,
    candidates_df: DataFrame,
) -> None:
    target_full = config.normalized_target
    generated_at = F.current_timestamp()

    spark.sql(
        f"DELETE FROM {config.audit_table('lineage_bfs_results')} WHERE target_full = '{target_full}'"
    )
    spark.sql(
        f"DELETE FROM {config.audit_table('lineage_job_candidates')} WHERE target_full = '{target_full}'"
    )

    bfs_edges_df.withColumn("target_full", F.lit(target_full)).withColumn(
        "generated_at", generated_at
    ).select(
        "target_full",
        "hop",
        "from_schema",
        "from_full",
        "to_schema",
        "to_full",
        "from_rank",
        "from_alias",
        "generated_at",
    ).write.mode("append").saveAsTable(config.audit_table("lineage_bfs_results"))

    candidates_df.withColumn("target_full", F.lit(target_full)).withColumn(
        "generated_at", generated_at
    ).select(
        "target_full",
        "from_full",
        "from_rank",
        "max_hop",
        "rel_notebook_path",
        "generated_at",
    ).write.mode("append").saveAsTable(config.audit_table("lineage_job_candidates"))
