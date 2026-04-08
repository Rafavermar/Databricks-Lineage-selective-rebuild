# Databricks notebook source
# MAGIC %run ../_common/00_setup

# COMMAND ----------

config = load_config_from_widgets(dbutils)
paths = ensure_base_objects(spark, config)

bfs_edges_df = bfs_upstream(
    spark,
    config,
    target_full_name=config.normalized_target,
    max_hops=config.max_hops,
)
candidates_df = select_run_candidates(
    bfs_edges_df=bfs_edges_df,
    notebook_base=config.notebook_base,
)
persist_bfs_outputs(spark, config, bfs_edges_df, candidates_df)

candidates = [row.asDict() for row in candidates_df.collect()]
smoke_candidates = apply_smoke_test_filters(
    candidates,
    test_ranks=config.test_ranks,
    test_limit_per_rank=config.test_limit_per_rank,
)
smoke_test_mode = bool(config.test_ranks or config.test_limit_per_rank > 0)
payload = build_job_payload(
    config,
    candidates=smoke_candidates if smoke_test_mode else candidates,
    smoke_test_mode=smoke_test_mode,
)
yaml_text = render_yaml(payload)

spark.sql(
    f"DELETE FROM {config.audit_table('generated_job_yaml')} WHERE target_full = '{config.normalized_target}'"
)
spark.createDataFrame(
    [(config.normalized_target, yaml_text, smoke_test_mode)],
    "target_full string, yaml_text string, smoke_test_mode boolean",
).withColumn("generated_at", F.current_timestamp()).write.mode("append").saveAsTable(
    config.audit_table("generated_job_yaml")
)

try:
    dbutils.fs.put(
        f"{paths['generated_root']}/{config.normalized_target.split('.')[-1]}_job.yml",
        yaml_text,
        True,
    )
except Exception:
    pass

safe_display(bfs_edges_df)
safe_display(candidates_df)
print(yaml_text)
