# Databricks notebook source
from pathlib import Path
import sys


def _register_project_root():
    notebook_path = (
        dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    )
    absolute_path = Path("/Workspace") / notebook_path.lstrip("/")
    for candidate in [absolute_path.parent, *absolute_path.parents]:
        if candidate.name == "notebooks":
            src_path_str = str(candidate.parent / "src")
            if src_path_str not in sys.path:
                sys.path.insert(0, src_path_str)
            return src_path_str
    raise RuntimeError("Could not derive project src path from notebook path.")


PROJECT_ROOT = _register_project_root()

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from lineage_poc.config import DEFAULT_DATE_END, DEFAULT_DATE_START  # noqa: E402
from lineage_poc.lineage import (  # noqa: E402
    append_lineage_edges,
    bfs_upstream,
    persist_bfs_outputs,
    select_run_candidates,
)
from lineage_poc.path_inference import (  # noqa: E402
    infer_relative_notebook_path,
    should_exclude_numeric_suffix,
)
from lineage_poc.quality import evaluate_sales_gold_quality  # noqa: E402
from lineage_poc.runtime import (  # noqa: E402
    ensure_base_objects,
    fs_path_exists,
    load_config_from_widgets,
    overwrite_json_dataset,
    read_seed_records,
    safe_display,
    tracked_task,
    write_seed_records,
)
from lineage_poc.yaml_generator import (  # noqa: E402
    apply_smoke_test_filters,
    build_job_payload,
    render_yaml,
)
