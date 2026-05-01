from __future__ import annotations

from typing import Dict, List

from lineage_poc.config import PocConfig, normalize_table_name
from lineage_poc.path_inference import infer_workspace_notebook_path


def build_job_payload(
    config: PocConfig,
    candidates: List[Dict[str, object]],
    smoke_test_mode: bool = False,
) -> Dict[str, object]:
    target_full = normalize_table_name(config.target_table_name, config.catalog_name)
    target_name = target_full.split(".")[-1]
    job_key = f"load_lineage_{target_name}"
    job_name = f"Load_Lineage_{target_name}"

    tasks: List[Dict[str, object]] = [
        _task(
            "Start_Load",
            f"${{workspace.file_path}}/{config.notebook_base}/ops/start_load.py",
            config,
        ),
        _task(
            "Preload_Run_Context",
            f"${{workspace.file_path}}/{config.notebook_base}/ops/preload_run_context.py",
            config,
            depends_on=["Start_Load"],
            extra_params={"MAX_HOPS": str(config.max_hops)},
        ),
    ]

    grouped_by_rank: Dict[int, List[Dict[str, object]]] = {}
    for candidate in candidates:
        grouped_by_rank.setdefault(int(candidate["from_rank"]), []).append(candidate)

    prior_gate = "Preload_Run_Context"
    for rank in sorted(grouped_by_rank):
        rank_tasks = grouped_by_rank[rank]
        current_task_keys: List[str] = []
        for candidate in rank_tasks:
            path = f"${{workspace.file_path}}/{candidate['rel_notebook_path']}"
            task_key = candidate["from_full"].split(".")[-1]
            tasks.append(
                _task(
                    task_key,
                    path,
                    config,
                    depends_on=[prior_gate],
                )
            )
            current_task_keys.append(task_key)

        gate_key = f"Gate_R{rank}"
        tasks.append(
            _task(
                gate_key,
                f"${{workspace.file_path}}/{config.notebook_base}/ops/gate_rank.py",
                config,
                depends_on=current_task_keys,
                extra_params={"RANK": str(rank)},
            )
        )
        prior_gate = gate_key

    if not smoke_test_mode:
        target_notebook_path = infer_workspace_notebook_path(
            target_full,
            notebook_base=config.notebook_base,
        )
        tasks.append(
            _task(
                f"target_{target_name}",
                target_notebook_path,
                config,
                depends_on=[prior_gate],
            )
        )
        tasks.append(
            _task(
                "Validate_Target_Quality",
                f"${{workspace.file_path}}/{config.notebook_base}/00_admin/06_run_sanity_checks.py",
                config,
                depends_on=[f"target_{target_name}"],
                extra_params={"RUN_QUALITY_ONLY": "true"},
            )
        )
        prior_gate = "Validate_Target_Quality"

    tasks.append(
        _task(
            "Complete_Load",
            f"${{workspace.file_path}}/{config.notebook_base}/ops/complete_load.py",
            config,
            depends_on=[prior_gate],
        )
    )

    return {
        "resources": {
            "jobs": {
                job_key: {
                    "name": job_name,
                    "queue": {"enabled": True},
                    "tasks": tasks,
                }
            }
        }
    }


def render_yaml(payload: Dict[str, object], indent: int = 0) -> str:
    lines: List[str] = []
    prefix = " " * indent

    if isinstance(payload, dict):
        for key, value in payload.items():
            if isinstance(value, (dict, list)):
                lines.append(f"{prefix}{key}:")
                lines.append(render_yaml(value, indent + 2))
            else:
                lines.append(f"{prefix}{key}: {_scalar(value)}")
        return "\n".join(lines)

    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                lines.append(f"{prefix}-")
                lines.append(render_yaml(item, indent + 2))
            else:
                lines.append(f"{prefix}- {_scalar(item)}")
        return "\n".join(lines)

    return f"{prefix}{_scalar(payload)}"


def apply_smoke_test_filters(
    candidates: List[Dict[str, object]],
    test_ranks: str,
    test_limit_per_rank: int,
) -> List[Dict[str, object]]:
    if not test_ranks and test_limit_per_rank <= 0:
        return candidates

    selected_ranks = (
        {int(rank.strip()) for rank in test_ranks.split(",") if rank.strip()}
        if test_ranks
        else None
    )

    counters: Dict[int, int] = {}
    filtered: List[Dict[str, object]] = []
    for candidate in candidates:
        rank = int(candidate["from_rank"])
        if selected_ranks and rank not in selected_ranks:
            continue
        counters.setdefault(rank, 0)
        if test_limit_per_rank > 0 and counters[rank] >= test_limit_per_rank:
            continue
        counters[rank] += 1
        filtered.append(candidate)
    return filtered


def _task(
    task_key: str,
    notebook_path: str,
    config: PocConfig,
    depends_on: List[str] | None = None,
    extra_params: Dict[str, str] | None = None,
) -> Dict[str, object]:
    params = {
        "CATALOG_NAME": config.catalog_name,
        "VOLUME_NAME": config.volume_name,
        "TARGET_TABLE_NAME": config.target_table_name,
        "LINEAGE_SOURCE": config.lineage_source,
    }
    if extra_params:
        params.update(extra_params)

    task: Dict[str, object] = {
        "task_key": task_key,
        "notebook_task": {
            "notebook_path": notebook_path,
            "base_parameters": params,
        },
    }
    if depends_on:
        task["depends_on"] = [{"task_key": item} for item in depends_on]
    return task


def _scalar(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "null"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value)
    if any(symbol in text for symbol in [":", "{", "}", "[", "]", ",", "$", "#", " "]):
        return f'"{text}"'
    return text
