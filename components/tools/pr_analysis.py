import json
import os
from typing import Annotated

from pydantic import Field
from fastmcp import Context
from fastmcp.tools import tool

from utils.constants import ORION_CONFIGS_PATH
from utils.utils import run_orion, safe_json_loads
from utils.es_context import extract_and_set_es_server


def _add_percentage_changes(pulls_list: list[dict], periodic_avg: dict) -> None:
    for pull_obj in pulls_list:
        for pull_entry in pull_obj.get("data", []):
            for metric_name, metric_data in pull_entry.get("metrics", {}).items():
                if metric_name not in periodic_avg:
                    periodic_value = None
                else:
                    periodic_data = periodic_avg[metric_name]
                    if isinstance(periodic_data, dict):
                        periodic_value = periodic_data.get("value")
                    else:
                        periodic_value = periodic_data

                pull_value = metric_data.get("value")
                if (
                    isinstance(periodic_value, (int, float))
                    and isinstance(pull_value, (int, float))
                    and periodic_value != 0
                ):
                    metric_data["percentage_change"] = ((pull_value - periodic_value) / periodic_value) * 100
                else:
                    metric_data["percentage_change"] = None


async def get_pr_details(
    organization: str,
    repository: str,
    pull_requests: list[str],
    version: str = "4.20",
    lookback: str = "15",
) -> list[dict]:
    """Get PR performance analysis details by running Orion with input variables."""
    configs = [
        "trt-external-payload-cluster-density.yaml",
        "trt-external-payload-node-density.yaml",
        "trt-external-payload-node-density-cni.yaml",
        "trt-external-payload-crd-scale.yaml",
        "trt-external-payload-udn-density-pods.yaml",
    ]

    if not pull_requests:
        raise ValueError("At least one pull request number is required")
    try:
        pull_numbers = [int(pr) for pr in pull_requests]
    except ValueError as exc:
        raise ValueError("Pull request numbers must be integers") from exc

    input_vars = {
        "jobtype": "pull",
        "organization": organization,
        "repository": repository,
        "pull_number": pull_requests[0],
        "version": version
    }

    full_config_paths = [os.path.join(ORION_CONFIGS_PATH, config) for config in configs]
    summaries: list[dict] = []

    for full_config_path in full_config_paths:
        result = await run_orion(
            config=full_config_path,
            version=version,
            lookback=lookback,
            input_vars=input_vars,
            pr_analysis=True,
            pull_numbers=pull_numbers,
        )

        try:
            data = safe_json_loads(result.stdout)
        except (json.JSONDecodeError, TypeError) as e:
            print(f"Failed to parse orion output for {full_config_path}: {e}")
            continue

        if not isinstance(data, dict):
            print(f"Unexpected data type from orion for {full_config_path}: {type(data)}")
            continue

        if "periodic_avg" not in data:
            print(f"Missing periodic_avg in orion output for {full_config_path}")
            continue

        periodic_avg = data["periodic_avg"]

        if "pulls" not in data:
            print(f"Missing pulls in orion output for {full_config_path}")
            continue

        pulls_list = data["pulls"]
        _add_percentage_changes(pulls_list, periodic_avg)

        summaries.append({
            "config": full_config_path,
            "periodic_avg": periodic_avg,
            "pulls": pulls_list,
        })

    return summaries


@tool
async def openshift_report_on_pr(
    version: Annotated[str, Field(description="OpenShift version to analyze")] = "4.20",
    *,
    lookback: Annotated[str, Field(description="Number of days to lookback")] = "15",
    organization: Annotated[str, Field(description="Organization to look into")] = "openshift",
    repository: Annotated[str, Field(description="Repository to look into")] = "ovn-kubernetes",
    pull_request: Annotated[str, Field(description="PR number to analyze (for single PR)")] = "2841",
    pull_requests: Annotated[str, Field(description="Comma-separated PR numbers to compare (e.g. '3169,3170'). Overrides pull_request if provided.")] = "",
    ctx: Context = None,
) -> dict:
    """Captures a performance analysis against the specified OpenShift version using Orion."""
    extract_and_set_es_server(ctx)

    if pull_requests and pull_requests.strip():
        pr_list = [pr.strip() for pr in pull_requests.split(",") if pr.strip()]
    else:
        pr_list = [pull_request]

    summaries = await get_pr_details(organization, repository, pr_list, version, lookback)
    if not summaries:
        return {
            "summaries": [],
            "message": "No performance data found for this PR. Please ensure the PR has been tested and the version is correct."
        }
    return {"summaries": summaries}
