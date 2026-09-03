"""Tool for analyzing PR performance against periodic baselines."""

import json
import logging
from typing import Annotated

from fastmcp import Context
from fastmcp.tools import tool
from pydantic import Field

from components.common import (
    ConfigParam,
    InputVarsParam,
    config_path,
    extract_and_set_es_server,
    parse_input_vars,
    split_configs,
)
from utils.constants import DEFAULT_LOOKBACK_DAYS
from utils.utils import run_orion

logger = logging.getLogger(__name__)


def _add_percentage_changes(pulls_list: list[dict], periodic_avg: dict) -> None:
    """Mutate pull run metric entries to add percentage_change vs periodic baseline."""
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
    lookback: str = DEFAULT_LOOKBACK_DAYS,
    *,
    configs: list[str] | None = None,
    input_vars: dict | None = None,
) -> list[dict]:
    """Get PR performance analysis details by running Orion with input variables."""
    if not configs:
        raise ValueError("config_name is required — call discover_jobs with job_type='pull' first to resolve PR configs")

    if not pull_requests:
        raise ValueError("At least one pull request number is required")
    try:
        pull_numbers = [int(pr) for pr in pull_requests]
    except ValueError as exc:
        raise ValueError("Pull request numbers must be integers") from exc

    pr_iv = dict(input_vars) if input_vars else {}
    pr_iv["jobtype"] = "pull"
    pr_iv["organization"] = organization
    pr_iv["repository"] = repository
    pr_iv["pull_number"] = pull_requests[0]

    summaries: list[dict] = []
    for config in configs:
        full_config_path = config_path(config)
        result = await run_orion(
            config=full_config_path,
            version=version,
            lookback=lookback,
            input_vars=pr_iv,
            pr_analysis=True,
            pull_numbers=pull_numbers,
        )

        try:
            data = json.loads(result.stdout)
        except json.JSONDecodeError as e:
            logger.error("Failed to parse orion output for %s: %s", full_config_path, e)
            continue

        if not isinstance(data, dict):
            logger.error("Unexpected data type from orion: %s", type(data))
            continue

        if "periodic_avg" not in data:
            logger.warning("Missing periodic_avg in orion output for %s", full_config_path)
            continue

        periodic_avg = data["periodic_avg"]

        if "pulls" not in data:
            logger.warning("Missing pulls in orion output for %s", full_config_path)
            continue

        pulls_list = data["pulls"]
        _add_percentage_changes(pulls_list, periodic_avg)

        summaries.append({
            "config": full_config_path,
            "periodic_avg": data["periodic_avg"],
            "pulls": pulls_list,
        })

    return summaries


@tool
async def openshift_report_on_pr(
    version: Annotated[str, Field(description="OpenShift version to analyze")] = "4.20",
    *,
    lookback: Annotated[str, Field(description="Number of days to lookback")] = DEFAULT_LOOKBACK_DAYS,
    organization: Annotated[str, Field(description="Organization to look into")] = "openshift",
    repository: Annotated[str, Field(description="Repository to look into")] = "ovn-kubernetes",
    pull_request: Annotated[str, Field(description="PR number to analyze (for single PR)")] = "2841",
    pull_requests: Annotated[str, Field(description="Comma-separated PR numbers to compare (e.g. '3169,3170'). Overrides pull_request if provided.")] = "",
    config_name: ConfigParam = None,
    input_vars: InputVarsParam = "",
    ctx: Context = None,
) -> dict:
    """Captures a performance analysis against the specified OpenShift version using Orion.

    Args:
        version: OpenShift version to analyze.
        lookback: The number of days to look back for performance data.
        organization: The organization to look into.
        repository: The repository to look into.
        pull_request: Single PR number to analyze.
        pull_requests: Comma-separated PR numbers for multi-PR comparison.
        config_name: Orion config filename or comma-separated list.
        input_vars: JSON string of template variables for the config.

    Returns:
        Dictionary with summaries containing PR analysis results for each config.
    """
    extract_and_set_es_server(ctx)

    try:
        iv = parse_input_vars(input_vars)
    except ValueError as exc:
        return {"summaries": [], "error": str(exc)}

    if pull_requests and pull_requests.strip():
        pr_list = [pr.strip() for pr in pull_requests.split(",") if pr.strip()]
    else:
        pr_list = [pull_request]

    if not config_name:
        return {"summaries": [], "error": "config_name is required — call discover_jobs with job_type='pull' first to resolve PR configs"}
    configs = split_configs(config_name)

    try:
        summaries = await get_pr_details(organization, repository, pr_list, version, lookback,
                                         configs=configs, input_vars=iv)
    except ValueError as exc:
        return {"summaries": [], "error": str(exc)}

    if not summaries:
        return {
            "summaries": [],
            "message": "No performance data found for this PR. Please ensure the PR has been tested and the version is correct."
        }
    return {"summaries": summaries}
