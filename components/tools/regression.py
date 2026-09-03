"""Tools for detecting performance regressions via changepoint analysis."""

import json
import logging
import os
from datetime import datetime
from typing import Annotated

from fastmcp import Context
from fastmcp.tools import tool
from pydantic import Field

from components.common import (
    ConfigParam,
    InputVarsParam,
    LookbackParam,
    VersionParam,
    config_path,
    extract_and_set_es_server,
    orion_error_snippet,
    parse_input_vars,
    split_configs,
)
from utils.constants import DEFAULT_LOOKBACK_DAYS, DEFAULT_NETWORKING_CONFIGS
from utils.utils import (
    filter_data_by_timestamp,
    parse_nightly_version,
    parse_timestamp,
    run_orion,
)

logger = logging.getLogger(__name__)


def _extract_regression_details(stdout: str) -> list[dict]:
    """Extract changepoint details from Orion JSON output."""
    data = json.loads(stdout)
    if not isinstance(data, list):
        return []
    details: list[dict] = []
    for idx, dat in enumerate(data):
        if not dat.get("is_changepoint"):
            continue

        metrics: list[str] = []
        for metric_name, metric_info in dat.get("metrics", {}).items():
            pct = metric_info.get("percentage_change")
            if not isinstance(pct, (int, float)) or isinstance(pct, bool) or pct == 0:
                continue
            direction = "increased" if pct > 0 else "decreased"
            metrics.append(f"{metric_name} {direction} by {abs(pct):.2f}%")

        prev_doc = data[idx - 1] if idx > 0 else None
        prev_ocp_version = prev_doc.get("ocpVersion") if isinstance(prev_doc, dict) else None

        current_prs = dat.get("prs", []) or []
        prev_prs = (prev_doc.get("prs", []) if isinstance(prev_doc, dict) else []) or []
        prs_added = [p for p in current_prs if p not in prev_prs]

        details.append({
            "buildUrl": dat.get("buildUrl"),
            "ocpVersion": dat.get("ocpVersion"),
            "previousOcpVersion": prev_ocp_version,
            "prs_added": prs_added,
            "metrics": metrics,
        })

    return details


async def _run_regression_checks(
    configs: list[str],
    version: str,
    lookback: str,
    input_vars: dict | None = None,
) -> str:
    """Execute Orion across configs and return a summary of detected changepoints."""
    full_config_paths = [config_path(config) for config in configs]
    changepoints: list[str] = []

    for full_config_path in full_config_paths:
        result = await run_orion(
            config=full_config_path,
            version=version,
            lookback=lookback,
            input_vars=input_vars,
            jira_ack=True,
            jira_status_filter="Done",
        )

        config_short = os.path.basename(full_config_path)
        if result.returncode == 3:
            continue

        try:
            details = _extract_regression_details(result.stdout)
        except (json.JSONDecodeError, TypeError) as exc:
            logger.error("Failed to parse Orion output for %s (exit %d): %s", config_short, result.returncode, exc)
            stderr_snippet = orion_error_snippet(result)
            changepoints.append(f"❌ Error: Orion failed for {config_short} (exit {result.returncode}): {stderr_snippet}")
            continue

        for det in details:
            header_lines = [
                f"⚠️ Change detected in configuration: '{config_short}'",
                f"OCP Version: {det.get('ocpVersion')}",
                f"Previous OCP Version: {det.get('previousOcpVersion')}",
            ]
            build_url = det.get("buildUrl")
            if build_url:
                header_lines.append(f"Build URL: {build_url}")
            header_lines.append("PRs added since Previous OCP Version:")
            prs_added = det.get("prs_added") or []
            if prs_added:
                header_lines.extend([f"  - {pr}" for pr in prs_added])
            else:
                header_lines.append("  - None")

            metrics_list = det.get("metrics", [])
            if metrics_list:
                header_lines.append("Affected metrics:")
                header_lines.extend([f"  - {m}" for m in metrics_list])

            changepoints.append("\n".join(header_lines))

    if changepoints:
        return "\n\n".join(changepoints)
    return "No changepoints found"


async def _check_regression(
    ctx,
    config_name,
    input_vars,
    version: str,
    lookback: str,
    default_configs: list[str] | None = None,
) -> str:
    """Shared body for regression tools."""
    extract_and_set_es_server(ctx)
    try:
        iv = parse_input_vars(input_vars)
    except ValueError as exc:
        return f"Error: {exc}"
    configs = split_configs(config_name, default=default_configs)
    return await _run_regression_checks(configs, version=version, lookback=lookback, input_vars=iv)


@tool
async def has_openshift_regressed(
    version: VersionParam = "4.19",
    lookback: LookbackParam = DEFAULT_LOOKBACK_DAYS,
    config_name: ConfigParam = None,
    input_vars: InputVarsParam = "",
    ctx: Context = None,
) -> str:
    """Check if an OpenShift version has performance regressions using changepoint detection.

    Triggers: "has 4.22 regressed", "check regressions for 5.0",
    "are there regressions in 4.20".

    Args:
        version: OpenShift version (default: '4.19').
        lookback: Days to look back (default: '15').
        config_name: Orion config filename or comma-separated list.
        input_vars: JSON string of template variables for the config.

    Returns:
        Changepoint details or "No changepoints found".
    """
    return await _check_regression(ctx, config_name, input_vars, version, lookback)


@tool
async def has_networking_regressed(
    version: VersionParam = "4.19",
    lookback: LookbackParam = DEFAULT_LOOKBACK_DAYS,
    config_name: ConfigParam = None,
    input_vars: InputVarsParam = "",
    ctx: Context = None,
) -> str:
    """Check if networking benchmarks (node-density-cni, udn-*) have regressed for an OpenShift version.

    Triggers: "has networking regressed in 4.22", "check networking regressions",
    "any CNI or UDN regressions in 5.0".

    Args:
        version: OpenShift version (default: '4.19').
        lookback: Days to look back (default: '15').
        config_name: Orion config filename or comma-separated list.
        input_vars: JSON string of template variables for the config.

    Returns:
        Changepoint details or "No changepoints found".
    """
    return await _check_regression(ctx, config_name, input_vars, version, lookback,
                                   default_configs=DEFAULT_NETWORKING_CONFIGS)


def _timestamp_after(timestamp_val, cutoff_datetime: datetime) -> bool:
    """Check if a timestamp is after the cutoff datetime."""
    entry_dt = parse_timestamp(timestamp_val)
    return entry_dt is not None and entry_dt > cutoff_datetime


@tool
async def has_nightly_regressed(
    nightly_version: Annotated[str, Field(description="Full nightly version string (e.g., '4.22.0-0.nightly-2026-01-05-203335')")],
    previous_nightly: Annotated[str, Field(description="Optional previous nightly to compare against")] = "",
    lookback: LookbackParam = DEFAULT_LOOKBACK_DAYS,
    config_name: ConfigParam = None,
    input_vars: InputVarsParam = "",
    ctx: Context = None,
) -> str:
    """Check if a specific nightly build has regressions by running changepoint detection scoped to that build's time window.

    Triggers: "inspect nightly 5.0.0-0.nightly-2026-08-10-122052", "has this nightly regressed",
    "check nightly build", "compare nightly X vs Y".

    Args:
        nightly_version: Full nightly string (required).
        previous_nightly: Earlier nightly to scope the comparison window (default: empty).
        lookback: Days to look back (default: '15').
        config_name: Orion config filename or comma-separated list.
        input_vars: JSON string of template variables for the config.

    Returns:
        Regression details or "No regressions found".
    """
    extract_and_set_es_server(ctx)

    try:
        nightly_info = parse_nightly_version(nightly_version)
    except ValueError as e:
        logger.error("Error parsing nightly version '%s': %s", nightly_version, e)
        return f"Error parsing nightly version: {e}"

    if not nightly_info.is_nightly:
        return f"Error: '{nightly_version}' is not a nightly version."

    prev_nightly_info = None
    if previous_nightly.strip():
        try:
            prev_nightly_info = parse_nightly_version(previous_nightly)
        except ValueError as e:
            return f"Error parsing previous_nightly: {e}"
        if not prev_nightly_info.is_nightly:
            return f"Error: '{previous_nightly}' is not a nightly version."
        if prev_nightly_info.nightly_date >= nightly_info.nightly_date:
            return "Error: previous_nightly must be earlier than nightly_version."

    try:
        iv = parse_input_vars(input_vars)
    except ValueError as exc:
        return f"Error: {exc}"
    configs = split_configs(config_name)

    all_regressions: list[str] = []
    for config_value in configs:
        full_config_path = config_path(config_value)

        result = await run_orion(
            config=full_config_path,
            version=nightly_info.major_version,
            lookback=lookback,
            input_vars=iv,
            jira_ack=True,
            jira_status_filter="Done",
        )

        try:
            data = json.loads(result.stdout)
        except (json.JSONDecodeError, TypeError) as exc:
            logger.error("Failed to parse Orion output for nightly %s, config %s: %s", nightly_version, config_value, exc)
            stderr_snippet = orion_error_snippet(result)
            all_regressions.append(f"❌ Error: Orion failed for {config_value} (exit {result.returncode}): {stderr_snippet}")
            continue

        if not isinstance(data, list):
            logger.error("Unexpected data type from Orion for nightly %s, config %s: %s", nightly_version, config_value, type(data).__name__)
            all_regressions.append(f"❌ Error: Orion returned unexpected data type for {config_value}: {type(data).__name__}")
            continue

        data = filter_data_by_timestamp(data, nightly_info.nightly_date)
        if prev_nightly_info:
            data = [e for e in data if e.get("timestamp") and _timestamp_after(e["timestamp"], prev_nightly_info.nightly_date)]

        details = _extract_regression_details(json.dumps(data))
        for det in details:
            lines = [
                f"⚠️ Regression in {nightly_info.full_version}",
                f"Config: {config_value}",
                f"Version: {det.get('ocpVersion')} (prev: {det.get('previousOcpVersion', 'N/A')})",
            ]
            if prev_nightly_info:
                lines.insert(1, f"Comparing against: {prev_nightly_info.full_version}")
            build_url = det.get("buildUrl")
            if build_url:
                lines.append(f"Build URL: {build_url}")
            prs_added = det.get("prs_added") or []
            if prs_added:
                lines.append(f"PRs: {', '.join(prs_added)}")
            metrics_list = det.get("metrics", [])
            if metrics_list:
                lines.append(f"Metrics: {'; '.join(metrics_list)}")

            all_regressions.append("\n".join(lines))

    if all_regressions:
        return "\n\n".join(all_regressions)
    return "No regressions found"
