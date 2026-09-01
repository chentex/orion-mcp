import json
import os
from typing import Annotated

from pydantic import Field
from fastmcp import Context
from fastmcp.tools import tool

from utils.constants import ORION_CONFIGS_PATH
from utils.utils import (
    run_orion,
    parse_nightly_version,
    filter_data_by_timestamp,
)
from utils.config_parser import _timestamp_after
from utils.es_context import extract_and_set_es_server


def _extract_regression_details(stdout: str) -> list[dict]:
    data = json.loads(stdout)
    details: list[dict] = []
    for idx, dat in enumerate(data):
        if not dat.get("is_changepoint"):
            continue

        metrics: list[str] = []
        for metric_name, metric_info in dat.get("metrics", {}).items():
            percentage_change = metric_info.get("percentage_change", 0)
            if percentage_change > 0:
                metrics.append(f"{metric_name} increased by {percentage_change:.2f}%")
            elif percentage_change < 0:
                metrics.append(f"{metric_name} decreased by {abs(percentage_change):.2f}%")

        prev_doc = data[idx - 1] if idx > 0 else None
        prev_ocp_version = prev_doc.get("ocpVersion") if isinstance(prev_doc, dict) else None

        current_prs = dat.get("prs", []) or []
        prev_prs = (prev_doc.get("prs", []) if isinstance(prev_doc, dict) else []) or []
        prs_added = [p for p in current_prs if p not in prev_prs]

        details.append({
            "uuid": dat.get("uuid"),
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
) -> str:
    full_config_paths = [os.path.join(ORION_CONFIGS_PATH, config) for config in configs]
    changepoints: list[str] = []

    for full_config_path in full_config_paths:
        result = await run_orion(
            config=full_config_path,
            version=version,
            lookback=lookback,
            jira_ack=True,
            jira_status_filter="Done",
        )

        if result.returncode not in (0, 3):
            details = _extract_regression_details(result.stdout)
            for det in details:
                header_lines = [
                    f"Change detected in configuration: '{full_config_path}'",
                    f"UUID: {det.get('uuid')}",
                    f"OCP Version: {det.get('ocpVersion')}",
                    f"Previous OCP Version: {det.get('previousOcpVersion')}",
                    "PRs added since Previous OCP Version:",
                ]
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


@tool
async def has_openshift_regressed(
    version: Annotated[str, Field(description="Version of OpenShift to look into")] = "4.19",
    lookback: Annotated[str, Field(description="Number of days to lookback")] = "15",
    ctx: Context = None,
) -> str:
    """Runs a performance regression analysis against the OpenShift version using Orion.

    Orion uses an EDivisive algorithm to analyze performance data from a specified
    configuration file to detect any performance regressions.
    """
    extract_and_set_es_server(ctx)

    configs = [
        "trt-external-payload-cluster-density.yaml",
        "trt-external-payload-node-density.yaml",
        "trt-external-payload-node-density-cni.yaml",
        "trt-external-payload-crd-scale.yaml",
        "trt-external-payload-udn-density-pods.yaml",
    ]
    return await _run_regression_checks(configs, version=version, lookback=lookback)


@tool
async def has_networking_regressed(
    version: Annotated[str, Field(description="Version of OpenShift to look into")] = "4.19",
    lookback: Annotated[str, Field(description="Number of days to lookback")] = "15",
    ctx: Context = None,
) -> str:
    """Runs a performance regression analysis against networking-focused configs.

    Checks only: small-scale-udn-l3.yaml, trt-external-payload-node-density-cni.yaml.
    """
    extract_and_set_es_server(ctx)

    configs = [
        "small-scale-udn-l3.yaml",
        "trt-external-payload-node-density-cni.yaml",
    ]
    return await _run_regression_checks(configs, version=version, lookback=lookback)


@tool
async def has_nightly_regressed(
    nightly_version: Annotated[str, Field(description="Full nightly version string (e.g., '4.22.0-0.nightly-2026-01-05-203335')")],
    previous_nightly: Annotated[str, Field(description="Optional previous nightly to compare against (e.g., '4.22.0-0.nightly-2026-01-01-123456')")] = "",
    lookback: Annotated[str, Field(description="Number of days to lookback")] = "30",
    configs: Annotated[str, Field(description="Comma-separated list of config files (optional, defaults to TRT configs)")] = "",
    ctx: Context = None,
) -> str:
    """Detect regressions for a specific OpenShift nightly version.

    Parses the nightly version to extract major version and date, queries Orion,
    filters data to the nightly date, and reports any changepoints found.
    """
    extract_and_set_es_server(ctx)

    try:
        nightly_info = parse_nightly_version(nightly_version)
    except ValueError as e:
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

    config_list = ([c.strip() for c in configs.split(",") if c.strip()] if configs.strip() else [
        "trt-external-payload-cluster-density.yaml",
        "trt-external-payload-node-density.yaml",
        "trt-external-payload-node-density-cni.yaml",
        "trt-external-payload-crd-scale.yaml",
        "trt-external-payload-udn-density-pods.yaml",
    ])

    all_regressions: list[str] = []

    for config in config_list:
        full_config_path = os.path.join(ORION_CONFIGS_PATH, config)
        result = await run_orion(
            config=full_config_path,
            version=nightly_info.major_version,
            lookback=lookback,
            jira_ack=True,
            jira_status_filter="Done",
        )

        try:
            data = json.loads(result.stdout)
            if not isinstance(data, list):
                continue
            data = filter_data_by_timestamp(data, nightly_info.nightly_date)
            if prev_nightly_info:
                data = [e for e in data if e.get("timestamp") and _timestamp_after(e["timestamp"], prev_nightly_info.nightly_date)]
        except (json.JSONDecodeError, TypeError):
            continue

        for idx, entry in enumerate(data):
            if not entry.get("is_changepoint"):
                continue

            metrics = []
            for name, info in entry.get("metrics", {}).items():
                pct = info.get("percentage_change", 0)
                if pct != 0:
                    metrics.append(f"{name} {'increased' if pct > 0 else 'decreased'} by {abs(pct):.2f}%")

            prev = data[idx - 1] if idx > 0 else {}
            prs_added = [p for p in (entry.get("prs") or []) if p not in (prev.get("prs") or [])]

            lines = [
                f"Regression in {nightly_info.full_version}",
                f"Config: {config}",
                f"UUID: {entry.get('uuid')}",
                f"Version: {entry.get('ocpVersion')} (prev: {prev.get('ocpVersion', 'N/A')})",
            ]
            if prev_nightly_info:
                lines.insert(1, f"Comparing against: {prev_nightly_info.full_version}")
            if prs_added:
                lines.append(f"PRs: {', '.join(prs_added)}")
            if metrics:
                lines.append(f"Metrics: {'; '.join(metrics)}")

            all_regressions.append("\n".join(lines))

    return "\n\n".join(all_regressions) if all_regressions else "No regressions found"
