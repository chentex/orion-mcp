"""
Model Context Protocol (MCP) server for Orion performance regression analysis.

This module provides tools for running performance regression analysis using
the cloud-bulldozer/orion library.
"""

import asyncio
import json
import logging
import os
import re
from datetime import datetime
from typing import Annotated
from pydantic import Field
import jinja2
import yaml

from mcp import types
from mcp.server.fastmcp import Context, FastMCP

# Import utility functions from utils module
import httpx

from utils.utils import (
    run_orion,
    summarize_result,
    get_data_source,
    orion_metrics,
    orion_configs,
    generate_correlation_plot,
    list_orion_configs,
    parse_nightly_version,
    parse_timestamp,
    filter_data_by_timestamp,
    get_es_metadata_index,
    current_es_config,  # Context variable for ES config isolation
)
from utils.header_decryption import get_es_config_from_headers

RELEASE_DATES = {
    "4.17": "2024-10-29",
    "4.18": "2025-02-28",
    "4.19": "2025-06-17",
    "4.20": "2025-10-23",
    "4.21": "2026-02-25",
    "4.22": "2026-06-17",
    "5.0": "2026-10-31",
}

mcp = FastMCP(name="orion-mcp",
              host="0.0.0.0",
              port=3030,
              log_level='INFO')

ORION_CONFIGS_PATH = "/Users/balatripurakumaribodapati/Desktop/orion-ai/orion/examples/"
ORION_CONFIGS = list_orion_configs()

FULL_ORION_CONFIG_PATHS = [os.path.join(ORION_CONFIGS_PATH, config) for config in ORION_CONFIGS]

logger = logging.getLogger(__name__)

# Common parameter types — define once, reuse across all tools
VersionParam = Annotated[str, Field(description="OpenShift version (e.g. '4.22', '5.0')")]
LookbackParam = Annotated[str, Field(description="Number of days to lookback")]
ConfigParam = Annotated[str | None, Field(
    description="Orion configuration file name (e.g. 'cluster-density.yaml'). For regression tools, supports comma-separated list (e.g. 'cluster-density.yaml,node-density.yaml').",
)]
InputVarsParam = Annotated[str, Field(
    description="JSON string of template variables for the config (e.g. platform, workerNodesCount, clusterType, fips, ipsec, encrypted, networkType, masterNodesType, masterNodesCount, workerNodesType, jobtype).",
)]




def _parse_input_vars(input_vars: str) -> dict | None:
    """Parse a JSON input_vars string into a dict, or return None if empty.

    Raises ValueError on malformed JSON so callers can distinguish
    "not provided" (None) from "provided but broken".
    """
    if not input_vars:
        return None
    try:
        return json.loads(input_vars)
    except (json.JSONDecodeError, TypeError) as exc:
        raise ValueError(f"Malformed input_vars JSON: {exc}") from exc


DEFAULT_CONFIG = "cluster-density.yaml"


def _split_configs(config_name: str | None) -> list[str]:
    """Split a comma-separated config_name into a list. Returns [DEFAULT_CONFIG] if empty."""
    if not config_name:
        return [DEFAULT_CONFIG]
    return [c.strip() for c in config_name.split(",") if c.strip()]


async def _resolve_config_and_vars(
    ctx,
    config_name: str | None,
    version: str,
    input_vars: str = "",
) -> tuple[str, dict | None, dict]:
    """Common setup for tools: extract ES config, parse config name and input_vars.

    Returns (config_value, iv, search_info).
    """
    _extract_and_set_es_server(ctx)
    config_value = config_name or DEFAULT_CONFIG
    search_info = {}
    try:
        iv = _parse_input_vars(input_vars)
    except ValueError as exc:
        search_info["input_vars_error"] = str(exc)
        iv = None
    return config_value, iv, search_info


def _extract_and_set_es_server(ctx) -> None:
    """
    Extract ES config from request headers and set in context variable.

    If encrypted ES config found in headers, decrypts it and sets current_es_config
    context variable. Config includes: es_server, es_metadata_index, es_benchmark_index.
    Downstream code (get_data_source, get_es_metadata_index, get_es_benchmark_index)
    checks context first.

    Falls back to environment variables if no header present.
    """
    if not ctx:
        return

    # Access HTTP headers through ctx.request_context.request.headers (Starlette Request)
    try:
        if hasattr(ctx, 'request_context') and ctx.request_context:
            request = ctx.request_context.request
            if request and hasattr(request, 'headers'):
                # Convert Starlette Headers to dict
                headers_dict = dict(request.headers)
                es_config = get_es_config_from_headers(headers_dict)
                if es_config:
                    current_es_config.set(es_config)
    except Exception:
        # Silently fall back to environment variables
        pass


@mcp.resource("orion-mcp://release_dates")
def release_dates_resource() -> dict[str, str]:
    """
    Provides the release dates for the different OpenShift versions.
    """
    return RELEASE_DATES

@mcp.resource("orion-mcp://get_data_source")
def get_data_source_resource() -> str:
    """
    Provides the data source URL for Orion analysis.

    User must launch MCP server with the environment variable ES_SERVER
    set to the OpenSearch URL.

    Returns:
        The OpenSearch URL as a string.
    """
    return get_data_source()

@mcp.tool()
async def get_release_date(
    version : Annotated[str, Field(description="OCP Version to get Release date")] = "4.20") -> str:
    """Look up when an OpenShift version was released (GA date). Use when a user asks "when did X release" or "what is the release date for X".

    Triggers: "when did 4.19 release", "release date for 4.20", "when was 5.0 GA".

    Args:
        version: OpenShift version (default: '4.20').

    Returns:
        Release date string or "Invalid version".
    """
    if version in RELEASE_DATES :
        return RELEASE_DATES[version]
    return f"Invalid version: {version}"

@mcp.tool()
def get_orion_configs() -> list[str]:
    """List all available benchmark config files. Use when a user asks "what benchmarks exist", "list configs", or "what workloads can I test".

    Triggers: "what configs are available", "list benchmarks", "show all workloads".

    Returns:
        List of config filenames (e.g. ['cluster-density.yaml', 'node-density.yaml', ...]).
    """
    return orion_configs(ORION_CONFIGS)


_discover_jobs_cache: dict = {}
_CACHE_TTL_SECONDS = 86400  # 24 hours

_GCSWEB_BASE = "https://gcsweb-ci.apps.ci.l2s4.p1.openshiftapps.com/gcs"
_PROW_VIEW_PREFIX = "https://prow.ci.openshift.org/view/gs/"


async def _resolve_configs_from_prow(build_url: str) -> list[str]:
    """Resolve Orion config filenames from prow build-log artifacts.

    Deterministic: HTTP GET artifact listings + grep ORION_CONFIG= from build logs.
    Generic for any workload — new jobs work automatically if they log ORION_CONFIG.
    """
    if not build_url or _PROW_VIEW_PREFIX not in build_url:
        return []

    gcs_path = build_url.replace(_PROW_VIEW_PREFIX, "")
    gcs_base = f"{_GCSWEB_BASE}/{gcs_path}"

    configs = set()
    try:
        async with httpx.AsyncClient(timeout=15, verify=False, follow_redirects=True) as client:
            resp = await client.get(f"{gcs_base}/artifacts/")
            if resp.status_code != 200:
                return []

            dirs = re.findall(r'>\s*([a-zA-Z0-9][a-zA-Z0-9._-]+)/<', resp.text)
            test_dirs = [d for d in dirs if d not in ("build-resources", "release")]
            if not test_dirs:
                return []
            test_name = test_dirs[0]

            resp = await client.get(f"{gcs_base}/artifacts/{test_name}/")
            if resp.status_code != 200:
                return []

            orion_dirs = re.findall(r'>\s*(openshift-qe-orion-[a-zA-Z0-9_-]+)/<', resp.text)
            if not orion_dirs:
                return []

            async def _fetch_config(step_dir: str) -> str | None:
                try:
                    r = await client.get(
                        f"{gcs_base}/artifacts/{test_name}/{step_dir}/build-log.txt"
                    )
                    if r.status_code != 200:
                        return None
                    m = re.search(r'ORION_CONFIG=examples/(\S+\.yaml)', r.text)
                    return m.group(1) if m else None
                except Exception:
                    return None

            results = await asyncio.gather(*[_fetch_config(d) for d in orion_dirs])
            configs = {r for r in results if r}
    except Exception as exc:
        logger.warning("Failed to resolve configs from prow: %s", exc)

    return sorted(configs)


@mcp.tool()
async def discover_jobs(
    version: VersionParam = "",
    platform: Annotated[str | None, Field(description="Platform filter (e.g. 'AWS', 'GCP', 'BareMetal')")] = None,
    cluster_type: Annotated[str | None, Field(description="Cluster type filter (e.g. 'self-managed', 'rosa-hcp')")] = None,
    workload: Annotated[str | None, Field(description="Workload substring filter on job name (e.g. 'payload', 'control-plane')")] = None,
    scale: Annotated[int | None, Field(description="Worker node count filter (e.g. 6, 24)")] = None,
    fips: Annotated[str | None, Field(description="FIPS filter ('true' or 'false')")] = None,
    ipsec: Annotated[str | None, Field(description="IPsec filter ('true' or 'false')")] = None,
    encrypted: Annotated[str | None, Field(description="Encryption filter ('true' or 'false')")] = None,
    ctx: Context = None,
) -> dict:
    """Discover CI jobs, benchmarks, config files, and cluster metadata from Elasticsearch and prow artifacts.

    Returns job names, benchmarks, resolved Orion config filenames, cluster metadata, and build URLs.
    Config files are resolved automatically from prow build logs (deterministic, cached 24h).

    Args:
        version: OCP version prefix filter (e.g. '4.22'). Empty string returns all versions.
        platform: Platform filter (e.g. 'AWS', 'GCP', 'BareMetal').
        cluster_type: Cluster type filter (e.g. 'self-managed', 'rosa-hcp').
        workload: Substring filter on job name (e.g. 'payload', 'control-plane', 'udn').
        scale: Worker node count filter (e.g. 6, 24).
        fips: FIPS filter ('true' or 'false').
        ipsec: IPsec filter ('true' or 'false').
        encrypted: Encryption filter ('true' or 'false').

    Returns:
        Dict with 'jobs' mapping job names to benchmarks, configs (list of config filenames), metadata, buildUrl.
    """
    _extract_and_set_es_server(ctx)

    cache_key = f"{version}|{platform}|{cluster_type}|{workload}|{scale}|{fips}|{ipsec}|{encrypted}"
    now = datetime.now().timestamp()
    if cache_key in _discover_jobs_cache:
        cached_time, cached_result = _discover_jobs_cache[cache_key]
        if now - cached_time < _CACHE_TTL_SECONDS:
            return cached_result

    es_server = get_data_source()
    es_index = get_es_metadata_index()

    must_clauses = [{"term": {"jobType": "periodic"}}]
    if version:
        must_clauses.append({"prefix": {"ocpVersion.keyword": version}})
    if platform:
        must_clauses.append({"term": {"platform.keyword": platform}})
    if cluster_type:
        must_clauses.append({"term": {"clusterType.keyword": cluster_type}})
    if scale is not None:
        must_clauses.append({"term": {"workerNodesCount": scale}})
    if fips:
        must_clauses.append({"term": {"fips": fips}})
    if ipsec:
        must_clauses.append({"term": {"ipsec": ipsec}})
    if encrypted:
        must_clauses.append({"term": {"encrypted": encrypted}})

    query = {
        "size": 0,
        "query": {"bool": {"must": must_clauses}},
        "aggs": {
            "jobs": {
                "terms": {"field": "upstreamJob.keyword", "size": 200},
                "aggs": {
                    "benchmarks": {"terms": {"field": "benchmark.keyword", "size": 10}},
                    "top_hit": {
                        "top_hits": {
                            "size": 1,
                            "sort": [{"timestamp": {"order": "desc"}}],
                            "_source": [
                                "platform", "clusterType", "workerNodesCount",
                                "networkType", "fips", "ipsec", "encrypted",
                                "masterNodesType", "masterNodesCount",
                                "workerNodesType", "ocpVersion", "buildUrl",
                            ],
                        }
                    },
                }
            }
        },
    }

    if workload:
        query["query"]["bool"].setdefault("filter", []).append(
            {"wildcard": {"upstreamJob.keyword": {"value": f"*{workload}*"}}}
        )

    try:
        async with httpx.AsyncClient(timeout=30, verify=False) as client:
            resp = await client.post(
                f"{es_server}/{es_index}/_search",
                json=query,
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:
        return {"error": f"ES query failed: {exc}"}

    jobs = {}
    for bucket in data.get("aggregations", {}).get("jobs", {}).get("buckets", []):
        job_name = bucket["key"]
        benchmarks = [b["key"] for b in bucket.get("benchmarks", {}).get("buckets", [])]
        hit = bucket.get("top_hit", {}).get("hits", {}).get("hits", [])
        metadata = hit[0]["_source"] if hit else {}

        jobs[job_name] = {
            "benchmarks": benchmarks,
            "metadata": {
                "platform": metadata.get("platform", ""),
                "clusterType": metadata.get("clusterType", ""),
                "workerNodesCount": str(metadata.get("workerNodesCount", "")),
                "networkType": metadata.get("networkType", ""),
                "fips": str(metadata.get("fips", "false")),
                "ipsec": str(metadata.get("ipsec", "false")),
                "encrypted": str(metadata.get("encrypted", "false")),
                "masterNodesType": metadata.get("masterNodesType", ""),
                "masterNodesCount": str(metadata.get("masterNodesCount", "")),
                "workerNodesType": metadata.get("workerNodesType", ""),
            },
            "buildUrl": metadata.get("buildUrl", ""),
        }

    # Resolve config files from prow build logs (concurrent, all jobs at once)
    async def _resolve_for_job(job_data):
        job_data["configs"] = await _resolve_configs_from_prow(job_data.get("buildUrl", ""))

    await asyncio.gather(*[_resolve_for_job(d) for d in jobs.values()])

    result = {"jobs": jobs, "total": len(jobs)}
    _discover_jobs_cache[cache_key] = (now, result)
    return result


@mcp.tool()
async def get_orion_metrics(
    config_name: ConfigParam = None,
    version: VersionParam = "4.20",
    input_vars: InputVarsParam = "",
    ctx: Context = None,
) -> dict:
    """List what metrics a benchmark tracks. Use when a user asks "what metrics does cluster-density have" or "list metrics for node-density".

    Triggers: "what metrics does cluster-density track", "list metrics for node-density",
    "what can I measure for this config".

    Args:
        config_name: Orion config filename (default: 'cluster-density.yaml').
        version: OpenShift version (default: '4.20').
        input_vars: JSON string of template variables for the config.

    Returns:
        Dict keyed by config with list of metric names.
    """
    effective_config, iv, search_info = await _resolve_config_and_vars(ctx, config_name, version, input_vars)

    result = await orion_metrics([ORION_CONFIGS_PATH + effective_config], version=version, input_vars=iv)

    if isinstance(result, str):
        return {"error": f"Failed to fetch Orion metrics: {result}"}

    if search_info:
        result["search"] = search_info
    return result


@mcp.tool()
async def get_orion_metrics_with_meta(
    config_name: ConfigParam = None,
    version: VersionParam = "4.19",
    input_vars: InputVarsParam = "",
    ctx: Context = None,
) -> dict:
    """Get metric details including thresholds, directions (higher-is-better or lower-is-better), and labels for a benchmark. Use when a user asks "what are the thresholds", "which direction is good for ovnCPU", or "show metric metadata".

    Triggers: "what are the metric thresholds for cluster-density", "which metrics are higher-is-better",
    "show metric details".

    Args:
        config_name: Orion config filename (default: 'cluster-density.yaml').
        version: OpenShift version (default: '4.19').
        input_vars: JSON string of template variables for the config.

    Returns:
        Dict with "metrics" (list of names) and "meta" (per-metric label, direction, threshold).
    """
    effective_config, iv, search_info = await _resolve_config_and_vars(ctx, config_name, version, input_vars)
    try:
        metrics, meta_map = _load_config_metrics_with_meta(
            os.path.join(ORION_CONFIGS_PATH, effective_config),
            version=version,
            input_vars=iv,
        )
        resp = {"metrics": metrics, "meta": meta_map}
        if search_info:
            resp["search"] = search_info
        return resp
    except Exception as e:
        result = await orion_metrics(
            [ORION_CONFIGS_PATH + effective_config], version=version, input_vars=iv,
        )
        if isinstance(result, str):
            return {"error": f"{e} | {result}"}
        resp = {"metrics": result, "meta": {}, "warning": str(e)}
        if search_info:
            resp["search"] = search_info
        return resp


@mcp.tool()
async def openshift_report_on(
    versions: Annotated[str, Field(description="Comma-separated list of OpenShift versions e.g. '4.19,4.20'")] = "4.19",
    lookback: LookbackParam = "15",
    since: Annotated[str, Field(description="Date to begin lookback")] = None,
    *,
    metric: Annotated[str, Field(description="Metric to analyze")] = "podReadyLatency_P99",
    config_name: ConfigParam = None,
    input_vars: InputVarsParam = "",
    ctx: Context = None,
) -> dict:
    """Compare or fetch a specific metric across OpenShift versions in a single call. Pass metric name and comma-separated versions (e.g. versions='4.22,5.0', metric='podReadyLatency_P99').

    Triggers: "compare podReadyLatency_P99 for 4.22 vs 5.0", "show ovnCPU_avg values for 4.20",
    "get podReadyLatency data", "etcdCPU numbers for 4.22 and 5.0".

    Args:
        versions: Comma-separated versions (default: '4.19'). Example: '4.22,5.0' for multi-version comparison.
        lookback: Days to look back (default: '15').
        since: Start date for lookback (default: None).
        metric: Metric to return (default: 'podReadyLatency_P99').
        config_name: Orion config filename (e.g. 'cluster-density.yaml').
        input_vars: JSON string of template variables for the config.

    Returns:
        Dict with config, metric, and per-version data.
        Each version has: values (flat list of floats) and runs (list with timestamp, ocpVersion, buildUrl).
        values[i] corresponds to runs[i].
    """
    if isinstance(versions, str):
        version_list = [v.strip() for v in versions.split(',') if v.strip()]
    else:
        version_list = list(versions)

    if not version_list:
        return {"error": "No valid versions provided"}

    config_value, iv, search_info = await _resolve_config_and_vars(
        ctx, config_name, version_list[0], input_vars,
    )

    output: dict = {
        "config": config_value,
        "metric": metric,
        "versions": {},
    }
    if search_info:
        output["search"] = search_info

    errors = []
    for ver in version_list:
        ver_iv = dict(iv, version=ver) if iv else None
        result = await run_orion(
            config=ORION_CONFIGS_PATH + config_value,
            version=ver,
            lookback=lookback,
            since=since,
            input_vars=ver_iv,
        )

        sum_result = await summarize_result(result, isolate=metric)

        if not isinstance(sum_result, dict) or metric not in sum_result:
            errors.append(f"No data for version {ver}: {sum_result}")
            continue

        raw_values = sum_result[metric].get("value", [])
        if not isinstance(raw_values, list):
            errors.append(f"Unexpected data format for version {ver}")
            continue

        raw_runs = sum_result.get("runs", [])
        values = []
        runs_context = []
        for i, v in enumerate(raw_values):
            if v is None:
                continue
            values.append(v)
            if i < len(raw_runs):
                run = raw_runs[i]
                runs_context.append({
                    "timestamp": run.get("timestamp"),
                    "ocpVersion": run.get("ocpVersion"),
                    "buildUrl": run.get("buildUrl"),
                })

        if not values:
            errors.append(f"All values are None for version {ver}")
            continue

        output["versions"][ver] = {"values": values, "runs": runs_context}

    if errors:
        output["errors"] = errors
    if not output["versions"]:
        output["error"] = "No data found for any version"

    return output


def _add_percentage_changes(pulls_list: list[dict], periodic_avg: dict) -> None:
    """Calculate and set percentage_change on each metric in pull data."""
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


def _flatten_pr_summary(summaries: list[dict]) -> list[dict]:
    results = []
    for summary in summaries:
        config_name = os.path.basename(summary.get("config", "unknown"))

        if "error" in summary:
            results.append({"config": config_name, "error": summary["error"]})
            continue

        periodic_avg = summary.get("periodic_avg", {})
        pulls_list = summary.get("pulls", [])

        # Normalize baselines once
        baselines = {}
        for metric_name, raw in periodic_avg.items():
            val = raw.get("value") if isinstance(raw, dict) else raw
            baselines[metric_name] = round(val, 4) if isinstance(val, float) else val

        # Build per-PR runs
        pr_runs = []
        for pull_obj in pulls_list:
            pr_num = pull_obj.get("pull_number", "")
            for dat in pull_obj.get("data", []):
                run_metrics = []
                for metric_name, metric_data in sorted(dat.get("metrics", {}).items()):
                    pr_val = metric_data.get("value")
                    if isinstance(pr_val, float):
                        pr_val = round(pr_val, 4)
                    pct = metric_data.get("percentage_change")
                    if isinstance(pct, float):
                        pct = round(pct, 2)
                    run_metrics.append({
                        "name": metric_name,
                        "baseline": baselines.get(metric_name),
                        "pr_value": pr_val,
                        "change_pct": pct,
                    })
                pr_runs.append({
                    "pull_number": pr_num,
                    "buildUrl": dat.get("buildUrl"),
                    "timestamp": dat.get("timestamp"),
                    "metrics": run_metrics,
                })

        if not pr_runs:
            results.append({"config": config_name, "error": "No PR test data"})
            continue

        results.append({
            "config": config_name,
            "runs": pr_runs,
        })

    return results


@mcp.tool()
async def openshift_report_on_pr(
    version: Annotated[str, Field(description="OpenShift version to analyze")] = "4.20",
    *,
    lookback: Annotated[str, Field(description="Number of days to lookback")] = "15",
    organization: Annotated[str, Field(description="Organization to look into")] = "openshift",
    repository: Annotated[str, Field(description="Repository to look into")] = "ovn-kubernetes",
    pull_request: Annotated[str, Field(description="PR number to analyze (for single PR)")] = "2841",
    pull_requests: Annotated[str, Field(description="Comma-separated PR numbers to compare (e.g. '3169,3170'). Overrides pull_request if provided.")] = "",
    config_name: ConfigParam = None,
    input_vars: InputVarsParam = "",
    ctx: Context = None,
) -> list[dict]:
    """Analyze how a GitHub pull request affected CI performance by comparing PR test runs against periodic baselines. Use when a user says "analyze PR", "check PR impact", or provides a PR number or GitHub PR URL. Input: PR number + org/repo.

    Triggers: "analyze pr 3332", "analyze pr https://github.com/openshift/ovn-kubernetes/pull/3332",
    "how did PR 3317 affect performance", "check PR impact on 5.0".

    Compares PR CI test runs against periodic baseline averages.

    Args:
        version: OpenShift version to compare against (default: '4.20').
        lookback: Days to look back (default: '15').
        organization: GitHub organization (default: 'openshift').
        repository: GitHub repository (default: 'ovn-kubernetes').
        pull_request: Single PR number (default: '2841').
        pull_requests: Comma-separated PR numbers for multi-PR comparison (default: empty).
        config_name: Orion config filename (e.g. 'cluster-density.yaml').
        input_vars: JSON string of template variables for the config.

    Returns:
        List grouped by config. Each entry has config and runs (list of per-PR-run results).
        Each run has: pull_number, buildUrl, timestamp, and metrics list.
        Each metric has: name, baseline, pr_value, change_pct.
        Error entries have: config, error.
    """
    _extract_and_set_es_server(ctx)
    try:
        iv = _parse_input_vars(input_vars)
    except ValueError:
        iv = None
    configs = _split_configs(config_name)

    if pull_requests and pull_requests.strip():
        pr_list = [pr.strip() for pr in pull_requests.split(",") if pr.strip()]
    else:
        pr_list = [pull_request]

    try:
        pull_numbers = [int(pr) for pr in pr_list]
    except ValueError:
        return [{"config": configs[0], "error": "Pull request numbers must be integers"}]

    pr_iv = dict(iv) if iv else {}
    pr_iv["jobtype"] = "pull"
    pr_iv["organization"] = organization
    pr_iv["repository"] = repository
    pr_iv["pull_number"] = pr_list[0]

    summaries: list[dict] = []
    for config_value in configs:
        full_config_path = os.path.join(ORION_CONFIGS_PATH, config_value)
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
        except json.JSONDecodeError:
            stderr_snippet = (result.stderr or result.stdout or "")[:200].strip()
            summaries.append({"config": full_config_path, "error": f"Orion failed (exit {result.returncode}): {stderr_snippet}"})
            continue

        if not isinstance(data, dict):
            summaries.append({"config": full_config_path, "error": f"Unexpected data type: {type(data).__name__}"})
            continue

        if "periodic_avg" not in data or "pulls" not in data:
            summaries.append({"config": full_config_path, "error": "No PR test data"})
            continue

        periodic_avg = data["periodic_avg"]
        pulls_list = data["pulls"]
        _add_percentage_changes(pulls_list, periodic_avg)

        for pull_obj in pulls_list:
            for dat in pull_obj.get("data", []):
                for key in ("uuid", "is_changepoint", "prs", "ocpVersion"):
                    dat.pop(key, None)
                for metric_data in dat.get("metrics", {}).values():
                    metric_data.pop("labels", None)

        summaries.append({
            "config": full_config_path,
            "periodic_avg": periodic_avg,
            "pulls": pulls_list,
        })

    return _flatten_pr_summary(summaries)


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
            pct = metric_info.get("percentage_change", 0)
            if pct == 0:
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
    """
    Execute Orion across the provided configs and return a formatted summary of
    detected changepoints, or "No changepoints found" if none are detected.
    """
    full_config_paths = [os.path.join(ORION_CONFIGS_PATH, config) for config in configs]
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
        except (json.JSONDecodeError, TypeError):
            stderr_snippet = (result.stderr or result.stdout or "")[:200].strip()
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


@mcp.tool()
async def has_openshift_regressed(
    version: VersionParam = "4.19",
    lookback: LookbackParam = "15",
    config_name: ConfigParam = None,
    input_vars: InputVarsParam = "",
    ctx: Context = None,
) -> str:
    """Check if an OpenShift version has performance regressions using changepoint detection. Use when a user asks "has X regressed", "check for regressions", or "any regressions in version X". Input: version like '4.20' or '5.0'.

    Triggers: "has 4.22 regressed", "check regressions for 5.0",
    "are there regressions in 4.20".

    Runs EDivisive changepoint detection on the provided config(s) for that version.

    Args:
        version: OpenShift version (default: '4.19').
        lookback: Days to look back (default: '15').
        config_name: Orion config filename or comma-separated list (e.g. 'cluster-density.yaml,node-density.yaml').
        input_vars: JSON string of template variables for the config.

    Returns:
        Changepoint details (config, version, PRs, metrics with % change)
        or "No changepoints found".
    """
    _extract_and_set_es_server(ctx)
    try:
        iv = _parse_input_vars(input_vars)
    except ValueError:
        iv = None
    configs = _split_configs(config_name)
    return await _run_regression_checks(configs, version=version, lookback=lookback, input_vars=iv)


@mcp.tool()
async def has_networking_regressed(
    version: VersionParam = "4.19",
    lookback: LookbackParam = "15",
    config_name: ConfigParam = None,
    input_vars: InputVarsParam = "",
    ctx: Context = None,
) -> str:
    """Check if networking benchmarks (node-density-cni, udn-*) have regressed for an OpenShift version. Use when a user specifically asks about networking, CNI, or UDN regressions. Input: version like '4.20'.

    Triggers: "has networking regressed in 4.22", "check networking regressions",
    "any CNI or UDN regressions in 5.0".

    Same as has_openshift_regressed but for networking-related configs.

    Args:
        version: OpenShift version (default: '4.19').
        lookback: Days to look back (default: '15').
        config_name: Orion config filename or comma-separated list (e.g. 'node-density-cni.yaml,small-scale-udn-l3.yaml').
        input_vars: JSON string of template variables for the config.

    Returns:
        Changepoint details or "No changepoints found".
    """
    _extract_and_set_es_server(ctx)
    try:
        iv = _parse_input_vars(input_vars)
    except ValueError:
        iv = None
    configs = _split_configs(config_name)
    return await _run_regression_checks(configs, version=version, lookback=lookback, input_vars=iv)

# Correlation tool

@mcp.tool()
async def metrics_correlation(
    metric1: Annotated[str, Field(description="First metric to analyze")] = "podReadyLatency_P99",
    metric2: Annotated[str, Field(description="Second metric to analyze")] = "ovnCPU_avg",
    *,
    config_name: ConfigParam = None,
    since: Annotated[str, Field(description="Date to begin looking back for performance data")] = None,
    version: VersionParam = "4.19",
    lookback: LookbackParam = "15",
    input_vars: InputVarsParam = "",
    ctx: Context = None,
) -> types.ImageContent | types.TextContent:
    """Check if two metrics are correlated by computing Pearson coefficient and plotting a scatter chart. Use when a user asks "are these metrics related", "correlate X with Y", or "is ovnCPU correlated with podReadyLatency". Input: two metric names.

    Triggers: "correlate podReadyLatency with ovnCPU", "are ovnCPU and etcdCPU related",
    "is there a correlation between X and Y".

    Args:
        metric1: First metric, Y-axis (default: 'podReadyLatency_P99').
        metric2: Second metric, X-axis (default: 'ovnCPU_avg').
        config_name: Orion config filename (e.g. 'cluster-density.yaml').
        since: Start date for lookback (default: None).
        version: OpenShift version (default: '4.19').
        lookback: Days to look back (default: '15').
        input_vars: JSON string of template variables for the config.

    Returns:
        ImageContent (scatter-plot PNG) or TextContent (error).
    """
    config_value, iv, _ = await _resolve_config_and_vars(
        ctx, config_name, version, input_vars,
    )

    result = await run_orion(
        config=ORION_CONFIGS_PATH + config_value,
        version=version,
        lookback=lookback,
        since=since,
        input_vars=iv,
    )

    summary = await summarize_result(result)

    # Ensure we received a valid dict back
    if not isinstance(summary, dict):
        return types.TextContent(type="text", text=f"Error processing Orion output: {summary}")

    # Extract metric values
    try:
        values1 = summary[metric1]["value"]
        values2 = summary[metric2]["value"]
    except KeyError:
        return types.TextContent(
            type="text",
            text="Requested metrics not present in the Orion summary for the chosen configuration.",
        )

    # Compute correlation & generate plot
    corr_b64 = generate_correlation_plot(values1, values2, metric1, metric2, title_prefix=f"{config_value}: ")

    return types.ImageContent(type="image", data=corr_b64.decode("utf-8"), mimeType="image/jpeg")


@mcp.tool()
async def has_nightly_regressed(
    nightly_version: Annotated[str, Field(description="Full nightly version string (e.g., '4.22.0-0.nightly-2026-01-05-203335')")],
    previous_nightly: Annotated[str, Field(description="Optional previous nightly to compare against (e.g., '4.22.0-0.nightly-2026-01-01-123456')")] = "",
    lookback: LookbackParam = "15",
    config_name: ConfigParam = None,
    input_vars: InputVarsParam = "",
    ctx: Context = None,
) -> str:
    """Check if a specific nightly build has regressions by running changepoint detection scoped to that build's time window. Use when a user provides a full nightly version string and asks "inspect this nightly", "has this nightly regressed", or "check nightly X". Input: full nightly string like '5.0.0-0.nightly-2026-08-10-122052'.

    Triggers: "inspect nightly 5.0.0-0.nightly-2026-08-10-122052", "has this nightly regressed",
    "check nightly build", "compare nightly X vs Y".

    Args:
        nightly_version: Full nightly string (required, e.g. '4.22.0-0.nightly-2026-01-05-203335').
        previous_nightly: Earlier nightly to scope the comparison window (default: empty).
        lookback: Days to look back (default: '15').
        config_name: Orion config filename (e.g. 'cluster-density.yaml').
        input_vars: JSON string of template variables for the config.

    Returns:
        Regression details (config, version, PRs, metrics with % change)
        or "No regressions found".
    """
    _extract_and_set_es_server(ctx)

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

    try:
        iv = _parse_input_vars(input_vars)
    except ValueError:
        iv = None
    configs = _split_configs(config_name)

    all_regressions: list[str] = []
    for config_value in configs:
        full_config_path = os.path.join(ORION_CONFIGS_PATH, config_value)

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
        except (json.JSONDecodeError, TypeError):
            stderr_snippet = (result.stderr or result.stdout or "")[:200].strip()
            all_regressions.append(f"❌ Error: Orion failed for {config_value} (exit {result.returncode}): {stderr_snippet}")
            continue

        if not isinstance(data, list):
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


def _timestamp_after(timestamp_val, cutoff_datetime: datetime) -> bool:
    """Check if a timestamp is after (not on or before) the cutoff datetime."""
    entry_dt = parse_timestamp(timestamp_val)
    return entry_dt is not None and entry_dt > cutoff_datetime


async def _summarize_single_config(
    config_value: str, version: str, lookback: int, iv: dict | None,
) -> dict:
    full_path = os.path.join(ORION_CONFIGS_PATH, config_value)

    try:
        metrics_list, meta_map = _load_config_metrics_with_meta(full_path, version, input_vars=iv)
    except Exception as e:
        return {"config": config_value, "success": False, "error": f"Failed to load config metrics: {e}"}

    try:
        result = await run_orion(
            config=full_path,
            version=version,
            lookback=str(lookback),
            input_vars=iv,
        )
        sum_result = await summarize_result(result)
    except Exception as e:
        return {"config": config_value, "success": False, "error": f"Orion failed: {e}"}

    if not isinstance(sum_result, dict):
        return {"config": config_value, "success": False, "error": f"Unexpected Orion output: {sum_result}"}

    prior_sum = {}
    try:
        prior_result = await run_orion(
            config=full_path,
            version=version,
            lookback=str(lookback * 2),
            input_vars=iv,
        )
        prior_sum_raw = await summarize_result(prior_result)
        if isinstance(prior_sum_raw, dict):
            prior_sum = prior_sum_raw
    except Exception:
        pass

    metric_summaries = []
    for m_name in metrics_list:
        if m_name not in sum_result:
            continue
        values = sum_result[m_name].get("value", [])
        values = [v for v in values if v is not None]
        if not values:
            continue

        avg_val = sum(values) / len(values)
        meta = meta_map.get(m_name, {})

        change_pct = None
        two_period_values = prior_sum.get(m_name, {}).get("value", [])
        two_period_values = [v for v in two_period_values if v is not None]
        current_count = len(values)
        if len(two_period_values) > current_count:
            previous_values = two_period_values[:-current_count]
            if previous_values:
                previous_avg = sum(previous_values) / len(previous_values)
                current_avg = sum(values) / len(values)
                if previous_avg != 0:
                    change_pct = round(((current_avg - previous_avg) / abs(previous_avg)) * 100, 2)

        metric_summaries.append({
            "name": m_name,
            "runs": len(values),
            "min": round(min(values), 4),
            "max": round(max(values), 4),
            "avg": round(avg_val, 4),
            "change_percent": change_pct,
            "direction": meta.get("direction"),
            "threshold": meta.get("threshold"),
        })

    return {
        "config": config_value,
        "success": len(metric_summaries) > 0,
        "metrics": metric_summaries,
    }


@mcp.tool()
async def get_performance_summary(
    version: VersionParam = "4.19",
    lookback: Annotated[int, Field(description="Number of days to look back for data")] = 14,
    config_name: ConfigParam = None,
    input_vars: InputVarsParam = "",
    ctx: Context = None,
) -> dict:
    """Health check — aggregated stats (min, max, avg, change%) across ALL metrics for one or more configs.

    Triggers: "how is 4.22 doing overall", "give me a health check for 5.0",
    "is 4.20 healthy", "overall performance report for 4.22".

    Args:
        version: OpenShift version (default: '4.19').
        lookback: Days to look back (default: 14).
        config_name: Orion config filename or comma-separated list (e.g. 'cluster-density.yaml,node-density.yaml').
        input_vars: JSON string of template variables for the config.

    Returns:
        Dict with per-config results, each containing per-metric stats.
    """
    _extract_and_set_es_server(ctx)
    try:
        iv = _parse_input_vars(input_vars)
    except ValueError:
        iv = None
    configs = _split_configs(config_name)
    if not configs:
        config_value, iv, _ = await _resolve_config_and_vars(ctx, config_name, version, input_vars)
        configs = [config_value]

    results = await asyncio.gather(*[
        _summarize_single_config(c, version, lookback, iv) for c in configs
    ])

    return {
        "success": any(r.get("success") for r in results),
        "results": list(results),
    }


def _metric_key(metric: dict) -> str:
    name = metric.get("name", "unknown")
    if "agg" in metric and isinstance(metric["agg"], dict):
        agg_type = metric["agg"].get("agg_type", "")
        if agg_type:
            return f"{name}_{agg_type}"
    metric_of_interest = metric.get("metric_of_interest", "value")
    return f"{name}_{metric_of_interest}"


def _render_config_yaml(config_path: str, version: str = "", input_vars: dict | None = None) -> dict:
    with open(config_path, "r", encoding="utf-8") as template_file:
        template_content = template_file.read()

    env_vars = {k.lower(): v for k, v in os.environ.items()}
    defaults = {
        "version": version,
        "jobtype": "periodic",
        "pull_number": 0,
        "organization": "",
        "repository": "",
    }
    for k, v in defaults.items():
        if v or k not in env_vars:
            env_vars[k] = v

    if input_vars:
        iv = input_vars
        if isinstance(iv, str):
            iv = json.loads(iv)
        env_vars.update({str(k): str(v) for k, v in iv.items()})

    try:
        template = jinja2.Template(template_content, undefined=jinja2.StrictUndefined)
        rendered = template.render(env_vars)
    except jinja2.exceptions.UndefinedError:
        template = jinja2.Template(template_content)
        rendered = template.render(env_vars)

    return yaml.safe_load(rendered)


def _load_config_metrics_with_meta(config_path: str, version: str = "", input_vars: dict | None = None) -> tuple[list[str], dict]:
    rendered_config = _render_config_yaml(config_path, version, input_vars=input_vars)
    metrics_list: list[str] = []
    meta_map: dict = {}

    def _process_metric(metric: dict) -> None:
        if metric.get("type") == "metadata":
            return
        key = _metric_key(metric)
        metrics_list.append(key)
        direction_raw = metric.get("direction")
        threshold_raw = metric.get("threshold")
        try:
            direction_val = (
                int(direction_raw) if direction_raw is not None else None
            )
        except (TypeError, ValueError):
            direction_val = None
        try:
            threshold_val = (
                float(threshold_raw) if threshold_raw is not None else None
            )
        except (TypeError, ValueError):
            threshold_val = None
        meta_map[key] = {
            "direction": direction_val,
            "threshold": threshold_val,
            "metric_of_interest": metric.get("metric_of_interest"),
            "agg_type": metric.get("agg", {}).get("agg_type") if isinstance(metric.get("agg"), dict) else None,
        }

    def _load_metrics_file(mf_name):
        mf_path = os.path.join(os.path.dirname(config_path), mf_name)
        try:
            mf_config = _render_config_yaml(mf_path, version, input_vars=input_vars)
            mf_metrics = mf_config if isinstance(mf_config, list) else mf_config.get("metrics", [])
            for metric in mf_metrics:
                if isinstance(metric, dict):
                    _process_metric(metric)
        except Exception:
            pass

    top_metrics_file = rendered_config.get("metricsFile")
    if top_metrics_file:
        _load_metrics_file(top_metrics_file)

    for test in rendered_config.get("tests", []):
        test_metrics_file = test.get("metricsFile")
        if test_metrics_file:
            _load_metrics_file(test_metrics_file)

        for metric in test.get("metrics", []):
            _process_metric(metric)

    return metrics_list, meta_map


if __name__ == "__main__":
    if os.getenv("ES_SERVER") is None:
        print("ES_SERVER environment variable is not set")
        import sys
        sys.exit(1)
    TRANSPORT = os.getenv("MCP_TRANSPORT", "streamable-http")
    asyncio.run(mcp.run(transport=TRANSPORT))

