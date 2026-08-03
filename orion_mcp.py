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
import httpx
import jinja2
import yaml

from mcp import types
from mcp.server.fastmcp import Context, FastMCP

# Import utility functions from utils module
from utils.utils import (
    run_orion,
    summarize_result,
    get_data_source,
    get_es_metadata_index,
    orion_metrics,
    orion_configs,
    generate_correlation_plot,
    generate_multi_line_plot,
    list_orion_configs,
    parse_nightly_version,
    parse_timestamp,
    filter_data_by_timestamp,
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
_configs=list_orion_configs()
if _configs == []:
    ORION_CONFIGS = [
    "metal-perfscale-cpt-virt-udn-density.yaml",
    "trt-external-payload-cluster-density.yaml",
    "trt-external-payload-node-density.yaml",
    "trt-external-payload-node-density-cni.yaml",
    "trt-external-payload-crd-scale.yaml",
    "small-scale-udn-l3.yaml",
    "med-scale-udn-l3.yaml",]
else:
    ORION_CONFIGS = _configs

FULL_ORION_CONFIG_PATHS = [os.path.join(ORION_CONFIGS_PATH, config) for config in ORION_CONFIGS]

logger = logging.getLogger(__name__)


def _build_benchmark_config_map() -> dict[str, list[dict]]:
    """Scan all config YAMLs and build a map from benchmark.keyword to config files."""
    config_map: dict[str, list[dict]] = {}
    config_dir = ORION_CONFIGS_PATH
    try:
        files = os.listdir(config_dir)
    except FileNotFoundError:
        files = ORION_CONFIGS

    for filename in files:
        if not filename.endswith((".yaml", ".yml")):
            continue
        if filename.startswith("trt-external"):
            continue
        filepath = os.path.join(config_dir, filename)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                raw = f.read()
            safe_content = re.sub(r'"?\{\{[^}]*\}\}[^"]*"?', "PLACEHOLDER", raw)
            parsed = yaml.safe_load(safe_content)
            if not isinstance(parsed, dict):
                continue
            for test in parsed.get("tests", []):
                metadata = test.get("metadata", {})
                benchmark = metadata.get("benchmark.keyword")
                test_name = test.get("name", "")
                if benchmark:
                    config_map.setdefault(benchmark, []).append({
                        "file": filename,
                        "test_name": test_name,
                    })
        except Exception:
            continue
    return config_map


_BENCHMARK_CONFIG_MAP = _build_benchmark_config_map()

_CONFIG_TO_BENCHMARK: dict[str, str] = {}
for _bm, _entries in _BENCHMARK_CONFIG_MAP.items():
    for _entry in _entries:
        _CONFIG_TO_BENCHMARK[_entry["file"]] = _bm


_STREAM_PREFIXES = [
    ("rosa-hcp", "small-rosa-hcp-"),
    ("rosa", "small-rosa-"),
    ("okd", "okd-"),
    ("metal", "metal-"),
    ("stackrox", "stackrox-"),
    ("readout", "readout-"),
    ("payload-scale", "payload-scale"),
    ("small-scale", "small-scale-"),
]

_ALL_STREAM_FILE_PREFIXES = tuple(p for _, p in _STREAM_PREFIXES)


def _select_config(benchmark: str, upstream_job: str = "") -> str | None:
    """Pick the best config file for a given benchmark keyword.

    Strategy:
    1. Direct test_name match in upstream_job (exact substring)
    2. Stream-tag match: find a stream keyword in the job name, pick
       the config whose filename starts with that stream's prefix
    3. Generic fallback: pick the config whose filename has no stream prefix
    """
    candidates = _BENCHMARK_CONFIG_MAP.get(benchmark, [])
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]["file"]

    if upstream_job:
        for c in candidates:
            if c["test_name"] and c["test_name"] in upstream_job:
                return c["file"]

        job_lower = upstream_job.lower().replace("_", "-")
        for stream_tag, file_prefix in _STREAM_PREFIXES:
            if stream_tag in job_lower:
                for c in candidates:
                    if c["file"].startswith(file_prefix):
                        return c["file"]
                break

    for c in candidates:
        if not c["file"].startswith(_ALL_STREAM_FILE_PREFIXES):
            return c["file"]
    return candidates[0]["file"]


def _parse_input_vars(input_vars: str) -> dict | None:
    """Parse a JSON input_vars string into a dict, or return None if empty."""
    return json.loads(input_vars) if input_vars else None


def _build_input_vars(metadata: dict, version: str) -> dict:
    """Build an input_vars dict from ES metadata fields."""
    iv = {
        "version": version,
        "jobtype": "periodic",
        "pull_number": 0,
        "organization": "",
        "repository": "",
    }
    for field in (
        "platform", "clusterType", "masterNodesType", "masterNodesCount",
        "workerNodesType", "workerNodesCount", "networkType",
        "fips", "ipsec", "encrypted",
    ):
        if field in metadata:
            val = metadata[field]
            if isinstance(val, bool):
                val = str(val).lower()
            iv[field] = val
    return iv


async def _discover_configs_with_vars(
    version: str,
    lookback: int = 30,
    *,
    platform: str = "",
    workload: str = "",
    scale: str = "",
    fips: bool = False,
    ipsec: bool = False,
    encrypted: bool = False,
    benchmark_filter=None,
    job_filters: list[str] | None = None,
    job_size: int = 20,
    es_benchmark: str = "",
) -> tuple[list[dict], list[str], list[dict]]:
    """Discover jobs from ES and return (config, input_vars) pairs.

    Returns:
        (configs_with_vars, filters_used, raw_jobs) where configs_with_vars is
        [{"config": str, "input_vars": dict, "job": str, "benchmark": str}, ...]
    """
    exclude = []
    if job_filters is None:
        job_filters, exclude = _build_job_filters(
            platform=platform, workload=workload, scale=scale,
            fips=fips, ipsec=ipsec, encrypted=encrypted,
        )
    jobs = await _discover_from_es(version, lookback=lookback, job_filters=job_filters, job_size=job_size, es_benchmark=es_benchmark)
    if exclude:
        jobs = [j for j in jobs if not any(kw in j["upstreamJob"].lower() for kw in exclude)]
    configs_with_vars: list[dict] = []
    seen: set[tuple] = set()
    for job in jobs:
        iv = _build_input_vars(job["metadata"], version)
        iv_key = tuple(sorted(iv.items()))
        for bm in job["benchmarks"]:
            cfg = bm.get("config")
            if not cfg:
                continue
            dedup_key = (cfg, iv_key)
            if dedup_key in seen:
                continue
            if benchmark_filter is not None and not benchmark_filter(bm):
                continue
            seen.add(dedup_key)
            configs_with_vars.append({
                "config": cfg,
                "input_vars": iv,
                "job": job["upstreamJob"],
                "benchmark": bm.get("name", ""),
            })
    return configs_with_vars, job_filters, jobs


def _build_job_filters(
    platform: str = "",
    workload: str = "",
    scale: str = "",
    fips: bool = False,
    ipsec: bool = False,
    encrypted: bool = False,
) -> tuple[list[str], list[str]]:
    """Convert structured NLP-friendly params into ES wildcard filters + exclusion list.

    Returns (filters, exclude_keywords):
      - filters: wildcard clauses for ES bool.must
      - exclude_keywords: substrings to reject from job names post-query
        (modifiers not explicitly requested are excluded)
    """
    filters = ["periodic-*"]
    has_cp_filter = any([scale, fips, ipsec, encrypted])
    if workload:
        filters.append(f"*{workload}*")
    elif has_cp_filter:
        filters.append("*control-plane*")
    elif not platform:
        filters.append("*payload-control-plane*")
    if platform:
        filters.append(f"*{platform.replace('-', '_')}*")
    else:
        filters.append("*aws*")
    if scale:
        filters.append(f"*{scale}nodes*")
    if fips:
        filters.append("*fips*")
    if ipsec:
        filters.append("*ipsec*")
    if encrypted:
        filters.append("*etcdencrypt*")

    exclude = []
    if not fips:
        exclude.append("fips")
    if not ipsec:
        exclude.append("ipsec")
    if not encrypted:
        exclude.append("etcdencrypt")
    return filters, exclude


def _build_search_block(
    version: str,
    filters: list[str],
    lookback_days: int,
    jobs: list[dict],
    *,
    config: str = "",
    benchmark: str = "",
    message: str = "",
) -> dict:
    """Build a standardized search block for tool responses."""
    block = {
        "version": version,
        "filters": filters,
        "lookback_days": lookback_days,
        "jobs_found": len(jobs),
    }
    if config:
        block["config"] = config
    if benchmark:
        block["benchmark"] = benchmark
    if message:
        block["message"] = message
    return block


async def _discover_input_vars_for_config(
    config_file: str,
    version: str,
) -> tuple[dict | None, dict]:
    """Discover input_vars for a single config via standard job discovery.

    Uses the normal discovery path with ES-level benchmark filtering
    to find which job ran this config's benchmark and extract its metadata.
    """
    benchmark = _CONFIG_TO_BENCHMARK.get(config_file, "")
    if not benchmark:
        return None, {"message": f"No benchmark mapping found for config '{config_file}'"}

    discovered, filters_used, jobs = await _discover_configs_with_vars(
        version, es_benchmark=benchmark,
    )
    for entry in discovered:
        if entry["config"] == config_file:
            return entry["input_vars"], _build_search_block(version, filters_used, 30, jobs)

    return None, _build_search_block(
        version, filters_used, 30, jobs,
        config=config_file, benchmark=benchmark,
        message=f"No jobs found for config '{config_file}'",
    )


async def _discover_from_es(
    version: str,
    lookback: int = 30,
    job_pattern: str = "*payload-control-plane*",
    job_filters: list[str] | None = None,
    job_size: int = 20,
    es_benchmark: str = "",
) -> list[dict]:
    """Query perf_scale_ci ES index to discover jobs, benchmarks, and metadata.

    Args:
        version: OCP version prefix (e.g. "4.19").
        lookback: Days to look back.
        job_pattern: Single wildcard for upstreamJob (backward compat).
        job_filters: List of independent wildcards — each becomes a bool.must
            clause so they match regardless of position in the job name.
            When provided, takes precedence over job_pattern.

    Returns a list of dicts, each with:
      - upstreamJob: full prow job name
      - benchmarks: list of {name, config} where config is the matched YAML file
      - metadata: dict of cluster metadata (platform, workerNodesCount, etc.)
      - run_count: number of matching documents
    """
    es_url = get_data_source()
    index = get_es_metadata_index()

    must_clauses = [
        {"wildcard": {"ocpVersion": {"value": f"{version}*"}}},
        {"range": {"timestamp": {"gte": f"now-{lookback}d"}}},
    ]
    if job_filters:
        for filt in job_filters:
            must_clauses.append({"wildcard": {"upstreamJob.keyword": {"value": filt}}})
    else:
        must_clauses.append({"wildcard": {"upstreamJob.keyword": job_pattern}})
    if es_benchmark:
        must_clauses.append({"wildcard": {"benchmark.keyword": {"value": f"*{es_benchmark}*"}}})

    query = {
        "size": 0,
        "query": {
            "bool": {
                "must": must_clauses,
            }
        },
        "aggs": {
            "jobs": {
                "terms": {"field": "upstreamJob.keyword", "size": job_size},
                "aggs": {
                    "benchmarks": {
                        "terms": {"field": "benchmark.keyword", "size": 20},
                        "aggs": {
                            "latest": {
                                "top_hits": {
                                    "size": 1,
                                    "sort": [{"timestamp": {"order": "desc"}}],
                                    "_source": [
                                        "platform", "clusterType",
                                        "masterNodesType", "masterNodesCount",
                                        "workerNodesType", "workerNodesCount",
                                        "networkType", "fips", "ipsec", "encrypted",
                                    ],
                                }
                            }
                        },
                    }
                },
            }
        },
    }

    try:
        async with httpx.AsyncClient(timeout=30.0, verify=False) as client:
            resp = await client.post(f"{es_url}/{index}/_search", json=query)
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        logger.warning("ES discovery query failed: %s", e)
        return []

    total_hits = data.get("hits", {}).get("total", {}).get("value", 0)
    job_buckets = data.get("aggregations", {}).get("jobs", {}).get("buckets", [])
    logger.info("ES discovery: %d hits, %d job buckets for version=%s pattern=%s",
                total_hits, len(job_buckets), version, job_pattern)

    results = []
    for job_bucket in job_buckets:
        upstream_job = job_bucket["key"]
        benchmarks = []
        metadata = {}

        for bm_bucket in job_bucket.get("benchmarks", {}).get("buckets", []):
            bm_name = bm_bucket["key"]
            config_file = _select_config(bm_name, upstream_job)
            benchmarks.append({"name": bm_name, "config": config_file})

            if not metadata:
                hits = bm_bucket.get("latest", {}).get("hits", {}).get("hits", [])
                if hits:
                    metadata = hits[0].get("_source", {})

        if benchmarks:
            results.append({
                "upstreamJob": upstream_job,
                "benchmarks": benchmarks,
                "metadata": metadata,
                "run_count": job_bucket.get("doc_count", 0),
            })

    logger.info("ES discovery: returning %d jobs for version=%s",
                len(results), version)
    return results


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
    """
    Get the release date for a given OpenShift version.

    Args:
        version: OpenShift version to get the release date for.
        Defaults to 4.20.

    Returns:
        The release date for the given OpenShift version.
        If the version is not a valid OpenShift version, returns "Invalid version: {version}".
    """
    if version in RELEASE_DATES :
        return RELEASE_DATES[version]
    return f"Invalid version: {version}"

@mcp.tool()
def get_orion_configs() -> list[str]:
    """
    Return the list of Orion config filenames (not full paths).
    """
    return orion_configs(ORION_CONFIGS)


@mcp.tool()
async def discover_jobs(
    version: Annotated[str, Field(description="OpenShift version prefix (e.g. '4.18', '4.19', '4.20', '4.21', '4.22', '5.0')")] = "4.19",
    lookback: Annotated[int, Field(description="Number of days to look back for job runs")] = 30,
    platform: Annotated[str, Field(
        description=(
            "Cloud platform filter. Values: 'aws' (default when omitted), 'rosa-hcp' (ROSA HCP managed), "
            "'rosa' (ROSA classic managed), 'gcp', 'azure', 'metal' (bare-metal). "
            "When platform is specified without workload, returns ALL jobs on that platform. "
            "When omitted, defaults to AWS payload-control-plane jobs only."
        ),
    )] = "",
    workload: Annotated[str, Field(
        description=(
            "Workload/test-name filter — matched as substring in CI job name. Common values: "
            "'payload-control-plane' (nightly 6-node payload jobs, the default), "
            "'control-plane' (all control-plane jobs including payload, 24/120/252 nodes), "
            "'data-path' (network throughput/latency, 9 nodes), "
            "'node-density-heavy' (24 nodes), "
            "'netpol' (network policy, 24 nodes), "
            "'udn-density-l3' (UDN L3, 24 nodes), "
            "'udn-bgp' (UDN BGP, 24 nodes), "
            "'olmv1' (OLM benchmark), "
            "'loaded-upgrade' (upgrade testing, 24 nodes). "
            "Note: 'small-scale' typically means 24-node control-plane jobs (use scale='24')."
        ),
    )] = "",
    scale: Annotated[str, Field(
        description=(
            "Cluster node count filter. Values: '3' (upgrade/UDN), '6' (payload), '9' (data-path), "
            "'24' (standard control-plane / small-scale), '120' (medium-scale), "
            "'249' (ROSA large-scale), '252' (AWS large-scale). Maps to '*{N}nodes*' in job name."
        ),
    )] = "",
    fips: Annotated[bool, Field(description="Filter for FIPS-enabled jobs (adds '*fips*' to job name filter).")] = False,
    ipsec: Annotated[bool, Field(description="Filter for IPSec-enabled jobs (adds '*ipsec*' to job name filter).")] = False,
    encrypted: Annotated[bool, Field(description="Filter for etcd-encrypted jobs (adds '*etcdencrypt*' to job name filter).")] = False,
    ctx: Context = None,
) -> dict:
    """Discover CI jobs, benchmarks, and cluster metadata from Elasticsearch.

    Each param becomes an independent wildcard on the CI job name — order doesn't matter.
    Defaults: periodic AWS payload-control-plane jobs (nightly 6-node).
    When platform is given without workload, shows all jobs on that platform.
    Modifiers (fips/ipsec/scale/encrypted) auto-scope to control-plane jobs.

    Returns dict with 'success', 'search' (filters used), and 'jobs' list with benchmarks and metadata.
    """
    _extract_and_set_es_server(ctx)

    filters_used, exclude = _build_job_filters(
        platform=platform, workload=workload, scale=scale,
        fips=fips, ipsec=ipsec, encrypted=encrypted,
    )
    jobs = await _discover_from_es(version, lookback, job_filters=filters_used)
    if exclude:
        jobs = [j for j in jobs if not any(kw in j["upstreamJob"].lower() for kw in exclude)]

    search = _build_search_block(version, filters_used, lookback, jobs)
    if not jobs:
        search["message"] = "No jobs matched the search filters"
        return {"success": False, "search": search, "jobs": []}

    return {
        "success": True,
        "search": search,
        "jobs": jobs,
    }


@mcp.tool()
async def get_orion_metrics(
    config_name: Annotated[
        str | None,
        Field(
            description="Orion configuration file name (e.g. 'small-scale-udn-l3.yaml')"
        ),
    ] = None,
    version: Annotated[str, Field(description="OpenShift version used to query metrics")] = "4.20",
    input_vars: Annotated[str, Field(description="JSON string of template variables for the config (e.g. platform, workerNodesCount)")] = "",
    ctx: Context = None,
) -> dict:
    """Return the list of metrics available for a specific Orion *config*.

    Args:
        config_name: **Filename** of the Orion configuration to query (not the full path).
        version: OpenShift version used to query metrics.
        input_vars: JSON string of template variables for config rendering.
        ctx: MCP context for accessing request headers

    Returns:
        A dictionary where the key is the *config* (full path) and the value is a
        list of metric names available for that configuration.
    """
    # Extract and set ES_SERVER from request headers if present
    _extract_and_set_es_server(ctx)

    default_config = "small-scale-udn-l3.yaml"
    effective_config = config_name or default_config

    iv = _parse_input_vars(input_vars)
    search_info = {}
    if iv is None:
        iv, search_info = await _discover_input_vars_for_config(effective_config, version)
    result = await orion_metrics([ORION_CONFIGS_PATH + effective_config], version=version)

    if isinstance(result, str):
        return {"error": f"Failed to fetch Orion metrics: {result}"}

    if search_info:
        result["search"] = search_info
    return result


@mcp.tool()
async def get_orion_metrics_with_meta(
    config_name: Annotated[
        str | None,
        Field(
            description="Orion configuration file name (e.g. 'small-scale-udn-l3.yaml')"
        ),
    ] = None,
    version: Annotated[str, Field(description="OpenShift version used to render the config template")] = "4.19",
    input_vars: Annotated[str, Field(description="JSON string of template variables for the config (e.g. platform, workerNodesCount)")] = "",
    ctx: Context = None,
) -> dict:
    """Return metrics and metadata for a specific Orion *config*.

    Args:
        config_name: **Filename** of the Orion configuration to query (not the full path).
        version: OpenShift version used to render the config template.
        input_vars: JSON string of template variables for config rendering.
        ctx: MCP context for accessing request headers

    Returns:
        A dictionary with "metrics" (list) and "meta" (per-metric metadata).
    """
    # Extract and set ES_SERVER from request headers if present
    _extract_and_set_es_server(ctx)

    default_config = "small-scale-udn-l3.yaml"
    effective_config = config_name or default_config
    iv = _parse_input_vars(input_vars)
    search_info = {}
    if iv is None:
        iv, search_info = await _discover_input_vars_for_config(effective_config, version)
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
            [ORION_CONFIGS_PATH + effective_config], version=version
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
    lookback: Annotated[str, Field(description="Number of days to lookback")] = "15",
    since: Annotated[str, Field(description="Date to begin lookback")] = None,
    *,
    metric: Annotated[str, Field(description="Metric to analyze")] = "podReadyLatency_P99",
    config_name: Annotated[
        str | None,
        Field(description="Orion configuration file name (e.g. 'small-scale-udn-l3.yaml')"),
    ] = None,
    input_vars: Annotated[str, Field(description="JSON string of template variables for the config (e.g. platform, workerNodesCount)")] = "",
    options: Annotated[str, Field(description="Options in format 'output_format' or 'output_format:display_field'. Examples: 'image', 'json', 'both', 'json:ocpVirtVersion'")] = "image",
    ctx: Context = None,
) -> types.ImageContent | types.TextContent:
    """
    Captures a performance analysis against the specified OpenShift version using Orion.

    Orion uses an EDivisive algorithm to analyze performance data from a specified
    configuration file to detect any performance regressions.

    Args:
        versions: Comma-separated list of OpenShift versions to analyze.
        lookback: The number of days to look back for performance data. Defaults to 15 days.
        since: The date to begin looking back for performance data. Defaults to None.
        metric: The metric to analyze. Defaults to podReadyLatency_P99.
        config_name: The config to analyze. Defaults to small-scale-udn-l3.yaml.
        input_vars: JSON string of template variables for config rendering.
        options: Output format and optional display field. Format: 'output_format' or
                'output_format:display_field'. Examples: 'image', 'json:ocpVirtVersion'.

    Returns:
        Returns an image showing the performance overtime, or JSON data based on options.
    """
    # Extract and set ES_SERVER from request headers if present
    _extract_and_set_es_server(ctx)

    iv = _parse_input_vars(input_vars)

    # Parse options to extract output_format and display
    if ":" in options:
        output_format, display = options.split(":", 1)
    else:
        output_format = options
        display = ""

    # Parse versions into list
    if isinstance(versions, str):
        version_list = [v.strip() for v in versions.split(',') if v.strip()]
    else:
        version_list = list(versions)

    series: dict[str, list[float]] = {}
    full_data: dict[str, dict] = {}

    default_config = "small-scale-udn-l3.yaml"
    config_value = config_name or default_config

    if iv is None:
        iv, _ = await _discover_input_vars_for_config(config_value, version_list[0])

    errors = []
    for ver in version_list:
        result = await run_orion(
            config=ORION_CONFIGS_PATH + config_value,
            version=ver,
            lookback=lookback,
            since=since,
            input_vars=iv,
            display=display if display.strip() else None,
        )

        sum_result = await summarize_result(result, isolate=metric)

        # Ensure we have the expected structure before indexing
        if not isinstance(sum_result, dict) or metric not in sum_result:
            errors.append(f"No data for version {ver}: {sum_result}")
            continue

        raw_values = sum_result[metric].get("value", [])  # type: ignore[assignment]
        if not isinstance(raw_values, list):
            errors.append(f"Unexpected data format for version {ver}")
            continue

        # Remove None values to keep the plot continuous
        values = [v for v in raw_values if v is not None]
        if not values:
            errors.append(f"All values are None for version {ver}")
            continue

        series[ver] = values
        full_data[ver] = sum_result

    if errors and not series:
        return types.TextContent(type="text", text="\n".join(errors))

    # Handle different output formats
    if output_format.lower() == "json":
        # Return JSON data
        json_output = {
            "config": config_value,
            "metric": metric,
            "lookback": lookback,
            "display": display if display.strip() else None,
            "data": full_data
        }
        return types.TextContent(type="text", text=json.dumps(json_output, indent=2))

    if output_format.lower() == "both":
        # Return both JSON and image info
        json_output = {
            "config": config_value,
            "metric": metric,
            "lookback": lookback,
            "display": display if display.strip() else None,
            "data": full_data,
            "plot_info": "Image data follows JSON data"
        }
        try:
            img_b64 = generate_multi_line_plot(series, metric, title_prefix=f"{config_value}: ")
            combined_output = json.dumps(json_output, indent=2) + "\n\n[IMAGE_DATA_BASE64]\n" + img_b64.decode("utf-8")
            return types.TextContent(type="text", text=combined_output)
        except ValueError as e:
            return types.TextContent(type="text", text=f"Error generating plot: {str(e)}\n\nJSON data:\n{json.dumps(json_output, indent=2)}")

    else:
        # Default: return image
        try:
            img_b64 = generate_multi_line_plot(series, metric, title_prefix=f"{config_value}: ")
            return types.ImageContent(type="image", data=img_b64.decode("utf-8"), mimeType="image/jpeg")
        except ValueError as e:
            return types.TextContent(type="text", text=str(e))


@mcp.tool()
async def get_orion_performance_data(
    config_name: Annotated[
        str | None,
        Field(
            description="Orion configuration file name (e.g. 'small-scale-udn-l3.yaml')"
        ),
    ] = None,
    *,
    metric: Annotated[str, Field(description="Metric to analyze")] = "podReadyLatency_P99",
    version: Annotated[str, Field(description="OpenShift version to analyze")] = "4.19",
    lookback: Annotated[str, Field(description="Number of days to lookback")] = "15",
    since: Annotated[str | None, Field(description="Date to begin looking back for performance data")] = None,
    input_vars: Annotated[str, Field(description="JSON string of template variables for the config (e.g. platform, workerNodesCount)")] = "",
    ctx: Context = None,
) -> dict:
    """Return performance data values for a specific config/metric/version.

    Returns:
        Dict with config, metric, version, lookback, values, count.
    """
    # Extract and set ES_SERVER from request headers if present
    _extract_and_set_es_server(ctx)

    iv = _parse_input_vars(input_vars)
    default_config = "small-scale-udn-l3.yaml"
    config_value = config_name or default_config

    search_info = {}
    if iv is None:
        iv, search_info = await _discover_input_vars_for_config(config_value, version)

    try:
        result = await run_orion(
            config=ORION_CONFIGS_PATH + config_value,
            version=version,
            lookback=lookback,
            since=since,
            input_vars=iv,
        )
        sum_result = await summarize_result(result, isolate=metric)

        if not isinstance(sum_result, dict) or metric not in sum_result:
            resp = {"error": f"No data found for metric {metric}"}
            if search_info:
                resp["search"] = search_info
            return resp

        metric_data = sum_result[metric]
        values = metric_data.get("value", [])
        if not isinstance(values, list):
            return {"error": f"Unexpected data format for metric {metric}"}

        values = [v for v in values if v is not None]
        resp = {
            "config": config_value,
            "metric": metric,
            "version": version,
            "lookback": lookback,
            "values": values,
            "count": len(values),
        }
        if search_info:
            resp["search"] = search_info
        return resp
    except Exception as e:
        return {"error": str(e)}

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


async def get_pr_details(
    organization: str,
    repository: str,
    pull_requests: list[str],
    version: str = "4.20",
    lookback: str = "15",
) -> list[dict]:
    """
    Get PR performance analysis details by running Orion with input variables.

    Dynamically discovers configs and cluster metadata from Elasticsearch,
    then runs Orion with PR-specific input_vars for each benchmark.

    Args:
        organization: GitHub organization name
        repository: Repository name
        pull_requests: List of pull request numbers to analyze
        version: OpenShift version to analyze
        lookback: Days to look back for data

    Returns:
        List of dictionaries containing PR analysis results for each config.
    """
    if not pull_requests:
        raise ValueError("At least one pull request number is required")
    try:
        pull_numbers = [int(pr) for pr in pull_requests]
    except ValueError as exc:
        raise ValueError("Pull request numbers must be integers") from exc

    pr_filters = [f"pull-ci-{organization}-{repository}-*"]
    discovered, _, _ = await _discover_configs_with_vars(
        version, job_filters=pr_filters, job_size=10,
    )
    if not discovered:
        discovered, _, _ = await _discover_configs_with_vars(version)

    for entry in discovered:
        entry["input_vars"]["jobtype"] = "pull"
        entry["input_vars"]["organization"] = organization
        entry["input_vars"]["repository"] = repository
        entry["input_vars"]["pull_number"] = pull_requests[0]

    if not discovered:
        return []

    summaries: list[dict] = []

    for entry in discovered:
        config = entry["config"]
        input_vars = entry["input_vars"]
        full_config_path = os.path.join(ORION_CONFIGS_PATH, config)
        result = await run_orion(
            config=full_config_path,
            version=version,
            lookback=lookback,
            input_vars=input_vars,
            pr_analysis=True,
            pull_numbers=pull_numbers,
        )

        try:
            data = json.loads(result.stdout)
        except json.JSONDecodeError as e:
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

        for pull_obj in pulls_list:
            for dat in pull_obj.get("data", []):
                for key in ("uuid", "is_changepoint", "prs", "ocpVersion"):
                    dat.pop(key, None)
                for metric_data in dat.get("metrics", {}).values():
                    metric_data.pop("labels", None)

        summaries.append({
            "config": config,
            "periodic_avg": periodic_avg,
            "pulls": pulls_list,
        })

    return summaries

@mcp.tool()
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
    """
    Captures a performance analysis against the specified OpenShift version using Orion.

    Args:
        version: OpenShift version to analyze.
        lookback: The number of days to look back for performance data. Defaults to 15 days.
        organization: The organization to look into. Defaults to openshift.
        repository: The repository to look into. Defaults to ovn-kubernetes.
        pull_request: Single PR number to analyze. Defaults to 2841.
        pull_requests: Comma-separated PR numbers for multi-PR comparison (e.g. '3169,3170').
            When provided, overrides pull_request.
        ctx: MCP context for accessing request headers

    Returns:
        Dictionary with summaries containing PR analysis results for each config.
    """
    # Extract and set ES_SERVER from request headers if present
    _extract_and_set_es_server(ctx)

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
    return {
        "summaries": summaries
    }


def _extract_regression_details(stdout: str) -> list[dict]:
    """Extract changepoint details from Orion JSON output."""
    data = json.loads(stdout)
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

        if result.returncode not in (0, 3):
            config_short = os.path.basename(full_config_path)
            details = _extract_regression_details(result.stdout)
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


async def _discover_and_check_regressions(
    version: str,
    lookback: str,
    configs: str = "",
    benchmark_filter=None,
    *,
    platform: str = "",
    workload: str = "",
    scale: str = "",
    fips: bool = False,
    ipsec: bool = False,
    encrypted: bool = False,
) -> str:
    """Discover jobs from ES and run regression checks on their configs.

    Args:
        configs: Comma-separated list of config files. If provided, skips
            discovery and uses these directly.
        benchmark_filter: Optional callable(bm_dict) -> bool to filter benchmarks.
            If None, all benchmarks with a config are included.
        platform/workload/scale/fips/ipsec/encrypted: Structured filter params.
    """
    if configs.strip():
        config_list = [c.strip() for c in configs.split(",") if c.strip()]
        result = await _run_regression_checks(config_list, version=version, lookback=lookback)
        return result

    discovered, filters_used, jobs = await _discover_configs_with_vars(
        version, platform=platform, workload=workload, scale=scale,
        fips=fips, ipsec=ipsec, encrypted=encrypted,
        benchmark_filter=benchmark_filter,
    )

    search_header = (
        f"[Search: version={version}, filters={', '.join(filters_used)}, "
        f"jobs_found={len(jobs)}]\n\n"
    )

    if not discovered:
        return search_header + f"No jobs found matching filters for version {version}"

    all_results = []
    for entry in discovered:
        result = await _run_regression_checks(
            [entry["config"]], version=version, lookback=lookback,
            input_vars=entry["input_vars"],
        )
        if result != "No changepoints found":
            all_results.append(result)

    body = "\n\n".join(all_results) if all_results else "No changepoints found"
    return search_header + body


@mcp.tool()
async def has_openshift_regressed(
    version: Annotated[str, Field(description="Version of OpenShift to look into")] = "4.19",
    lookback: Annotated[str, Field(description="Number of days to lookback")] = "15",
    configs: Annotated[str, Field(description="Comma-separated list of config files to check (optional, auto-discovered if empty)")] = "",
    platform: Annotated[str, Field(description="Cloud platform filter (e.g. 'aws', 'gcp', 'metal'). Defaults to 'aws'.")] = "",
    workload: Annotated[str, Field(description="Workload type filter (e.g. 'cluster-density', 'netpol', 'cudn-density').")] = "",
    scale: Annotated[str, Field(description="Cluster scale filter (e.g. '24', '120', '252').")] = "",
    fips: Annotated[bool, Field(description="Filter for FIPS-enabled jobs.")] = False,
    ipsec: Annotated[bool, Field(description="Filter for IPSec-enabled jobs.")] = False,
    encrypted: Annotated[bool, Field(description="Filter for etcd-encrypted jobs.")] = False,
    ctx: Context = None,
) -> str:
    """Runs performance regression analysis against OpenShift using Orion.

    Discovers CI jobs from Elasticsearch using structured filters, then runs
    EDivisive changepoint detection on each discovered config.

    Defaults when nothing specified: periodic AWS payload-control-plane jobs.

    Returns string with search context and regression results.
    """
    _extract_and_set_es_server(ctx)
    return await _discover_and_check_regressions(
        version, lookback, configs=configs,
        platform=platform, workload=workload, scale=scale,
        fips=fips, ipsec=ipsec, encrypted=encrypted,
    )


@mcp.tool()
async def has_networking_regressed(
    version: Annotated[str, Field(description="Version of OpenShift to look into")] = "4.19",
    lookback: Annotated[str, Field(description="Number of days to lookback")] = "15",
    configs: Annotated[str, Field(description="Comma-separated list of config files to check (optional, auto-discovered if empty)")] = "",
    platform: Annotated[str, Field(description="Cloud platform filter (e.g. 'aws', 'gcp', 'metal'). Defaults to 'aws'.")] = "",
    workload: Annotated[str, Field(description="Workload type filter (e.g. 'cluster-density', 'netpol').")] = "",
    scale: Annotated[str, Field(description="Cluster scale filter (e.g. '24', '120', '252').")] = "",
    fips: Annotated[bool, Field(description="Filter for FIPS-enabled jobs.")] = False,
    ipsec: Annotated[bool, Field(description="Filter for IPSec-enabled jobs.")] = False,
    encrypted: Annotated[bool, Field(description="Filter for etcd-encrypted jobs.")] = False,
    ctx: Context = None,
) -> str:
    """Runs regression analysis on networking-focused benchmarks.

    Discovers CI jobs from Elasticsearch, then filters to networking-related
    benchmarks (node-density-cni, udn-*).

    Returns string with search context and regression results.
    """
    _extract_and_set_es_server(ctx)

    networking_benchmarks = {"node-density-cni", "udn-density-pods"}

    def _is_networking(bm: dict) -> bool:
        return bm["name"] in networking_benchmarks or bm["name"].startswith("udn")

    return await _discover_and_check_regressions(
        version, lookback, configs=configs, benchmark_filter=_is_networking,
        platform=platform, workload=workload, scale=scale,
        fips=fips, ipsec=ipsec, encrypted=encrypted,
    )

# Correlation tool

@mcp.tool()
async def metrics_correlation(
    metric1: Annotated[str, Field(description="First metric to analyze")] = "podReadyLatency_P99",
    metric2: Annotated[str, Field(description="Second metric to analyze")] = "ovnCPU_avg",
    *,
    config_name: Annotated[
        str | None,
        Field(
            description="Orion configuration file name (e.g. 'trt-external-payload-cluster-density.yaml')"
        ),
    ] = None,
    since: Annotated[str, Field(description="Date to begin looking back for performance data")] = None,
    version: Annotated[str, Field(description="Version of OpenShift to look into")] = "4.19",
    lookback: Annotated[str, Field(description="Number of days to lookback")] = "15",
    input_vars: Annotated[str, Field(description="JSON string of template variables for the config (e.g. platform, workerNodesCount)")] = "",
    ctx: Context = None,
) -> types.ImageContent | types.TextContent:
    """
    Calculate and visualise the correlation between two metrics for a given
    Orion configuration.

    A scatter-plot annotated with the Pearson correlation coefficient is
    returned. If either metric is missing from the Orion results the function
    falls back to returning a textual error message.
    """
    # Extract and set ES_SERVER from request headers if present
    _extract_and_set_es_server(ctx)

    iv = _parse_input_vars(input_vars)
    default_config = "trt-external-payload-cluster-density.yaml"
    config_value = config_name or default_config

    if iv is None:
        iv, _ = await _discover_input_vars_for_config(config_value, version)

    # Run Orion to gather data
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
    lookback: Annotated[str, Field(description="Number of days to lookback")] = "30",
    configs: Annotated[str, Field(description="Comma-separated list of config files (optional, auto-discovered if empty)")] = "",
    platform: Annotated[str, Field(description="Cloud platform filter (e.g. 'aws', 'gcp', 'metal'). Defaults to 'aws'.")] = "",
    workload: Annotated[str, Field(description="Workload type filter (e.g. 'cluster-density', 'netpol').")] = "",
    scale: Annotated[str, Field(description="Cluster scale filter (e.g. '24', '120', '252').")] = "",
    fips: Annotated[bool, Field(description="Filter for FIPS-enabled jobs.")] = False,
    ipsec: Annotated[bool, Field(description="Filter for IPSec-enabled jobs.")] = False,
    encrypted: Annotated[bool, Field(description="Filter for etcd-encrypted jobs.")] = False,
    ctx: Context = None,
) -> str:
    """Detect regressions for a specific OpenShift nightly version.

    Discovers CI jobs from Elasticsearch using structured filters, then runs
    changepoint detection filtered to the nightly date.

    Defaults when nothing specified: periodic AWS payload-control-plane jobs.

    Returns string with search context and regression details.
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

    requested_configs = set()
    if configs.strip():
        requested_configs = {c.strip() for c in configs.split(",") if c.strip()}

    discovered, filters_used, jobs = await _discover_configs_with_vars(
        nightly_info.major_version, platform=platform, workload=workload,
        scale=scale, fips=fips, ipsec=ipsec, encrypted=encrypted,
    )
    if not discovered:
        search_header = (
            f"[Search: version={nightly_info.major_version}, "
            f"filters={', '.join(filters_used)}, jobs_found=0]\n\n"
        )
        return search_header + f"No jobs found matching filters for version {nightly_info.major_version}"

    if requested_configs:
        available_configs = {d["config"] for d in discovered}
        discovered = [d for d in discovered if d["config"] in requested_configs]
        if not discovered:
            return (
                f"No ES jobs matched the requested configs: {', '.join(requested_configs)}. "
                f"Available configs from discovery: {', '.join(available_configs)}"
            )

    search_header = ""
    if filters_used:
        search_header = (
            f"[Search: version={nightly_info.major_version}, "
            f"filters={', '.join(filters_used)}, configs_found={len(discovered)}]\n\n"
        )

    all_regressions: list[str] = []

    for entry in discovered:
        config = entry["config"]
        full_config_path = os.path.join(ORION_CONFIGS_PATH, config)
        result = await run_orion(
            config=full_config_path,
            version=nightly_info.major_version,
            lookback=lookback,
            input_vars=entry["input_vars"],
            jira_ack=True,
            jira_status_filter="Done",
        )

        try:
            data = json.loads(result.stdout)
            if not isinstance(data, list):
                print(f"[nightly] {config}: stdout not a list, skipping")
                continue
            print(f"[nightly] {config}: {len(data)} runs, changepoints={sum(1 for d in data if d.get('is_changepoint'))}")
            data = filter_data_by_timestamp(data, nightly_info.nightly_date)
            print(f"[nightly] {config}: {len(data)} runs after timestamp filter")
            if prev_nightly_info:
                data = [e for e in data if e.get("timestamp") and _timestamp_after(e["timestamp"], prev_nightly_info.nightly_date)]
        except (json.JSONDecodeError, TypeError) as exc:
            print(f"[nightly] {config}: parse error: {exc}, stdout[:200]={result.stdout[:200]}")
            continue

        details = _extract_regression_details(json.dumps(data))
        print(f"[nightly] {config}: {len(details)} regressions extracted")
        for det in details:
            lines = [
                f"⚠️ Regression in {nightly_info.full_version}",
                f"Config: {config}",
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

    body = "\n\n".join(all_regressions) if all_regressions else "No regressions found"
    return search_header + body


def _timestamp_after(timestamp_val, cutoff_datetime: datetime) -> bool:
    """Check if a timestamp is after (not on or before) the cutoff datetime."""
    entry_dt = parse_timestamp(timestamp_val)
    return entry_dt is not None and entry_dt > cutoff_datetime


@mcp.tool()
async def get_performance_summary(
    version: Annotated[str, Field(description="OpenShift version to analyze (e.g. '4.18', '4.19', '4.20', '4.21', '4.22', '5.0')")] = "4.19",
    lookback: Annotated[int, Field(description="Number of days to look back for data")] = 14,
    platform: Annotated[str, Field(
        description=(
            "Cloud platform filter. Values: 'aws' (default when omitted), 'rosa-hcp' (ROSA HCP managed), "
            "'rosa' (ROSA classic managed), 'gcp', 'azure', 'metal' (bare-metal). "
            "When platform is specified, returns jobs for that platform (no payload restriction). "
            "When omitted, defaults to AWS payload-control-plane jobs."
        ),
    )] = "",
    workload: Annotated[str, Field(
        description=(
            "Workload/test-name filter. Common values: "
            "'payload-control-plane' (nightly 6-node, the default), "
            "'control-plane' (all control-plane jobs at any scale), "
            "'data-path', 'node-density-heavy', 'netpol', 'udn-density-l3'. "
            "Note: 'small-scale' = 24-node control-plane (use scale='24')."
        ),
    )] = "",
    scale: Annotated[str, Field(
        description=(
            "Cluster node count filter. '3' (upgrade/UDN), '6' (payload), '9' (data-path), "
            "'24' (small/standard control-plane), '120' (medium-scale), "
            "'249' (ROSA large-scale), '252' (AWS large-scale)."
        ),
    )] = "",
    fips: Annotated[bool, Field(description="Filter for FIPS-enabled jobs.")] = False,
    ipsec: Annotated[bool, Field(description="Filter for IPSec-enabled jobs.")] = False,
    encrypted: Annotated[bool, Field(description="Filter for etcd-encrypted jobs.")] = False,
    config_name: Annotated[str | None, Field(
        description=(
            "Specific config file name to analyze (e.g. 'cluster-density.yaml', "
            "'small-rosa-hcp-cluster-density.yaml', 'node-density.yaml'). "
            "If given, only this config is analyzed. The correct config variant is auto-selected "
            "based on job type when not specified."
        ),
    )] = None,
    ctx: Context = None,
) -> dict:
    """Get a complete performance summary in one call.

    Discovers jobs, runs Orion analysis for each config, and computes
    per-metric statistics (min, max, avg, change%).

    Defaults: periodic AWS payload-control-plane jobs (nightly 6-node).
    When platform is given, analyzes all jobs on that platform.
    Modifiers (fips/ipsec/scale/encrypted) auto-scope to control-plane jobs.
    """
    _extract_and_set_es_server(ctx)

    es_benchmark = ""
    if config_name:
        es_benchmark = _CONFIG_TO_BENCHMARK.get(config_name, "")
        if not es_benchmark:
            return {"success": False, "search": {"message": f"No benchmark mapping found for config '{config_name}'"}, "configs": []}

    configs_to_run, filters_used, jobs_for_search = await _discover_configs_with_vars(
        version, platform=platform, workload=workload, scale=scale,
        fips=fips, ipsec=ipsec, encrypted=encrypted,
        es_benchmark=es_benchmark,
    )

    if config_name:
        configs_to_run = [c for c in configs_to_run if c["config"] == config_name]

    if not configs_to_run:
        search = _build_search_block(version, filters_used, 30, [],
            message=f"No jobs found for config '{config_name}'" if config_name else "No jobs found matching filters")
        return {"success": False, "search": search, "configs": []}

    search = _build_search_block(
        version, filters_used, lookback, jobs_for_search,
    )

    config_results = []
    for entry in configs_to_run:
        cfg = entry["config"]
        iv = entry["input_vars"]
        full_path = os.path.join(ORION_CONFIGS_PATH, cfg)

        try:
            metrics_list, meta_map = _load_config_metrics_with_meta(full_path, version, input_vars=iv)
        except Exception:
            continue

        try:
            result = await run_orion(
                config=full_path,
                version=version,
                lookback=str(lookback),
                input_vars=iv,
            )
            sum_result = await summarize_result(result)
        except Exception:
            continue

        if not isinstance(sum_result, dict):
            continue

        # Second Orion call with 2× lookback for prior-period comparison
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

        if metric_summaries:
            cfg_result = {
                "config": cfg,
                "benchmark": entry.get("benchmark", ""),
                "metrics": metric_summaries,
            }
            if entry.get("job"):
                cfg_result["job"] = entry["job"]
            config_results.append(cfg_result)

    return {
        "success": len(config_results) > 0,
        "search": search,
        "configs": config_results,
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
    env_vars.update(defaults)

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

    metrics_file = rendered_config.get("metricsFile")
    if metrics_file:
        metrics_file_path = os.path.join(os.path.dirname(config_path), metrics_file)
        try:
            mf_config = _render_config_yaml(metrics_file_path, version, input_vars=input_vars)
            mf_metrics = mf_config if isinstance(mf_config, list) else mf_config.get("metrics", [])
            for metric in mf_metrics:
                if isinstance(metric, dict):
                    _process_metric(metric)
        except Exception:
            pass

    for test in rendered_config.get("tests", []):
        metrics_file = test.get("metricsFile")
        if metrics_file:
            metrics_file_path = os.path.join(os.path.dirname(config_path), metrics_file)
            try:
                mf_config = _render_config_yaml(metrics_file_path, version, input_vars=input_vars)
                mf_metrics = mf_config if isinstance(mf_config, list) else mf_config.get("metrics", [])
                for metric in mf_metrics:
                    if isinstance(metric, dict):
                        _process_metric(metric)
            except Exception:
                pass

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

