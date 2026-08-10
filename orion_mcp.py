"""
Model Context Protocol (MCP) server for Orion performance regression analysis.

This module provides tools for running performance regression analysis using
the cloud-bulldozer/orion library.
"""

import asyncio
import json
import logging
import os
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

ORION_CONFIGS_PATH = "/orion/examples/"
ORION_CONFIGS = list_orion_configs()

FULL_ORION_CONFIG_PATHS = [os.path.join(ORION_CONFIGS_PATH, config) for config in ORION_CONFIGS]

logger = logging.getLogger(__name__)


class _SafePlaceholder(jinja2.Undefined):
    """Returns safe placeholder values so config YAML can be parsed without real variables."""
    def __str__(self):
        return "PLACEHOLDER"
    def __iter__(self):
        return iter([])
    def __bool__(self):
        return False
    def __int__(self):
        return 0


_JINJA_ENV = jinja2.Environment(undefined=_SafePlaceholder)


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
            rendered = _JINJA_ENV.from_string(raw).render()
            parsed = yaml.safe_load(rendered)
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

_CONFIG_TO_BENCHMARKS: dict[str, list[str]] = {}
for _bm, _entries in _BENCHMARK_CONFIG_MAP.items():
    for _entry in _entries:
        _CONFIG_TO_BENCHMARKS.setdefault(_entry["file"], [])
        if _bm not in _CONFIG_TO_BENCHMARKS[_entry["file"]]:
            _CONFIG_TO_BENCHMARKS[_entry["file"]].append(_bm)


_STREAM_PREFIXES = [
    ("rosa-hcp", "small-rosa-hcp-"),
    ("rosa", "small-rosa-"),
    ("okd", "okd-"),
    ("metal", "metal-"),
    ("stackrox", "stackrox-"),
    ("readout", "readout-"),
    ("payload-scale", "payload-scale"),
    ("small-scale", "small-scale-"),
    ("service-mesh", "servicemesh-"),
    ("netobserv", "netobserv-"),
    ("ols", "ols-"),
]

_ALL_STREAM_FILE_PREFIXES = tuple(p for _, p in _STREAM_PREFIXES)

_JOB_NAME_TO_CONFIG = {
    "cudn-density-single-ns-250": "small-scale-cudn-density-single-ns-250.yaml",
    "cudn-density-single-ns-500": "small-scale-cudn-density-single-ns-500.yaml",
    "cudn-density-multi-ns": "small-scale-cudn-density-l2-multi-ns.yaml",
    "cudn-churn-250": "small-scale-cudn-churn-250.yaml",
    "cudn-pod-churn-250": "small-scale-cudn-pod-churn-250.yaml",
    "cudn-incremental-700": "small-scale-cudn-incremental-700.yaml",
    "cudn-incremental-1000": "small-scale-cudn-incremental-700.yaml",
}


def _select_config(benchmark: str, upstream_job: str = "") -> str | None:
    """Pick the best config file for a given benchmark keyword.

    Strategy:
    0. Explicit job-name-to-config match (for cases like CUDN where
       the job name has no stream tag but maps to a specific config)
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
        job_lower = upstream_job.lower().replace("_", "-")

        for job_substr, config_file in _JOB_NAME_TO_CONFIG.items():
            if job_substr in job_lower:
                for c in candidates:
                    if c["file"] == config_file:
                        return c["file"]
                break

        for c in candidates:
            if c["test_name"] and c["test_name"] in upstream_job:
                return c["file"]

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


DEFAULT_CONFIG = "cluster-density.yaml"


async def _resolve_config_and_vars(
    ctx,
    config_name: str | None,
    version: str,
    input_vars: str = "",
) -> tuple[str, dict | None, dict]:
    """Common setup for tools: extract ES config, resolve config name, discover input_vars.

    Returns (config_value, iv, search_info).
    """
    _extract_and_set_es_server(ctx)
    config_value = config_name or DEFAULT_CONFIG
    iv = _parse_input_vars(input_vars)
    search_info = {}
    if iv is None:
        iv, search_info = await _discover_input_vars_for_config(config_value, version)
    return config_value, iv, search_info


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
    job_type: str = "periodic",
    organization: str = "",
    repository: str = "",
    benchmark_filter=None,
    job_size: int = 20,
    es_benchmarks: list[str] | None = None,
    known_config: str = "",
) -> tuple[list[dict], dict, list[dict]]:
    """Discover jobs from ES and return (config, input_vars) pairs.

    Converts user-facing params (platform name, bool flags) into ES indexed
    field values and delegates to ``_discover_from_es``.

    Defaults when nothing specific is requested: periodic AWS 6-node jobs
    with fips/ipsec/encrypted=false (equivalent to payload-control-plane).

    Returns:
        (configs_with_vars, filters_used, raw_jobs) where configs_with_vars is
        [{"config": str, "input_vars": dict, "job": str, "benchmark": str}, ...]
        and filters_used is a dict of the active ES field filters.
    """
    es_platform, es_cluster_type = _resolve_platform(platform)
    es_worker_count = int(scale) if scale else 0

    has_modifier = any([workload, scale, fips, ipsec, encrypted, platform, organization])
    if not has_modifier and not es_benchmarks:
        workload = "payload"
        es_worker_count = 6
    elif any([fips, ipsec, encrypted]) and not workload:
        workload = "control-plane"

    es_fips = "true" if fips else "false"
    es_ipsec = "true" if ipsec else "false"
    es_encrypted = "true" if encrypted else "false"

    filters_used: dict = {
        "version": version,
        "jobType": job_type,
        "platform": es_platform,
        "fips": es_fips,
        "ipsec": es_ipsec,
        "encrypted": es_encrypted,
    }
    if es_cluster_type:
        filters_used["clusterType"] = es_cluster_type
    if es_worker_count:
        filters_used["workerNodesCount"] = es_worker_count
    if workload:
        filters_used["workload"] = workload
    if organization:
        filters_used["organization"] = organization
    if repository:
        filters_used["repository"] = repository

    jobs = await _discover_from_es(
        version, lookback,
        job_type=job_type, platform=es_platform, cluster_type=es_cluster_type,
        worker_count=es_worker_count, fips=es_fips, ipsec=es_ipsec,
        encrypted=es_encrypted, organization=organization, repository=repository,
        workload=workload, benchmarks=es_benchmarks, known_config=known_config,
        job_size=job_size,
    )

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
    return configs_with_vars, filters_used, jobs


_PLATFORM_MAP = {
    "aws": ("AWS", ""),
    "gcp": ("GCP", ""),
    "azure": ("Azure", ""),
    "metal": ("BareMetal", ""),
    "baremetal": ("BareMetal", ""),
    "rosa-hcp": ("AWS", "rosa-hcp"),
    "rosa": ("AWS", "rosa"),
}


def _resolve_platform(platform: str) -> tuple[str, str]:
    """Return (es_platform, es_cluster_type) for a user-facing platform name."""
    if not platform:
        return "AWS", ""
    return _PLATFORM_MAP.get(platform.lower(), (platform.upper(), ""))


def _build_search_block(
    version: str,
    filters: dict,
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
    """Discover input_vars for a single config via ES job discovery.

    Passes known_config to skip the redundant _select_config lookup
    (we already know the config, no need to map benchmark→config again).
    """
    benchmarks = _CONFIG_TO_BENCHMARKS.get(config_file, [])
    if not benchmarks:
        return None, {"message": f"No benchmark mapping found for config '{config_file}'"}

    discovered, filters_used, jobs = await _discover_configs_with_vars(
        version, es_benchmarks=benchmarks, known_config=config_file,
    )
    for entry in discovered:
        if entry["config"] == config_file:
            return entry["input_vars"], _build_search_block(version, filters_used, 30, jobs)

    return None, _build_search_block(
        version, filters_used, 30, jobs,
        config=config_file, benchmark=", ".join(benchmarks),
        message=f"No jobs found for config '{config_file}'",
    )


async def _discover_from_es(
    version: str,
    lookback: int = 30,
    *,
    job_type: str = "",
    platform: str = "",
    cluster_type: str = "",
    worker_count: int = 0,
    fips: str = "",
    ipsec: str = "",
    encrypted: str = "",
    organization: str = "",
    repository: str = "",
    workload: str = "",
    benchmarks: list[str] | None = None,
    known_config: str = "",
    job_size: int = 20,
) -> list[dict]:
    """Query perf_scale_ci ES index to discover jobs, benchmarks, and metadata.

    Uses indexed field filters (jobType, platform, fips, etc.) instead of
    wildcard-matching on job names. The only remaining job name wildcard is
    ``workload``, which has no indexed equivalent.

    Args:
        version: OCP version prefix (e.g. "4.19").
        lookback: Days to look back.
        job_type: Filter on jobType field ("periodic", "pull", "rehearse").
        platform: Filter on platform field ("AWS", "GCP", "BareMetal").
        cluster_type: Filter on clusterType field ("rosa-hcp", "self-managed").
        worker_count: Filter on workerNodesCount (6, 24, 120, 252). 0 = no filter.
        fips/ipsec/encrypted: Filter on respective fields ("true"/"false").
        organization/repository: Filter on org/repo fields (for PR queries).
        workload: Job name wildcard (e.g. "data-path", "netpol").
        benchmarks: Filter on benchmark.keyword (exact match, any in list).
        known_config: Skip _select_config and use this config directly.
        job_size: Max number of job buckets in aggregation.

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
    if job_type:
        must_clauses.append({"term": {"jobType.keyword": job_type}})
    if platform:
        must_clauses.append({"term": {"platform.keyword": platform}})
    if cluster_type:
        must_clauses.append({"term": {"clusterType.keyword": cluster_type}})
    if worker_count > 0:
        must_clauses.append({"term": {"workerNodesCount": worker_count}})
    if fips:
        must_clauses.append({"term": {"fips.keyword": fips}})
    if ipsec:
        must_clauses.append({"term": {"ipsec.keyword": ipsec}})
    if encrypted:
        must_clauses.append({"term": {"encrypted.keyword": encrypted}})
    if organization:
        must_clauses.append({"term": {"organization.keyword": organization}})
    if repository:
        must_clauses.append({"term": {"repository.keyword": repository}})
    if workload:
        must_clauses.append({"wildcard": {"upstreamJob.keyword": {"value": f"*{workload}*"}}})
    if benchmarks:
        must_clauses.append({"terms": {"benchmark.keyword": benchmarks}})

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
    logger.info("ES discovery: %d hits, %d job buckets for version=%s",
                total_hits, len(job_buckets), version)

    results = []
    for job_bucket in job_buckets:
        upstream_job = job_bucket["key"]
        benchmarks = []
        metadata = {}

        for bm_bucket in job_bucket.get("benchmarks", {}).get("buckets", []):
            bm_name = bm_bucket["key"]
            cfg = known_config if known_config else _select_config(bm_name, upstream_job)
            benchmarks.append({"name": bm_name, "config": cfg})

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


@mcp.tool()
async def discover_jobs(
    version: Annotated[str, Field(description="OpenShift version prefix (e.g. '4.18', '4.19', '4.20', '4.21', '4.22', '5.0')")] = "4.19",
    lookback: Annotated[int, Field(description="Number of days to look back for job runs")] = 30,
    platform: Annotated[str, Field(
        description=(
            "Cloud platform filter (filters on ES 'platform' and 'clusterType' fields). "
            "Values: 'aws' (default), 'rosa-hcp' (ROSA HCP managed), "
            "'rosa' (ROSA classic), 'gcp', 'azure', 'metal' (bare-metal). "
            "When specified without workload, returns ALL jobs on that platform. "
            "When omitted, defaults to AWS 6-node payload jobs."
        ),
    )] = "",
    workload: Annotated[str, Field(
        description=(
            "Workload filter — matched as substring in CI job name (only filter not using an indexed field). "
            "Common values: 'control-plane', 'data-path', 'node-density-heavy', "
            "'netpol', 'udn-density-l3', 'udn-bgp', 'olmv1', 'loaded-upgrade'. "
            "When omitted and no other modifier is set, defaults to 6-node payload jobs."
        ),
    )] = "",
    scale: Annotated[str, Field(
        description=(
            "Cluster worker node count (filters on ES 'workerNodesCount' field). "
            "Values: '3', '6' (payload), '9' (data-path), '24', '120', '249', '252'."
        ),
    )] = "",
    fips: Annotated[bool, Field(description="Filter for FIPS-enabled jobs (filters on ES 'fips' field).")] = False,
    ipsec: Annotated[bool, Field(description="Filter for IPSec-enabled jobs (filters on ES 'ipsec' field).")] = False,
    encrypted: Annotated[bool, Field(description="Filter for etcd-encrypted jobs (filters on ES 'encrypted' field).")] = False,
    ctx: Context = None,
) -> dict:
    """Find what CI jobs are running in Elasticsearch for a given version, platform, or workload. Use when a user asks "what jobs run for 4.20", "show CI jobs on rosa-hcp", or "what benchmarks are running".

    Triggers: "what jobs run on 4.22", "list jobs for rosa-hcp", "what payload jobs exist",
    "show CI jobs for fips", "what benchmarks run on metal".

    Args:
        version: OpenShift version prefix (default: '4.19').
        lookback: Days to look back (default: 30).
        platform: Cloud platform — 'aws', 'rosa-hcp', 'gcp', 'metal' (default: 'aws' 6-node payload).
        workload: Substring matched against CI job name (default: empty = 6-node payload jobs).
        scale: Worker node count — '3', '6', '9', '24', '120', '252' (default: empty).
        fips/ipsec/encrypted: Boolean filters (default: false).

    Returns:
        Dict with 'success', 'search' (filters, job count), and 'jobs' (list with benchmark, config, cluster metadata).
    """
    _extract_and_set_es_server(ctx)

    discovered, filters_used, jobs = await _discover_configs_with_vars(
        version, lookback,
        platform=platform, workload=workload, scale=scale,
        fips=fips, ipsec=ipsec, encrypted=encrypted,
    )

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
            description="Orion configuration file name (e.g. 'cluster-density.yaml')"
        ),
    ] = None,
    version: Annotated[str, Field(description="OpenShift version used to query metrics")] = "4.20",
    input_vars: Annotated[str, Field(description="JSON string of template variables for the config (e.g. platform, workerNodesCount)")] = "",
    ctx: Context = None,
) -> dict:
    """List what metrics a benchmark tracks. Use when a user asks "what metrics does cluster-density have" or "list metrics for node-density".

    Triggers: "what metrics does cluster-density track", "list metrics for node-density",
    "what can I measure for this config".

    Args:
        config_name: Orion config filename (default: 'cluster-density.yaml').
        version: OpenShift version (default: '4.20').
        input_vars: JSON template variables (default: empty, auto-discovered from ES).

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
    config_name: Annotated[
        str | None,
        Field(
            description="Orion configuration file name (e.g. 'cluster-density.yaml')"
        ),
    ] = None,
    version: Annotated[str, Field(description="OpenShift version used to render the config template")] = "4.19",
    input_vars: Annotated[str, Field(description="JSON string of template variables for the config (e.g. platform, workerNodesCount)")] = "",
    ctx: Context = None,
) -> dict:
    """Get metric details including thresholds, directions (higher-is-better or lower-is-better), and labels for a benchmark. Use when a user asks "what are the thresholds", "which direction is good for ovnCPU", or "show metric metadata".

    Triggers: "what are the metric thresholds for cluster-density", "which metrics are higher-is-better",
    "show metric details".

    Args:
        config_name: Orion config filename (default: 'cluster-density.yaml').
        version: OpenShift version (default: '4.19').
        input_vars: JSON template variables (default: empty, auto-discovered from ES).

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
    lookback: Annotated[str, Field(description="Number of days to lookback")] = "15",
    since: Annotated[str, Field(description="Date to begin lookback")] = None,
    *,
    metric: Annotated[str, Field(description="Metric to analyze")] = "podReadyLatency_P99",
    config_name: Annotated[
        str | None,
        Field(description="Orion configuration file name (e.g. 'cluster-density.yaml')"),
    ] = None,
    input_vars: Annotated[str, Field(description="JSON string of template variables for the config (e.g. platform, workerNodesCount)")] = "",
    ctx: Context = None,
) -> dict:
    """Compare or fetch a specific metric across OpenShift versions in a single call. Pass metric name and comma-separated versions (e.g. versions='4.22,5.0', metric='podReadyLatency_P99'). Config is auto-discovered — no need to look up configs or metrics first.

    Triggers: "compare podReadyLatency_P99 for 4.22 vs 5.0", "show ovnCPU_avg values for 4.20",
    "get podReadyLatency data", "etcdCPU numbers for 4.22 and 5.0".

    Args:
        versions: Comma-separated versions (default: '4.19'). Example: '4.22,5.0' for multi-version comparison.
        lookback: Days to look back (default: '15').
        since: Start date for lookback (default: None).
        metric: Metric to return (default: 'podReadyLatency_P99').
        config_name: Orion config filename (default: 'cluster-density.yaml', auto-discovered from ES if omitted).
        input_vars: JSON template variables (default: empty, auto-discovered from ES).

    Returns:
        Dict with config, metric, and per-version data.
        Each version has: values (flat list of floats) and runs (list with timestamp, ocpVersion, buildUrl).
        values[i] corresponds to runs[i].
    """
    _extract_and_set_es_server(ctx)

    if isinstance(versions, str):
        version_list = [v.strip() for v in versions.split(',') if v.strip()]
    else:
        version_list = list(versions)

    config_value, iv, search_info = await _resolve_config_and_vars(ctx, config_name, version_list[0], input_vars)

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

        values = [v for v in raw_values if v is not None]
        if not values:
            errors.append(f"All values are None for version {ver}")
            continue

        runs_context = []
        for run in sum_result.get("runs", []):
            runs_context.append({
                "timestamp": run.get("timestamp"),
                "ocpVersion": run.get("ocpVersion"),
                "buildUrl": run.get("buildUrl"),
            })

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

    discovered, _, _ = await _discover_configs_with_vars(
        version,
        job_type="pull",
        organization=organization,
        repository=repository,
        job_size=10,
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
            stderr_snippet = (result.stderr or result.stdout or "")[:200].strip()
            print(f"Failed to parse orion output for {full_config_path}: {e}")
            summaries.append({"config": full_config_path, "error": f"Orion failed (exit {result.returncode}): {stderr_snippet}"})
            continue

        if not isinstance(data, dict):
            print(f"Unexpected data type from orion for {full_config_path}: {type(data)}")
            summaries.append({"config": full_config_path, "error": f"Unexpected data type: {type(data).__name__}"})
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
            "config": full_config_path,
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

    Returns:
        List grouped by config. Each entry has config and runs (list of per-PR-run results).
        Each run has: pull_number, buildUrl, timestamp, and metrics list.
        Each metric has: name, baseline, pr_value, change_pct.
        Error entries have: config, error.
    """
    _extract_and_set_es_server(ctx)

    if pull_requests and pull_requests.strip():
        pr_list = [pr.strip() for pr in pull_requests.split(",") if pr.strip()]
    else:
        pr_list = [pull_request]

    summaries = await get_pr_details(organization, repository, pr_list, version, lookback)
    return _flatten_pr_summary(summaries)


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

    filter_str = ", ".join(f"{k}={v}" for k, v in filters_used.items())
    search_header = f"[Search: {filter_str}, jobs_found={len(jobs)}]\n\n"

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
    platform: Annotated[str, Field(description="Cloud platform (ES 'platform'/'clusterType'). Values: 'aws', 'rosa-hcp', 'gcp', 'metal'. Defaults to 'aws'.")] = "",
    workload: Annotated[str, Field(description="Workload filter — matched as substring in job name. E.g. 'data-path', 'netpol', 'cudn-density'.")] = "",
    scale: Annotated[str, Field(description="Worker node count (ES 'workerNodesCount'). Values: '6', '24', '120', '252'.")] = "",
    fips: Annotated[bool, Field(description="Filter on ES 'fips' field.")] = False,
    ipsec: Annotated[bool, Field(description="Filter on ES 'ipsec' field.")] = False,
    encrypted: Annotated[bool, Field(description="Filter on ES 'encrypted' field.")] = False,
    ctx: Context = None,
) -> str:
    """Check if an OpenShift version has performance regressions using changepoint detection across all benchmarks. Use when a user asks "has X regressed", "check for regressions", or "any regressions in version X". Input: version like '4.20' or '5.0'.

    Triggers: "has 4.22 regressed", "check regressions for 5.0", "any regressions on rosa-hcp",
    "are there regressions in 4.20".

    Runs EDivisive changepoint detection on all discovered configs for that version.

    Args:
        version: OpenShift version (default: '4.19').
        lookback: Days to look back (default: '15').
        configs: Comma-separated config files (default: empty, auto-discovered from ES).
        platform: Cloud platform — 'aws', 'rosa-hcp', 'gcp', 'metal' (default: 'aws' 6-node payload).
        workload: Substring matched against CI job name (default: empty).
        scale: Worker node count (default: empty).
        fips/ipsec/encrypted: Boolean filters (default: false).

    Returns:
        Search filters header + changepoint details (config, version, PRs, metrics with % change)
        or "No changepoints found".
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
    platform: Annotated[str, Field(description="Cloud platform (ES 'platform'/'clusterType'). Values: 'aws', 'rosa-hcp', 'gcp', 'metal'. Defaults to 'aws'.")] = "",
    workload: Annotated[str, Field(description="Workload filter — matched as substring in job name. E.g. 'data-path', 'netpol'.")] = "",
    scale: Annotated[str, Field(description="Worker node count (ES 'workerNodesCount'). Values: '6', '24', '120', '252'.")] = "",
    fips: Annotated[bool, Field(description="Filter on ES 'fips' field.")] = False,
    ipsec: Annotated[bool, Field(description="Filter on ES 'ipsec' field.")] = False,
    encrypted: Annotated[bool, Field(description="Filter on ES 'encrypted' field.")] = False,
    ctx: Context = None,
) -> str:
    """Check if networking benchmarks (node-density-cni, udn-*) have regressed for an OpenShift version. Use when a user specifically asks about networking, CNI, or UDN regressions. Input: version like '4.20'.

    Triggers: "has networking regressed in 4.22", "check networking regressions",
    "any CNI or UDN regressions in 5.0".

    Same as has_openshift_regressed but filtered to networking-related benchmarks only.

    Args:
        version: OpenShift version (default: '4.19').
        lookback: Days to look back (default: '15').
        configs: Comma-separated config files (default: empty, auto-discovered from ES).
        platform: Cloud platform — 'aws', 'rosa-hcp', 'gcp', 'metal' (default: 'aws').
        workload: Substring matched against CI job name (default: empty).
        scale: Worker node count (default: empty).
        fips/ipsec/encrypted: Boolean filters (default: false).

    Returns:
        Search filters header + changepoint details or "No changepoints found".
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
            description="Orion configuration file name (e.g. 'cluster-density.yaml')"
        ),
    ] = None,
    since: Annotated[str, Field(description="Date to begin looking back for performance data")] = None,
    version: Annotated[str, Field(description="Version of OpenShift to look into")] = "4.19",
    lookback: Annotated[str, Field(description="Number of days to lookback")] = "15",
    input_vars: Annotated[str, Field(description="JSON string of template variables for the config (e.g. platform, workerNodesCount)")] = "",
    ctx: Context = None,
) -> types.ImageContent | types.TextContent:
    """Check if two metrics are correlated by computing Pearson coefficient and plotting a scatter chart. Use when a user asks "are these metrics related", "correlate X with Y", or "is ovnCPU correlated with podReadyLatency". Input: two metric names.

    Triggers: "correlate podReadyLatency with ovnCPU", "are ovnCPU and etcdCPU related",
    "is there a correlation between X and Y".

    Args:
        metric1: First metric, Y-axis (default: 'podReadyLatency_P99').
        metric2: Second metric, X-axis (default: 'ovnCPU_avg').
        config_name: Orion config filename (default: 'cluster-density.yaml', auto-discovered from ES if omitted).
        since: Start date for lookback (default: None).
        version: OpenShift version (default: '4.19').
        lookback: Days to look back (default: '15').
        input_vars: JSON template variables (default: empty, auto-discovered from ES).

    Returns:
        ImageContent (scatter-plot PNG) or TextContent (error).
    """
    config_value, iv, _ = await _resolve_config_and_vars(ctx, config_name, version, input_vars)

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
    lookback: Annotated[str, Field(description="Number of days to lookback")] = "15",
    configs: Annotated[str, Field(description="Comma-separated list of config files (optional, auto-discovered if empty)")] = "",
    platform: Annotated[str, Field(description="Cloud platform (ES 'platform'/'clusterType'). Values: 'aws', 'rosa-hcp', 'gcp', 'metal'. Defaults to 'aws'.")] = "",
    workload: Annotated[str, Field(description="Workload filter — matched as substring in job name. E.g. 'data-path', 'netpol'.")] = "",
    scale: Annotated[str, Field(description="Worker node count (ES 'workerNodesCount'). Values: '6', '24', '120', '252'.")] = "",
    fips: Annotated[bool, Field(description="Filter on ES 'fips' field.")] = False,
    ipsec: Annotated[bool, Field(description="Filter on ES 'ipsec' field.")] = False,
    encrypted: Annotated[bool, Field(description="Filter on ES 'encrypted' field.")] = False,
    ctx: Context = None,
) -> str:
    """Check if a specific nightly build has regressions by running changepoint detection scoped to that build's time window. Use when a user provides a full nightly version string and asks "inspect this nightly", "has this nightly regressed", or "check nightly X". Input: full nightly string like '5.0.0-0.nightly-2026-08-10-122052'.

    Triggers: "inspect nightly 5.0.0-0.nightly-2026-08-10-122052", "has this nightly regressed",
    "check nightly build", "compare nightly X vs Y".

    Args:
        nightly_version: Full nightly string (required, e.g. '4.22.0-0.nightly-2026-01-05-203335').
        previous_nightly: Earlier nightly to scope the comparison window (default: empty).
        lookback: Days to look back (default: '15').
        configs: Comma-separated config files (default: empty, auto-discovered from ES).
        platform: Cloud platform — 'aws', 'rosa-hcp', 'gcp', 'metal' (default: 'aws' 6-node payload).
        workload: Substring matched against CI job name (default: empty).
        scale: Worker node count (default: empty).
        fips/ipsec/encrypted: Boolean filters (default: false).

    Returns:
        Search filters header + regression details (config, version, PRs, metrics with % change)
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

    requested_configs = set()
    if configs.strip():
        requested_configs = {c.strip() for c in configs.split(",") if c.strip()}

    discovered, filters_used, jobs = await _discover_configs_with_vars(
        nightly_info.major_version, platform=platform, workload=workload,
        scale=scale, fips=fips, ipsec=ipsec, encrypted=encrypted,
    )
    filter_str = ", ".join(f"{k}={v}" for k, v in filters_used.items())

    if not discovered:
        search_header = f"[Search: {filter_str}, jobs_found=0]\n\n"
        return search_header + f"No jobs found matching filters for version {nightly_info.major_version}"

    if requested_configs:
        available_configs = {d["config"] for d in discovered}
        discovered = [d for d in discovered if d["config"] in requested_configs]
        if not discovered:
            return (
                f"No ES jobs matched the requested configs: {', '.join(requested_configs)}. "
                f"Available configs from discovery: {', '.join(available_configs)}"
            )

    search_header = f"[Search: {filter_str}, configs_found={len(discovered)}]\n\n"

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
        except (json.JSONDecodeError, TypeError):
            stderr_snippet = (result.stderr or result.stdout or "")[:200].strip()
            all_regressions.append(f"❌ Error: Orion failed for {config} (exit {result.returncode}): {stderr_snippet}")
            continue

        if not isinstance(data, list):
            all_regressions.append(f"❌ Error: Orion returned unexpected data type for {config}: {type(data).__name__}")
            continue

        data = filter_data_by_timestamp(data, nightly_info.nightly_date)
        if prev_nightly_info:
            data = [e for e in data if e.get("timestamp") and _timestamp_after(e["timestamp"], prev_nightly_info.nightly_date)]

        details = _extract_regression_details(json.dumps(data))
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
            "Cloud platform (ES 'platform'/'clusterType'). Values: 'aws' (default), "
            "'rosa-hcp', 'rosa', 'gcp', 'azure', 'metal'. "
            "When specified, returns jobs for that platform (no payload restriction). "
            "When omitted, defaults to AWS 6-node payload jobs."
        ),
    )] = "",
    workload: Annotated[str, Field(
        description=(
            "Workload filter — matched as substring in job name. Common values: "
            "'control-plane', 'data-path', 'node-density-heavy', 'netpol', 'udn-density-l3'. "
            "When omitted and no modifier set, defaults to 6-node payload jobs."
        ),
    )] = "",
    scale: Annotated[str, Field(
        description=(
            "Worker node count (ES 'workerNodesCount'). Values: '3', '6' (payload), '9', "
            "'24', '120', '249', '252'."
        ),
    )] = "",
    fips: Annotated[bool, Field(description="Filter on ES 'fips' field.")] = False,
    ipsec: Annotated[bool, Field(description="Filter on ES 'ipsec' field.")] = False,
    encrypted: Annotated[bool, Field(description="Filter on ES 'encrypted' field.")] = False,
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
    """Broad health check for a version — aggregated stats (min, max, avg, change%) across ALL metrics and ALL benchmarks. Only for broad questions like "how is 4.22 doing" — NOT for named metrics like podReadyLatency or ovnCPU.

    Triggers: "how is 4.22 doing overall", "give me a health check for 5.0",
    "is 4.20 healthy", "overall performance report for 4.22".

    Discovers jobs from ES, runs Orion for each config, computes stats with change% vs prior period.

    Args:
        version: OpenShift version (default: '4.19').
        lookback: Days to look back (default: 14).
        platform: Cloud platform — 'aws', 'rosa-hcp', 'gcp', 'metal' (default: 'aws' 6-node payload).
        workload: Substring matched against CI job name (default: empty).
        scale: Worker node count (default: empty).
        fips/ipsec/encrypted: Boolean filters (default: false).
        config_name: Specific config file (default: None, all configs auto-discovered from ES).

    Returns:
        Dict with 'success', 'search', and 'configs' (per-benchmark stats:
        min, max, avg, change_percent, direction, threshold per metric).
    """
    _extract_and_set_es_server(ctx)

    es_benchmarks = []
    if config_name:
        es_benchmarks = _CONFIG_TO_BENCHMARKS.get(config_name, [])
        if not es_benchmarks:
            return {"success": False, "search": {"message": f"No benchmark mapping found for config '{config_name}'"}, "configs": []}

    configs_to_run, filters_used, jobs_for_search = await _discover_configs_with_vars(
        version, platform=platform, workload=workload, scale=scale,
        fips=fips, ipsec=ipsec, encrypted=encrypted,
        es_benchmarks=es_benchmarks,
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

