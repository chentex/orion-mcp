"""Tool for discovering CI jobs, benchmarks, and configs from Elasticsearch and prow."""

import asyncio
import logging
import os
import re
from typing import Annotated

import httpx
from fastmcp import Context
from fastmcp.tools import tool
from pydantic import Field

from components.common import (
    VersionParam,
    extract_and_set_es_server,
)
from utils.constants import (
    ES_HTTP_TIMEOUT,
    GCSWEB_BASE_URL,
    PROW_CONCURRENCY_LIMIT,
    PROW_HTTP_TIMEOUT,
    PROW_VIEW_PREFIX,
)
from utils.utils import get_data_source, get_es_metadata_index

logger = logging.getLogger(__name__)

_VERIFY_TLS = os.getenv("ORION_VERIFY_TLS", "true").lower() != "false"
_PROW_SEMAPHORE = asyncio.Semaphore(PROW_CONCURRENCY_LIMIT)


async def _resolve_configs_from_prow(build_url: str) -> list[str]:
    """Resolve Orion config filenames from prow build-log artifacts."""
    if not build_url or PROW_VIEW_PREFIX not in build_url:
        return []

    gcs_path = build_url.replace(PROW_VIEW_PREFIX, "")
    gcs_base = f"{GCSWEB_BASE_URL}/{gcs_path}"

    configs = set()
    try:
        async with httpx.AsyncClient(timeout=PROW_HTTP_TIMEOUT, verify=_VERIFY_TLS, follow_redirects=True) as client:
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
                async with _PROW_SEMAPHORE:
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
        logger.error("Failed to resolve configs from prow: %s", exc)

    return sorted(configs)


@tool
async def discover_jobs(
    version: VersionParam = "",
    platform: Annotated[str | None, Field(description="Platform filter (e.g. 'AWS', 'GCP', 'BareMetal')")] = None,
    cluster_type: Annotated[str | None, Field(description="Cluster type filter (e.g. 'self-managed', 'rosa-hcp')")] = None,
    workload: Annotated[str | None, Field(description="Workload substring filter on job name (e.g. 'payload', 'control-plane')")] = None,
    scale: Annotated[int | None, Field(description="Worker node count filter (e.g. 6, 24)")] = None,
    fips: Annotated[str | None, Field(description="FIPS filter ('true' or 'false')")] = None,
    ipsec: Annotated[str | None, Field(description="IPsec filter ('true' or 'false')")] = None,
    encrypted: Annotated[str | None, Field(description="Encryption filter ('true' or 'false')")] = None,
    job_type: Annotated[str, Field(description="Job type filter: 'periodic' (default, scheduled nightly runs) or 'pull' (PR CI runs).")] = "periodic",
    ctx: Context = None,
) -> dict:
    """Discover CI jobs, benchmarks, config files, and cluster metadata from Elasticsearch and prow artifacts.

    Returns job names, benchmarks, resolved Orion config filenames, cluster metadata, and build URLs.
    Config files are resolved automatically from prow build logs.

    Args:
        version: OCP version prefix filter (e.g. '4.22'). Empty string returns all versions.
        platform: Platform filter (e.g. 'AWS', 'GCP', 'BareMetal').
        cluster_type: Cluster type filter (e.g. 'self-managed', 'rosa-hcp').
        workload: Substring filter on job name (e.g. 'payload', 'control-plane', 'udn').
        scale: Worker node count filter (e.g. 6, 24).
        fips: FIPS filter ('true' or 'false').
        ipsec: IPsec filter ('true' or 'false').
        encrypted: Encryption filter ('true' or 'false').
        job_type: 'periodic' (default) for scheduled nightly runs, 'pull' for PR CI runs.

    Returns:
        Dict with 'jobs' mapping job names to benchmarks, configs, metadata, buildUrl.
    """
    extract_and_set_es_server(ctx)

    es_server = get_data_source()
    es_index = get_es_metadata_index()

    must_clauses = [{"term": {"jobType": job_type}}]
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
                            "size": 20 if job_type == "pull" else 3,
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
        async with httpx.AsyncClient(timeout=ES_HTTP_TIMEOUT, verify=_VERIFY_TLS) as client:
            resp = await client.post(
                f"{es_server}/{es_index}/_search",
                json=query,
                headers={"Content-Type": "application/json"},
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as exc:
        logger.error("ES query failed for discover_jobs: %s", exc)
        return {"error": f"ES query failed: {exc}"}

    jobs = {}
    for bucket in data.get("aggregations", {}).get("jobs", {}).get("buckets", []):
        job_name = bucket["key"]
        benchmarks = [b["key"] for b in bucket.get("benchmarks", {}).get("buckets", [])]
        hits = bucket.get("top_hit", {}).get("hits", {}).get("hits", [])
        metadata = hits[0]["_source"] if hits else {}
        build_urls = [h["_source"].get("buildUrl", "") for h in hits if h["_source"].get("buildUrl")]

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
            "_build_urls": build_urls,
        }

    async def _resolve_for_job(job_data):
        for url in job_data.pop("_build_urls", []):
            configs = await _resolve_configs_from_prow(url)
            if configs:
                job_data["configs"] = configs
                return
        job_data["configs"] = []

    await asyncio.gather(*[_resolve_for_job(d) for d in jobs.values()])

    return {"jobs": jobs, "total": len(jobs)}
