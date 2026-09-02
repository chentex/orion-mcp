"""Shared constants for orion-mcp."""

import os

# AES-256-GCM parameters for symmetric decryption of header payloads
AES_GCM_KEY_LENGTH_BYTES = 32
AES_GCM_NONCE_LENGTH_BYTES = 12

# Orion config paths and defaults
ORION_CONFIGS_PATH = os.getenv("ORION_CONFIGS_PATH", "/orion/examples/")
DEFAULT_CONFIG = "cluster-density.yaml"
DEFAULT_LOOKBACK_DAYS = "15"

DEFAULT_NETWORKING_CONFIGS = [
    "node-density-cni.yaml",
    "udn-density-pods.yaml",
]

# OpenShift release dates (GA)
RELEASE_DATES = {
    "4.17": "2024-10-29",
    "4.18": "2025-02-28",
    "4.19": "2025-06-17",
    "4.20": "2025-10-23",
    "4.21": "2026-02-25",
    "4.22": "2026-06-17",
    "5.0": "2026-10-31",
}

# MCP server defaults
MCP_SERVER_HOST = "0.0.0.0"
MCP_SERVER_PORT = 3030

# Elasticsearch index defaults
DEFAULT_ES_METADATA_INDEX = "perf_scale_ci*"
DEFAULT_ES_BENCHMARK_INDEX = "ripsaw-kube-burner-*"

# Prow / GCS URLs
GCSWEB_BASE_URL = "https://gcsweb-ci.apps.ci.l2s4.p1.openshiftapps.com/gcs"
PROW_VIEW_PREFIX = "https://prow.ci.openshift.org/view/gs/"
PROW_CONCURRENCY_LIMIT = 15

# HTTP timeouts (seconds)
PROW_HTTP_TIMEOUT = 15
ES_HTTP_TIMEOUT = 30
GITHUB_HTTP_TIMEOUT = 10

# GitHub Orion Configs
GITHUB_CONFIGS_URL = "https://api.github.com/repos/cloud-bulldozer/orion/contents/examples"
