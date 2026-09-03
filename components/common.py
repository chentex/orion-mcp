"""Shared parameter types and helper functions used across MCP tool modules."""

import json
import logging
import os
from typing import Annotated

from pydantic import Field

from utils.constants import DEFAULT_CONFIG, ORION_CONFIGS_PATH
from utils.header_decryption import get_es_config_from_headers
from utils.utils import current_es_config

logger = logging.getLogger(__name__)

VersionParam = Annotated[str, Field(description="OpenShift version (e.g. '4.22', '5.0')")]
LookbackParam = Annotated[str, Field(description="Number of days to lookback")]
ConfigParam = Annotated[str | None, Field(
    description="Orion configuration file name (e.g. 'cluster-density.yaml'). For regression tools, supports comma-separated list (e.g. 'cluster-density.yaml,node-density.yaml').",
)]
InputVarsParam = Annotated[str, Field(
    description="JSON string of template variables for the config (e.g. platform, workerNodesCount, clusterType, fips, ipsec, encrypted, networkType, masterNodesType, masterNodesCount, workerNodesType, jobtype).",
)]


def parse_input_vars(input_vars: str) -> dict | None:
    """Parse a JSON input_vars string into a dict, or return None if empty."""
    if not input_vars:
        return None
    try:
        return json.loads(input_vars)
    except (json.JSONDecodeError, TypeError) as exc:
        raise ValueError(f"Malformed input_vars JSON: {exc}") from exc


def split_configs(config_name: str | None, default: list[str] | None = None) -> list[str]:
    """Split a comma-separated config_name into a list."""
    if not config_name:
        return default if default is not None else [DEFAULT_CONFIG]
    return [c.strip() for c in config_name.split(",") if c.strip()]


def config_path(config_name: str) -> str:
    """Return the full filesystem path for an Orion config filename."""
    resolved = os.path.realpath(os.path.join(ORION_CONFIGS_PATH, config_name))
    configs_root = os.path.realpath(ORION_CONFIGS_PATH)
    if not resolved.startswith(configs_root + os.sep) and resolved != configs_root:
        raise ValueError(f"Config path escapes allowed directory: {config_name}")
    return resolved


def orion_error_snippet(result) -> str:
    """Extract a short error message from an Orion subprocess result."""
    return (result.stderr or result.stdout or "")[:200].strip()


async def resolve_config_and_vars(ctx, config_name, _version, input_vars=""):
    """Common setup: extract ES config, parse config name and input_vars."""
    extract_and_set_es_server(ctx)
    config_value = config_name or DEFAULT_CONFIG
    try:
        iv = parse_input_vars(input_vars) if input_vars else None
    except ValueError as exc:
        raise ValueError(f"Failed to resolve config/vars: {exc}") from exc
    return config_value, iv


def extract_and_set_es_server(ctx) -> None:
    """Extract ES config from request headers and set in context variable."""
    if not ctx:
        return
    try:
        if hasattr(ctx, 'request_context') and ctx.request_context:
            request = ctx.request_context.request
            if request and hasattr(request, 'headers'):
                headers_dict = dict(request.headers)
                es_config = get_es_config_from_headers(headers_dict)
                if es_config:
                    current_es_config.set(es_config)
    except Exception as exc:
        logger.debug("Failed to extract ES config from headers, falling back to env vars: %s", exc)
