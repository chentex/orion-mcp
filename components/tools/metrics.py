import os
from typing import Annotated

from pydantic import Field
from fastmcp import Context
from fastmcp.tools import tool

from utils.constants import ORION_CONFIGS_PATH
from utils.utils import orion_metrics
from utils.config_parser import _load_config_metrics_with_meta
from utils.es_context import extract_and_set_es_server


@tool
async def get_orion_metrics(
    config_name: Annotated[
        str | None,
        Field(description="Orion configuration file name (e.g. 'small-scale-udn-l3.yaml')"),
    ] = None,
    version: Annotated[str, Field(description="OpenShift version used to query metrics")] = "4.20",
    ctx: Context = None,
) -> dict:
    """Return the list of metrics available for a specific Orion *config*."""
    extract_and_set_es_server(ctx)

    default_config = "small-scale-udn-l3.yaml"
    effective_config = config_name or default_config
    result = await orion_metrics([ORION_CONFIGS_PATH + effective_config], version=version)

    if isinstance(result, str):
        return {"error": f"Failed to fetch Orion metrics: {result}"}
    return result


@tool
async def get_orion_metrics_with_meta(
    config_name: Annotated[
        str | None,
        Field(description="Orion configuration file name (e.g. 'small-scale-udn-l3.yaml')"),
    ] = None,
    version: Annotated[str, Field(description="OpenShift version used to render the config template")] = "4.19",
    ctx: Context = None,
) -> dict:
    """Return metrics and metadata for a specific Orion *config*."""
    extract_and_set_es_server(ctx)

    default_config = "small-scale-udn-l3.yaml"
    effective_config = config_name or default_config
    try:
        metrics, meta_map = _load_config_metrics_with_meta(
            os.path.join(ORION_CONFIGS_PATH, effective_config),
            version=version,
        )
        return {"metrics": metrics, "meta": meta_map}
    except Exception as e:
        result = await orion_metrics(
            [ORION_CONFIGS_PATH + effective_config], version=version
        )
        if isinstance(result, str):
            return {"error": f"{e} | {result}"}
        return {"metrics": result, "meta": {}, "warning": str(e)}
