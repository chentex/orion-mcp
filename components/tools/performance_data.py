from typing import Annotated

from pydantic import Field
from fastmcp import Context
from fastmcp.tools import tool

from utils.constants import ORION_CONFIGS_PATH
from utils.utils import run_orion, summarize_result
from utils.es_context import extract_and_set_es_server


@tool
async def get_orion_performance_data(
    config_name: Annotated[
        str | None,
        Field(description="Orion configuration file name (e.g. 'small-scale-udn-l3.yaml')"),
    ] = None,
    *,
    metric: Annotated[str, Field(description="Metric to analyze")] = "podReadyLatency_P99",
    version: Annotated[str, Field(description="OpenShift version to analyze")] = "4.19",
    lookback: Annotated[str, Field(description="Number of days to lookback")] = "15",
    since: Annotated[str | None, Field(description="Date to begin looking back for performance data")] = None,
    ctx: Context = None,
) -> dict:
    """Return performance data values for a specific config/metric/version."""
    extract_and_set_es_server(ctx)

    default_config = "small-scale-udn-l3.yaml"
    config_value = config_name or default_config
    try:
        result = await run_orion(
            config=ORION_CONFIGS_PATH + config_value,
            version=version,
            lookback=lookback,
            since=since,
        )
        sum_result = await summarize_result(result, isolate=metric)

        if not isinstance(sum_result, dict) or metric not in sum_result:
            return {"error": f"No data found for metric {metric}"}

        metric_data = sum_result[metric]
        values = metric_data.get("value", [])
        if not isinstance(values, list):
            return {"error": f"Unexpected data format for metric {metric}"}

        values = [v for v in values if v is not None]
        return {
            "config": config_value,
            "metric": metric,
            "version": version,
            "lookback": lookback,
            "values": values,
            "count": len(values),
        }
    except Exception as e:
        return {"error": str(e)}
