"""Tool for computing and visualising metric correlations."""
# pylint: disable=duplicate-code
from typing import Annotated

from pydantic import Field
from fastmcp import Context
from fastmcp.utilities.types import Image
from fastmcp.tools import tool

from utils.constants import ORION_CONFIGS_PATH
from utils.utils import run_orion, summarize_result, generate_correlation_plot, validate_config_name
from utils.es_context import extract_and_set_es_server


@tool
async def metrics_correlation(
    metric1: Annotated[str, Field(description="First metric to analyze")] = "podReadyLatency_P99",
    metric2: Annotated[str, Field(description="Second metric to analyze")] = "ovnCPU_avg",
    *,
    config_name: Annotated[
        str | None,
        Field(description="Orion configuration file name (e.g. 'trt-external-payload-cluster-density.yaml')"),
    ] = None,
    since: Annotated[str, Field(description="Date to begin looking back for performance data")] = None,
    version: Annotated[str, Field(description="Version of OpenShift to look into")] = "4.19",
    lookback: Annotated[str, Field(description="Number of days to lookback")] = "15",
    ctx: Context = None,
) -> str | Image:
    """Calculate and visualise the correlation between two metrics for a given
    Orion configuration.

    A scatter-plot annotated with the Pearson correlation coefficient is
    returned.
    """
    extract_and_set_es_server(ctx)

    default_config = "trt-external-payload-cluster-density.yaml"
    config_value = validate_config_name(config_name or default_config)

    result = await run_orion(
        config=ORION_CONFIGS_PATH + config_value,
        version=version,
        lookback=lookback,
        since=since,
    )

    summary = await summarize_result(result)

    if not isinstance(summary, dict):
        return f"Error processing Orion output: {summary}"

    try:
        values1 = summary[metric1]["value"]
        values2 = summary[metric2]["value"]
    except KeyError:
        return "Requested metrics not present in the Orion summary for the chosen configuration."

    corr_b64 = generate_correlation_plot(values1, values2, metric1, metric2, title_prefix=f"{config_value}: ")

    return Image(data=corr_b64.decode("utf-8"), format="png")
