"""Tool for computing and plotting metric correlations."""

import logging
from typing import Annotated

from fastmcp import Context
from fastmcp.tools import tool
from mcp import types
from pydantic import Field

from components.common import (
    ConfigParam,
    InputVarsParam,
    LookbackParam,
    VersionParam,
    config_path,
    resolve_config_and_vars,
)
from utils.constants import DEFAULT_LOOKBACK_DAYS
from utils.utils import generate_correlation_plot, run_orion, summarize_result

logger = logging.getLogger(__name__)


@tool
async def metrics_correlation(
    metric1: Annotated[str, Field(description="First metric to analyze")] = "podReadyLatency_P99",
    metric2: Annotated[str, Field(description="Second metric to analyze")] = "ovnCPU_avg",
    *,
    config_name: ConfigParam = None,
    since: Annotated[str | None, Field(description="Date to begin looking back for performance data")] = None,
    version: VersionParam = "4.19",
    lookback: LookbackParam = DEFAULT_LOOKBACK_DAYS,
    input_vars: InputVarsParam = "",
    ctx: Context = None,
) -> types.ImageContent | types.TextContent:
    """Check if two metrics are correlated by computing Pearson coefficient and plotting a scatter chart.

    Triggers: "correlate podReadyLatency with ovnCPU", "are ovnCPU and etcdCPU related",
    "is there a correlation between X and Y".

    Args:
        metric1: First metric, Y-axis (default: 'podReadyLatency_P99').
        metric2: Second metric, X-axis (default: 'ovnCPU_avg').
        config_name: Orion config filename.
        since: Start date for lookback (default: None).
        version: OpenShift version (default: '4.19').
        lookback: Days to look back (default: '15').
        input_vars: JSON string of template variables for the config.

    Returns:
        ImageContent (scatter-plot PNG) or TextContent (error).
    """
    config_value, iv = await resolve_config_and_vars(ctx, config_name, version, input_vars)

    result = await run_orion(
        config=config_path(config_value),
        version=version,
        lookback=lookback,
        since=since,
        input_vars=iv,
    )

    summary = await summarize_result(result)

    if not isinstance(summary, dict):
        logger.warning("Error processing Orion output: %s", summary)
        return types.TextContent(type="text", text=f"Error processing Orion output: {summary}")

    try:
        raw1 = summary[metric1]["value"]
        raw2 = summary[metric2]["value"]
    except KeyError:
        return types.TextContent(
            type="text",
            text="Requested metrics not present in the Orion summary for the chosen configuration.",
        )

    paired = [
        (v1, v2) for v1, v2 in zip(raw1, raw2)
        if isinstance(v1, (int, float)) and isinstance(v2, (int, float))
    ]
    if len(paired) < 2:
        return types.TextContent(
            type="text",
            text=f"Not enough valid data points to compute correlation (got {len(paired)}, need at least 2).",
        )
    values1, values2 = zip(*paired)

    corr_b64 = generate_correlation_plot(list(values1), list(values2), metric1, metric2, title_prefix=f"{config_value}: ")

    return types.ImageContent(type="image", data=corr_b64.decode("utf-8"), mime_type="image/png")
