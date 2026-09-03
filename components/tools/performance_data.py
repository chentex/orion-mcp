"""Tool for fetching raw performance metric values."""

import logging
from typing import Annotated

from fastmcp import Context
from fastmcp.tools import tool
from pydantic import Field

from components.common import (
    ConfigParam,
    InputVarsParam,
    LookbackParam,
    VersionParam,
    config_path,
    extract_and_set_es_server,
    resolve_config_and_vars,
    split_configs,
)
from utils.constants import DEFAULT_LOOKBACK_DAYS
from utils.utils import run_orion, summarize_result

logger = logging.getLogger(__name__)


@tool
async def get_orion_performance_data(
    config_name: ConfigParam = None,
    *,
    metric: Annotated[str, Field(description="Metric to analyze")] = "podReadyLatency_P99",
    version: VersionParam = "4.19",
    lookback: LookbackParam = DEFAULT_LOOKBACK_DAYS,
    since: Annotated[str | None, Field(description="Date to begin looking back for performance data")] = None,
    input_vars: InputVarsParam = "",
    ctx: Context = None,
) -> dict:
    """Return flat metric values for programmatic use (min/max/avg computation). Use when raw values are needed rather than a chart.

    Triggers: "get raw podReadyLatency values for 4.22", "fetch ovnCPU numbers for cluster-density".

    Args:
        config_name: Orion config filename or comma-separated list.
        metric: Metric to fetch (default: 'podReadyLatency_P99').
        version: OpenShift version (default: '4.19').
        lookback: Days to look back (default: '15').
        since: Start date for lookback (default: None).
        input_vars: JSON string of template variables for the config.

    Returns:
        Single config: {config, metric, version, lookback, values, count}.
        Multiple configs: {results: [...]}.
    """
    extract_and_set_es_server(ctx)
    configs = split_configs(config_name)
    if not configs:
        config_value, iv = await resolve_config_and_vars(ctx, None, version, input_vars)
        configs = [config_value]
    else:
        _, iv = await resolve_config_and_vars(ctx, None, version, input_vars)

    results = []
    for cfg in configs:
        try:
            result = await run_orion(
                config=config_path(cfg),
                version=version,
                lookback=lookback,
                since=since,
                input_vars=iv,
            )
            sum_result = await summarize_result(result, isolate=metric)

            if not isinstance(sum_result, dict) or metric not in sum_result:
                logger.warning("No data for version %s in config %s: %s", version, cfg, sum_result)
                results.append({"config": cfg, "error": f"No data found for metric {metric}"})
                continue

            values = sum_result[metric].get("value", [])
            if not isinstance(values, list):
                logger.warning("Unexpected data format for metric %s in config %s", metric, cfg)
                results.append({"config": cfg, "error": f"Unexpected data format for metric {metric}"})
                continue

            values = [v for v in values if v is not None]
            results.append({
                "config": cfg,
                "metric": metric,
                "version": version,
                "lookback": lookback,
                "values": values,
                "count": len(values),
            })
        except Exception as e:
            results.append({"config": cfg, "error": str(e)})

    return results[0] if len(results) == 1 else {"results": results}
