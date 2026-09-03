"""Tools for visualizing metrics across versions and generating performance summaries."""

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
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
    extract_and_set_es_server,
    parse_input_vars,
    resolve_config_and_vars,
    split_configs,
)
from utils.config_parser import load_config_metrics_with_meta
from utils.constants import DEFAULT_LOOKBACK_DAYS
from utils.utils import (
    generate_multi_line_plot,
    parse_timestamp,
    run_orion,
    summarize_result,
)

logger = logging.getLogger(__name__)


@tool
async def openshift_report_on(
    versions: Annotated[str, Field(description="Comma-separated list of OpenShift versions e.g. '4.19,4.20'")] = "4.19",
    lookback: LookbackParam = DEFAULT_LOOKBACK_DAYS,
    since: Annotated[str, Field(description="Date to begin lookback")] = None,
    *,
    metric: Annotated[str, Field(description="Metric to analyze")] = "podReadyLatency_P99",
    config_name: ConfigParam = None,
    input_vars: InputVarsParam = "",
    options: Annotated[str, Field(description="Options in format 'output_format' or 'output_format:display_field'. Examples: 'image', 'json', 'both', 'json:ocpVirtVersion'")] = "image",
    ctx: Context = None,
) -> types.ImageContent | types.TextContent:
    """Show or compare a specific metric across OpenShift versions. Use when a user asks to visualize, plot, or compare metric values across versions.

    Triggers: "show podReadyLatency for 4.22", "compare ovnCPU for 4.22 vs 5.0", "plot etcdCPU over time".

    Args:
        versions: Comma-separated versions to analyze (default: '4.19').
        lookback: Days to look back (default: '15').
        since: Start date for lookback (default: None).
        metric: Metric to plot (default: 'podReadyLatency_P99').
        config_name: Orion config filename or comma-separated list.
        input_vars: JSON string of template variables for the config.
        options: Output format — 'image' (default), 'json', 'both', or 'json:displayField'.

    Returns:
        Image (PNG chart) by default, or JSON data when options='json'.
    """
    extract_and_set_es_server(ctx)

    if ":" in options:
        output_format, display = options.split(":", 1)
    else:
        output_format = options
        display = ""

    if isinstance(versions, str):
        version_list = [v.strip() for v in versions.split(',') if v.strip()]
    else:
        version_list = list(versions)

    first_ver = version_list[0] if version_list else "4.19"
    configs = split_configs(config_name)
    if not configs:
        config_value, iv = await resolve_config_and_vars(ctx, None, first_ver, input_vars)
        configs = [config_value]
    else:
        _, iv = await resolve_config_and_vars(ctx, None, first_ver, input_vars)

    all_series: dict[str, list[float]] = {}
    all_full_data: list[dict] = []
    all_errors = []

    for cfg in configs:
        series: dict[str, list[float]] = {}
        full_data: dict[str, dict] = {}
        errors = []

        for ver in version_list:
            result = await run_orion(
                config=config_path(cfg),
                version=ver,
                lookback=lookback,
                since=since,
                input_vars=iv,
                display=display if display.strip() else None,
            )

            sum_result = await summarize_result(result, isolate=metric)

            if not isinstance(sum_result, dict) or metric not in sum_result:
                logger.warning("No data for metric %s, version %s, config %s", metric, ver, cfg)
                errors.append(f"[{cfg}] No data for version {ver}, metric {metric}")
                continue

            raw_values = sum_result[metric].get("value", [])
            if not isinstance(raw_values, list):
                errors.append(f"[{cfg}] Unexpected data format for version {ver}")
                continue

            values = [v for v in raw_values if v is not None]
            if not values:
                errors.append(f"[{cfg}] All values are None for version {ver}")
                continue

            label = f"{cfg}:{ver}" if len(configs) > 1 else ver
            series[label] = values
            full_data[ver] = sum_result
            logger.debug("series: %s", series)

        all_errors.extend(errors)
        if series:
            all_series.update(series)
            all_full_data.append({"config": cfg, "metric": metric, "lookback": lookback,
                                   "display": display if display.strip() else None, "data": full_data})

    if all_errors and not all_series:
        return types.TextContent(type="text", text="\n".join(all_errors))

    if output_format.lower() == "json":
        output = all_full_data[0] if len(all_full_data) == 1 else {"results": all_full_data}
        return types.TextContent(type="text", text=json.dumps(output, indent=2))

    if output_format.lower() == "both":
        output = all_full_data[0] if len(all_full_data) == 1 else {"results": all_full_data}
        output["plot_info"] = "Image data follows JSON data"
        try:
            img_b64 = generate_multi_line_plot(all_series, metric)
            combined = json.dumps(output, indent=2) + "\n\n[IMAGE_DATA_BASE64]\n" + img_b64.decode("utf-8")
            return types.TextContent(type="text", text=combined)
        except ValueError as e:
            return types.TextContent(type="text", text=f"Error generating plot: {e}\n\nJSON data:\n{json.dumps(output, indent=2)}")

    try:
        img_b64 = generate_multi_line_plot(all_series, metric)
        return types.ImageContent(type="image", data=img_b64.decode("utf-8"), mimeType="image/png")
    except ValueError as e:
        return types.TextContent(type="text", text=str(e))


async def _summarize_single_config(
    config_value: str, version: str, lookback: int, iv: dict | None,
) -> dict:
    """Run Orion for one config and compute per-metric stats."""
    full_path = config_path(config_value)

    try:
        metrics_list, meta_map = load_config_metrics_with_meta(full_path, version, input_vars=iv)
    except Exception as e:
        logger.error("Failed to load config metrics for %s: %s", config_value, e)
        return {"config": config_value, "success": False, "error": f"Failed to load config metrics: {e}"}

    try:
        result = await run_orion(config=full_path, version=version, lookback=str(lookback), input_vars=iv)
        sum_result = await summarize_result(result)
    except Exception as e:
        logger.error("Orion execution failed for %s: %s", config_value, e)
        return {"config": config_value, "success": False, "error": f"Orion failed: {e}"}

    if not isinstance(sum_result, dict):
        return {"config": config_value, "success": False, "error": f"Unexpected Orion output: {sum_result}"}

    cutoff_dt = datetime.now(tz=timezone.utc) - timedelta(days=lookback)
    prior_sum: dict[str, list] = {}
    try:
        prior_result = await run_orion(config=full_path, version=version, lookback=str(lookback * 2), input_vars=iv)
        prior_sum_raw = await summarize_result(prior_result)
        if isinstance(prior_sum_raw, dict):
            for run in prior_sum_raw.get("runs", []):
                run_dt = parse_timestamp(run.get("timestamp"))
                if run_dt is None or run_dt >= cutoff_dt:
                    continue
                for m_name, m_data in run.get("metrics", {}).items():
                    v = m_data.get("value")
                    if v is not None:
                        prior_sum.setdefault(m_name, []).append(v)
    except Exception as exc:
        logger.warning("Prior-period query failed for %s: %s", config_value, exc)

    metric_summaries = []
    for m_name in metrics_list:
        if m_name not in sum_result:
            continue
        values = [v for v in sum_result[m_name].get("value", []) if v is not None]
        if not values:
            continue
        avg_val = sum(values) / len(values)
        meta = meta_map.get(m_name, {})
        previous_values = prior_sum.get(m_name, [])
        change_pct = None
        if previous_values:
            prev_avg = sum(previous_values) / len(previous_values)
            if prev_avg != 0:
                change_pct = round(((avg_val - prev_avg) / abs(prev_avg)) * 100, 2)
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

    return {"config": config_value, "success": bool(metric_summaries), "metrics": metric_summaries}


@tool
async def get_performance_summary(
    version: VersionParam = "4.19",
    lookback: Annotated[int, Field(description="Number of days to look back for data")] = int(DEFAULT_LOOKBACK_DAYS),
    config_name: ConfigParam = None,
    input_vars: InputVarsParam = "",
    ctx: Context = None,
) -> dict:
    """Health check — aggregated stats (min, max, avg, change%) across ALL metrics for one or more configs.

    Triggers: "how is 4.22 doing overall", "give me a performance summary for 5.0",
    "is 4.20 healthy", "overall performance report for 4.22".

    Args:
        version: OpenShift version (default: '4.19').
        lookback: Days to look back (default: 14).
        config_name: Orion config filename or comma-separated list.
        input_vars: JSON string of template variables for the config.

    Returns:
        Dict with per-config results, each containing per-metric stats.
    """
    extract_and_set_es_server(ctx)
    try:
        iv = parse_input_vars(input_vars)
    except ValueError as exc:
        return {"success": False, "error": str(exc), "results": []}

    configs = split_configs(config_name)
    if not configs:
        config_value, iv = await resolve_config_and_vars(ctx, None, version, input_vars)
        configs = [config_value]

    results = await asyncio.gather(*[
        _summarize_single_config(c, version, lookback, iv) for c in configs
    ])

    return {"success": any(r.get("success") for r in results), "results": list(results)}
