"""Tool for generating Orion performance analysis reports."""
import json
from typing import Annotated

from pydantic import Field
from fastmcp import Context
from fastmcp.utilities.types import Image
from fastmcp.tools import tool

from utils.constants import ORION_CONFIGS_PATH
from utils.utils import run_orion, summarize_result, generate_multi_line_plot, validate_config_name
from utils.es_context import extract_and_set_es_server


@tool
async def openshift_report_on(
    versions: Annotated[str, Field(description="Comma-separated list of OpenShift versions e.g. '4.19,4.20'")] = "4.19",
    lookback: Annotated[str, Field(description="Number of days to lookback")] = "15",
    since: Annotated[str, Field(description="Date to begin lookback")] = None,
    *,
    metric: Annotated[str, Field(description="Metric to analyze")] = "podReadyLatency_P99",
    config_name: Annotated[
        str | None,
        Field(description="Orion configuration file name (e.g. 'small-scale-udn-l3.yaml')"),
    ] = None,
    options: Annotated[str, Field(description="Options in format 'output_format' or 'output_format:display_field'. Examples: 'image', 'json', 'both', 'json:ocpVirtVersion'")] = "image",
    ctx: Context = None,
) -> str | Image:
    """Captures a performance analysis against the specified OpenShift version using Orion.

    Orion uses an EDivisive algorithm to analyze performance data from a specified
    configuration file to detect any performance regressions.
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

    series: dict[str, list[float]] = {}
    full_data: dict[str, dict] = {}

    default_config = "small-scale-udn-l3.yaml"
    config_value = validate_config_name(config_name or default_config)
    errors = []
    for ver in version_list:
        result = await run_orion(
            config=ORION_CONFIGS_PATH + config_value,
            version=ver,
            lookback=lookback,
            since=since,
            display=display if display.strip() else None,
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

        series[ver] = values
        full_data[ver] = sum_result
        print(f"series: {series}")

    if errors and not series:
        return "\n".join(errors)

    if output_format.lower() == "json":
        json_output = {
            "config": config_value,
            "metric": metric,
            "lookback": lookback,
            "display": display if display.strip() else None,
            "data": full_data,
        }
        if errors:
            json_output["warnings"] = errors
        return json.dumps(json_output, indent=2)

    if output_format.lower() == "both":
        json_output = {
            "config": config_value,
            "metric": metric,
            "lookback": lookback,
            "display": display if display.strip() else None,
            "data": full_data,
            "plot_info": "Image data follows JSON data",
        }
        if errors:
            json_output["warnings"] = errors
        try:
            img_b64 = generate_multi_line_plot(series, metric, title_prefix=f"{config_value}: ")
            combined_output = json.dumps(json_output, indent=2) + "\n\n[IMAGE_DATA_BASE64]\n" + img_b64.decode("utf-8")
            return combined_output
        except ValueError as e:
            return f"Error generating plot: {str(e)}\n\nJSON data:\n{json.dumps(json_output, indent=2)}"

    else:
        try:
            img_b64 = generate_multi_line_plot(series, metric, title_prefix=f"{config_value}: ")
            return Image(data=img_b64.decode("utf-8"), format="png")
        except ValueError as e:
            return str(e)
