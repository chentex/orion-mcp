"""Helpers for parsing and rendering Orion YAML configuration files."""
import os

import jinja2
import yaml

from utils.utils import parse_timestamp


def _metric_key(metric: dict) -> str:
    name = metric.get("name", "unknown")
    if "agg" in metric and isinstance(metric["agg"], dict):
        agg_type = metric["agg"].get("agg_type", "")
        if agg_type:
            return f"{name}_{agg_type}"
    metric_of_interest = metric.get("metric_of_interest", "value")
    return f"{name}_{metric_of_interest}"


def _render_config_yaml(config_path: str, version: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as template_file:
        template_content = template_file.read()

    env_vars = {k.lower(): v for k, v in os.environ.items()}
    env_vars.update(
        {
            "version": version,
            "jobtype": "periodic",
            "pull_number": 0,
            "organization": "",
            "repository": "",
        }
    )

    try:
        template = jinja2.Template(template_content, undefined=jinja2.StrictUndefined)
        rendered = template.render(env_vars)
    except jinja2.exceptions.UndefinedError:
        template = jinja2.Template(template_content)
        rendered = template.render(env_vars)

    return yaml.safe_load(rendered)


def _load_config_metrics_with_meta(config_path: str, version: str) -> tuple[list[str], dict]:
    rendered_config = _render_config_yaml(config_path, version)
    metrics_list: list[str] = []
    meta_map: dict = {}

    for test in rendered_config.get("tests", []):
        for metric in test.get("metrics", []):
            key = _metric_key(metric)
            metrics_list.append(key)
            direction_raw = metric.get("direction")
            threshold_raw = metric.get("threshold")
            try:
                direction_val = (
                    int(direction_raw) if direction_raw is not None else None
                )
            except (TypeError, ValueError):
                direction_val = None
            try:
                threshold_val = (
                    float(threshold_raw) if threshold_raw is not None else None
                )
            except (TypeError, ValueError):
                threshold_val = None
            meta_map[key] = {
                "direction": direction_val,
                "threshold": threshold_val,
                "metric_of_interest": metric.get("metric_of_interest"),
                "agg_type": metric.get("agg", {}).get("agg_type") if isinstance(metric.get("agg"), dict) else None,
            }

    return metrics_list, meta_map


def _timestamp_after(timestamp_val, cutoff_datetime) -> bool:
    entry_dt = parse_timestamp(timestamp_val)
    return entry_dt is not None and entry_dt > cutoff_datetime
