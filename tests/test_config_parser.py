import os
import tempfile

import pytest

from utils.config_parser import load_config_metrics_with_meta, metric_key, render_config_yaml


class TestMetricKey:
    def test_simple_metric(self):
        assert metric_key({"name": "podReadyLatency", "metric_of_interest": "P99"}) == "podReadyLatency_P99"

    def test_default_metric_of_interest(self):
        assert metric_key({"name": "cpuUsage"}) == "cpuUsage_value"

    def test_agg_type_overrides(self):
        result = metric_key({"name": "ovnCPU", "agg": {"agg_type": "avg"}})
        assert result == "ovnCPU_avg"

    def test_agg_empty_type_falls_through(self):
        result = metric_key({"name": "ovnCPU", "agg": {"agg_type": ""}})
        assert result == "ovnCPU_value"

    def test_agg_non_dict_ignored(self):
        result = metric_key({"name": "ovnCPU", "agg": "not_a_dict"})
        assert result == "ovnCPU_value"

    def test_missing_name(self):
        assert metric_key({}) == "unknown_value"

    def test_agg_with_no_agg_type_key(self):
        result = metric_key({"name": "mem", "agg": {"field": "x"}})
        assert result == "mem_value"


class TestRenderConfigYaml:
    def test_plain_yaml(self, tmp_path):
        cfg = tmp_path / "test.yaml"
        cfg.write_text("name: test\nversion_used: {{ version }}")
        result = render_config_yaml(str(cfg), version="4.22")
        assert result["name"] == "test"
        assert float(result["version_used"]) == 4.22

    def test_input_vars_override(self, tmp_path):
        cfg = tmp_path / "test.yaml"
        cfg.write_text("platform: {{ platform }}")
        result = render_config_yaml(str(cfg), input_vars={"platform": "AWS"})
        assert result["platform"] == "AWS"

    def test_defaults_applied(self, tmp_path):
        cfg = tmp_path / "test.yaml"
        cfg.write_text("jt: {{ jobtype }}")
        result = render_config_yaml(str(cfg))
        assert result["jt"] == "periodic"

    def test_undefined_var_falls_back_to_lenient(self, tmp_path):
        cfg = tmp_path / "test.yaml"
        cfg.write_text("val: {{ some_undefined_var }}")
        result = render_config_yaml(str(cfg))
        assert result["val"] is None or result["val"] == ""


class TestLoadConfigMetricsWithMeta:
    def test_extracts_metrics_from_tests(self, tmp_path):
        cfg = tmp_path / "bench.yaml"
        cfg.write_text("""
tests:
  - name: test1
    metrics:
      - name: podReadyLatency
        metric_of_interest: P99
        direction: 1
        threshold: 5000.0
      - name: ovnCPU
        agg:
          agg_type: avg
        direction: -1
        threshold: 10.0
""")
        metrics, meta = load_config_metrics_with_meta(str(cfg))
        assert "podReadyLatency_P99" in metrics
        assert "ovnCPU_avg" in metrics
        assert meta["podReadyLatency_P99"]["direction"] == 1
        assert meta["podReadyLatency_P99"]["threshold"] == 5000.0
        assert meta["ovnCPU_avg"]["direction"] == -1

    def test_skips_metadata_type(self, tmp_path):
        cfg = tmp_path / "bench.yaml"
        cfg.write_text("""
tests:
  - name: test1
    metrics:
      - name: timestamp
        type: metadata
      - name: realMetric
        metric_of_interest: value
""")
        metrics, meta = load_config_metrics_with_meta(str(cfg))
        assert "timestamp_value" not in metrics
        assert "realMetric_value" in metrics

    def test_handles_invalid_direction(self, tmp_path):
        cfg = tmp_path / "bench.yaml"
        cfg.write_text("""
tests:
  - name: test1
    metrics:
      - name: mem
        direction: not_a_number
        threshold: also_not
""")
        metrics, meta = load_config_metrics_with_meta(str(cfg))
        assert meta["mem_value"]["direction"] is None
        assert meta["mem_value"]["threshold"] is None

    def test_metrics_file_reference(self, tmp_path):
        metrics_file = tmp_path / "metrics.yaml"
        metrics_file.write_text("""
metrics:
  - name: externalMetric
    metric_of_interest: P50
    direction: 1
    threshold: 100.0
""")
        cfg = tmp_path / "bench.yaml"
        cfg.write_text(f"""
tests:
  - name: test1
    metricsFile: metrics.yaml
""")
        metrics, meta = load_config_metrics_with_meta(str(cfg))
        assert "externalMetric_P50" in metrics
        assert meta["externalMetric_P50"]["threshold"] == 100.0

    def test_empty_config(self, tmp_path):
        cfg = tmp_path / "empty.yaml"
        cfg.write_text("name: empty")
        metrics, meta = load_config_metrics_with_meta(str(cfg))
        assert metrics == []
        assert meta == {}
