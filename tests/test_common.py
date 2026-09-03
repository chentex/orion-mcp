import os
import types

import pytest

from components.common import (
    config_path,
    orion_error_snippet,
    parse_input_vars,
    split_configs,
)


class TestParseInputVars:
    def test_empty_string_returns_none(self):
        assert parse_input_vars("") is None

    def test_none_returns_none(self):
        assert parse_input_vars(None) is None

    def test_valid_json(self):
        result = parse_input_vars('{"platform": "AWS", "workerNodesCount": 6}')
        assert result == {"platform": "AWS", "workerNodesCount": 6}

    def test_empty_object(self):
        assert parse_input_vars("{}") == {}

    def test_malformed_json_raises(self):
        with pytest.raises(ValueError, match="Malformed input_vars JSON"):
            parse_input_vars("{bad json}")

    def test_non_json_string_raises(self):
        with pytest.raises(ValueError, match="Malformed input_vars JSON"):
            parse_input_vars("just a string")


class TestSplitConfigs:
    def test_none_returns_default(self):
        result = split_configs(None)
        assert result == ["cluster-density.yaml"]

    def test_none_with_custom_default(self):
        result = split_configs(None, default=["a.yaml", "b.yaml"])
        assert result == ["a.yaml", "b.yaml"]

    def test_empty_string_returns_default(self):
        result = split_configs("")
        assert result == ["cluster-density.yaml"]

    def test_single_config(self):
        assert split_configs("node-density.yaml") == ["node-density.yaml"]

    def test_comma_separated(self):
        result = split_configs("a.yaml, b.yaml, c.yaml")
        assert result == ["a.yaml", "b.yaml", "c.yaml"]

    def test_strips_whitespace(self):
        result = split_configs("  a.yaml ,  b.yaml  ")
        assert result == ["a.yaml", "b.yaml"]

    def test_skips_empty_segments(self):
        result = split_configs("a.yaml,,b.yaml,")
        assert result == ["a.yaml", "b.yaml"]


class TestConfigPath:
    def test_joins_with_orion_configs_path(self):
        result = config_path("cluster-density.yaml")
        expected_base = os.getenv("ORION_CONFIGS_PATH", "/orion/examples/")
        assert result == os.path.join(expected_base, "cluster-density.yaml")


class TestOrionErrorSnippet:
    def test_stderr_preferred(self):
        result = types.SimpleNamespace(stderr="error msg", stdout="output msg")
        assert orion_error_snippet(result) == "error msg"

    def test_falls_back_to_stdout(self):
        result = types.SimpleNamespace(stderr="", stdout="output msg")
        assert orion_error_snippet(result) == "output msg"

    def test_empty_returns_empty(self):
        result = types.SimpleNamespace(stderr="", stdout="")
        assert orion_error_snippet(result) == ""

    def test_truncates_at_200_chars(self):
        result = types.SimpleNamespace(stderr="x" * 300, stdout="")
        assert len(orion_error_snippet(result)) == 200
