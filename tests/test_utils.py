import math
from datetime import datetime, timezone

import pytest

from utils.utils import (
    compute_correlation,
    filter_data_by_timestamp,
    orion_configs,
    parse_nightly_version,
    parse_timestamp,
    resolve_env_var,
)


class TestResolveEnvVar:
    def test_primary_set(self, monkeypatch):
        monkeypatch.setenv("MY_VAR", "primary_val")
        assert resolve_env_var("MY_VAR", "MY_VAR_ALT", "default") == "primary_val"

    def test_secondary_fallback(self, monkeypatch):
        monkeypatch.delenv("MY_VAR", raising=False)
        monkeypatch.setenv("MY_VAR_ALT", "secondary_val")
        assert resolve_env_var("MY_VAR", "MY_VAR_ALT", "default") == "secondary_val"

    def test_default_when_none_set(self, monkeypatch):
        monkeypatch.delenv("MY_VAR", raising=False)
        monkeypatch.delenv("MY_VAR_ALT", raising=False)
        assert resolve_env_var("MY_VAR", "MY_VAR_ALT", "default") == "default"

    def test_empty_primary_falls_through(self, monkeypatch):
        monkeypatch.setenv("MY_VAR", "   ")
        monkeypatch.setenv("MY_VAR_ALT", "secondary_val")
        assert resolve_env_var("MY_VAR", "MY_VAR_ALT", "default") == "secondary_val"

    def test_empty_both_returns_default(self, monkeypatch):
        monkeypatch.setenv("MY_VAR", "")
        monkeypatch.setenv("MY_VAR_ALT", "")
        assert resolve_env_var("MY_VAR", "MY_VAR_ALT", "fallback") == "fallback"


class TestOrionConfigs:
    def test_extracts_basenames(self):
        result = orion_configs(["/orion/examples/cluster-density.yaml", "/orion/examples/node-density.yaml"])
        assert result == ["cluster-density.yaml", "node-density.yaml"]

    def test_empty_list(self):
        assert orion_configs([]) == []

    def test_single_basename(self):
        assert orion_configs(["just-a-name.yaml"]) == ["just-a-name.yaml"]


class TestComputeCorrelation:
    def test_perfect_positive(self):
        assert compute_correlation([1, 2, 3, 4], [10, 20, 30, 40]) == pytest.approx(1.0)

    def test_perfect_negative(self):
        assert compute_correlation([1, 2, 3, 4], [40, 30, 20, 10]) == pytest.approx(-1.0)

    def test_unequal_lengths_returns_nan(self):
        assert math.isnan(compute_correlation([1, 2], [1, 2, 3]))

    def test_too_few_items_returns_nan(self):
        assert math.isnan(compute_correlation([1], [2]))

    def test_zero_variance_returns_nan(self):
        assert math.isnan(compute_correlation([5, 5, 5], [1, 2, 3]))

    def test_no_correlation(self):
        r = compute_correlation([1, 2, 3, 4, 5], [5, 1, 4, 2, 3])
        assert -0.5 < r < 0.5


class TestParseNightlyVersion:
    def test_nightly_format(self):
        info = parse_nightly_version("4.22.0-0.nightly-2026-01-05-203335")
        assert info.is_nightly is True
        assert info.major_version == "4.22"
        assert info.full_version == "4.22.0-0.nightly-2026-01-05-203335"
        assert info.nightly_date == datetime(2026, 1, 5, 20, 33, 35)

    def test_ga_major_minor(self):
        info = parse_nightly_version("4.17")
        assert info.is_nightly is False
        assert info.major_version == "4.17"
        assert info.nightly_date is None

    def test_ga_with_patch(self):
        info = parse_nightly_version("4.17.0")
        assert info.is_nightly is False
        assert info.major_version == "4.17"

    def test_strips_whitespace(self):
        info = parse_nightly_version("  5.0  ")
        assert info.major_version == "5.0"

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError, match="Invalid version format"):
            parse_nightly_version("not-a-version")

    def test_partial_nightly_raises(self):
        with pytest.raises(ValueError, match="Invalid version format"):
            parse_nightly_version("4.22.0-0.nightly-2026-01-05")

    def test_5x_nightly(self):
        info = parse_nightly_version("5.0.0-0.nightly-2026-08-10-122052")
        assert info.major_version == "5.0"
        assert info.is_nightly is True
        assert info.nightly_date == datetime(2026, 8, 10, 12, 20, 52)


class TestParseTimestamp:
    def test_unix_int(self):
        dt = parse_timestamp(1700000000)
        assert dt is not None
        assert dt.tzinfo == timezone.utc

    def test_unix_float(self):
        dt = parse_timestamp(1700000000.5)
        assert dt is not None

    def test_unix_string(self):
        dt = parse_timestamp("1700000000")
        assert dt is not None

    def test_iso_string(self):
        dt = parse_timestamp("2026-01-15T10:30:00Z")
        assert dt is not None
        assert dt.year == 2026
        assert dt.month == 1
        assert dt.day == 15

    def test_iso_with_timezone(self):
        dt = parse_timestamp("2026-01-15T10:30:00+00:00")
        assert dt is not None

    def test_none_returns_none(self):
        assert parse_timestamp(None) is None

    def test_garbage_returns_none(self):
        assert parse_timestamp("not-a-date") is None

    def test_empty_string_returns_none(self):
        assert parse_timestamp("") is None


class TestFilterDataByTimestamp:
    def test_filters_after_cutoff(self):
        cutoff = datetime(2026, 1, 10, tzinfo=timezone.utc)
        data = [
            {"timestamp": 1736380800, "value": "before"},   # 2025-01-09
            {"timestamp": 1736467200, "value": "on"},       # 2025-01-10
            {"timestamp": 1768003200, "value": "after"},    # 2026-01-10
            {"timestamp": 1768089600, "value": "way_after"},# 2026-01-11
        ]
        result = filter_data_by_timestamp(data, cutoff)
        # Only entries on or before cutoff
        assert len(result) <= len(data)
        for entry in result:
            dt = parse_timestamp(entry["timestamp"])
            assert dt <= cutoff

    def test_empty_data(self):
        cutoff = datetime(2026, 1, 10, tzinfo=timezone.utc)
        assert filter_data_by_timestamp([], cutoff) == []

    def test_no_timestamp_field_excluded(self):
        cutoff = datetime(2030, 1, 1, tzinfo=timezone.utc)
        data = [{"no_timestamp": True}]
        assert filter_data_by_timestamp(data, cutoff) == []
