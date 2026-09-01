"""Tests for utils.utils module."""
import json
from datetime import datetime

import pytest

from utils.utils import (
    parse_nightly_version,
    parse_timestamp,
    filter_data_by_timestamp,
    validate_config_name,
    safe_json_loads,
)


class TestParseNightlyVersion:
    """Tests for parse_nightly_version parser."""

    def test_valid_nightly(self):
        """Verify full nightly version string is parsed correctly."""
        info = parse_nightly_version("4.22.0-0.nightly-2026-01-05-203335")
        assert info.is_nightly is True
        assert info.major_version == "4.22"
        assert info.nightly_date == datetime(2026, 1, 5, 20, 33, 35)
        assert info.full_version == "4.22.0-0.nightly-2026-01-05-203335"

    def test_valid_ga_two_part(self):
        """Verify two-part GA version is parsed correctly."""
        info = parse_nightly_version("4.17")
        assert info.is_nightly is False
        assert info.major_version == "4.17"
        assert info.nightly_date is None

    def test_valid_ga_three_part(self):
        """Verify three-part GA version is parsed correctly."""
        info = parse_nightly_version("4.17.0")
        assert info.is_nightly is False
        assert info.major_version == "4.17"

    def test_invalid_format(self):
        """Verify ValueError on invalid version string."""
        with pytest.raises(ValueError, match="Invalid version format"):
            parse_nightly_version("not-a-version")

    def test_whitespace_trimmed(self):
        """Verify leading/trailing whitespace is trimmed."""
        info = parse_nightly_version("  4.17  ")
        assert info.major_version == "4.17"


class TestParseTimestamp:
    """Tests for parse_timestamp converter."""

    def test_unix_int(self):
        """Verify integer unix timestamp is parsed."""
        dt = parse_timestamp(1704067200)
        assert dt is not None
        assert dt.year == 2024

    def test_unix_float(self):
        """Verify float unix timestamp is parsed."""
        dt = parse_timestamp(1704067200.5)
        assert dt is not None

    def test_unix_string(self):
        """Verify string unix timestamp is parsed."""
        dt = parse_timestamp("1704067200")
        assert dt is not None

    def test_iso_string(self):
        """Verify ISO format string is parsed."""
        dt = parse_timestamp("2025-06-17T10:30:00Z")
        assert dt is not None
        assert dt.month == 6

    def test_none_returns_none(self):
        """Verify None input returns None."""
        assert parse_timestamp(None) is None

    def test_garbage_returns_none(self):
        """Verify garbage input returns None."""
        assert parse_timestamp("not-a-timestamp") is None


class TestFilterDataByTimestamp:
    """Tests for filter_data_by_timestamp filter."""

    def test_filters_after_cutoff(self):
        """Verify only entries after cutoff are returned."""
        cutoff = datetime(2025, 6, 15)
        data = [
            {"timestamp": datetime(2025, 6, 10).timestamp()},
            {"timestamp": datetime(2025, 6, 20).timestamp()},
        ]
        result = filter_data_by_timestamp(data, cutoff)
        assert len(result) == 1

    def test_includes_exact_cutoff(self):
        """Verify exact cutoff timestamp is included."""
        cutoff = datetime(2025, 6, 15)
        data = [{"timestamp": cutoff.timestamp()}]
        result = filter_data_by_timestamp(data, cutoff)
        assert len(result) == 1

    def test_empty_data(self):
        """Verify empty input returns empty list."""
        assert not filter_data_by_timestamp([], datetime(2025, 1, 1))

    def test_missing_timestamp_field(self):
        """Verify entries without timestamp are excluded."""
        data = [{"no_timestamp": True}]
        result = filter_data_by_timestamp(data, datetime(2025, 1, 1))
        assert not result


class TestValidateConfigName:
    """Tests for validate_config_name validator."""

    def test_valid_name(self):
        """Verify valid config name passes through unchanged."""
        assert validate_config_name("small-scale-udn-l3.yaml") == "small-scale-udn-l3.yaml"

    def test_rejects_path_traversal(self):
        """Verify path traversal is rejected."""
        with pytest.raises(ValueError, match="Invalid config name"):
            validate_config_name("../../../etc/passwd")

    def test_rejects_absolute_path(self):
        """Verify absolute path is rejected."""
        with pytest.raises(ValueError, match="Invalid config name"):
            validate_config_name("/etc/passwd")

    def test_rejects_subdirectory(self):
        """Verify subdirectory path is rejected."""
        with pytest.raises(ValueError, match="Invalid config name"):
            validate_config_name("subdir/config.yaml")

    def test_rejects_dotdot_in_name(self):
        """Verify dotdot prefix in name is rejected."""
        with pytest.raises(ValueError, match="Invalid config name"):
            validate_config_name("..config.yaml")


class TestSafeJsonLoads:
    """Tests for safe_json_loads parser."""

    def test_clean_json_array(self):
        """Verify clean JSON array is parsed."""
        result = safe_json_loads('[{"key": "value"}]')
        assert result == [{"key": "value"}]

    def test_clean_json_object(self):
        """Verify clean JSON object is parsed."""
        result = safe_json_loads('{"key": "value"}')
        assert result == {"key": "value"}

    def test_json_with_prefix(self):
        """Verify JSON with log prefix is parsed."""
        result = safe_json_loads('INFO: some log\n[{"key": "value"}]')
        assert result == [{"key": "value"}]

    def test_json_with_suffix(self):
        """Verify JSON with trailing text is parsed."""
        result = safe_json_loads('[{"key": "value"}]\nDone.')
        assert result == [{"key": "value"}]

    def test_json_with_prefix_and_suffix(self):
        """Verify JSON with both prefix and suffix is parsed."""
        result = safe_json_loads('LOG [{"a": 1}] trailing')
        assert result == [{"a": 1}]

    def test_no_json_raises(self):
        """Verify JSONDecodeError when no JSON is present."""
        with pytest.raises(json.JSONDecodeError):
            safe_json_loads("no json here")
