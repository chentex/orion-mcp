from datetime import datetime

import pytest

from utils.utils import (
    parse_nightly_version,
    parse_timestamp,
    filter_data_by_timestamp,
)


class TestParseNightlyVersion:
    def test_valid_nightly(self):
        info = parse_nightly_version("4.22.0-0.nightly-2026-01-05-203335")
        assert info.is_nightly is True
        assert info.major_version == "4.22"
        assert info.nightly_date == datetime(2026, 1, 5, 20, 33, 35)
        assert info.full_version == "4.22.0-0.nightly-2026-01-05-203335"

    def test_valid_ga_two_part(self):
        info = parse_nightly_version("4.17")
        assert info.is_nightly is False
        assert info.major_version == "4.17"
        assert info.nightly_date is None

    def test_valid_ga_three_part(self):
        info = parse_nightly_version("4.17.0")
        assert info.is_nightly is False
        assert info.major_version == "4.17"

    def test_invalid_format(self):
        with pytest.raises(ValueError, match="Invalid version format"):
            parse_nightly_version("not-a-version")

    def test_whitespace_trimmed(self):
        info = parse_nightly_version("  4.17  ")
        assert info.major_version == "4.17"


class TestParseTimestamp:
    def test_unix_int(self):
        dt = parse_timestamp(1704067200)
        assert dt is not None
        assert dt.year == 2024

    def test_unix_float(self):
        dt = parse_timestamp(1704067200.5)
        assert dt is not None

    def test_unix_string(self):
        dt = parse_timestamp("1704067200")
        assert dt is not None

    def test_iso_string(self):
        dt = parse_timestamp("2025-06-17T10:30:00Z")
        assert dt is not None
        assert dt.month == 6

    def test_none_returns_none(self):
        assert parse_timestamp(None) is None

    def test_garbage_returns_none(self):
        assert parse_timestamp("not-a-timestamp") is None


class TestFilterDataByTimestamp:
    def test_filters_after_cutoff(self):
        cutoff = datetime(2025, 6, 15)
        data = [
            {"timestamp": datetime(2025, 6, 10).timestamp()},
            {"timestamp": datetime(2025, 6, 20).timestamp()},
        ]
        result = filter_data_by_timestamp(data, cutoff)
        assert len(result) == 1

    def test_includes_exact_cutoff(self):
        cutoff = datetime(2025, 6, 15)
        data = [{"timestamp": cutoff.timestamp()}]
        result = filter_data_by_timestamp(data, cutoff)
        assert len(result) == 1

    def test_empty_data(self):
        assert filter_data_by_timestamp([], datetime(2025, 1, 1)) == []

    def test_missing_timestamp_field(self):
        data = [{"no_timestamp": True}]
        result = filter_data_by_timestamp(data, datetime(2025, 1, 1))
        assert result == []
