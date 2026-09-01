from datetime import datetime

from utils.config_parser import _metric_key, _timestamp_after


class TestMetricKey:
    def test_name_with_agg_type(self):
        metric = {"name": "cpu", "agg": {"agg_type": "avg"}}
        assert _metric_key(metric) == "cpu_avg"

    def test_name_with_empty_agg_type(self):
        metric = {"name": "cpu", "agg": {"agg_type": ""}}
        assert _metric_key(metric) == "cpu_value"

    def test_name_with_metric_of_interest(self):
        metric = {"name": "latency", "metric_of_interest": "P99"}
        assert _metric_key(metric) == "latency_P99"

    def test_name_only(self):
        metric = {"name": "throughput"}
        assert _metric_key(metric) == "throughput_value"

    def test_missing_name(self):
        metric = {}
        assert _metric_key(metric) == "unknown_value"

    def test_agg_not_dict(self):
        metric = {"name": "cpu", "agg": "avg"}
        assert _metric_key(metric) == "cpu_value"


class TestTimestampAfter:
    def test_unix_timestamp_after(self):
        cutoff = datetime(2025, 6, 1)
        ts = datetime(2025, 7, 1).timestamp()
        assert _timestamp_after(ts, cutoff) is True

    def test_unix_timestamp_before(self):
        cutoff = datetime(2025, 6, 1)
        ts = datetime(2025, 5, 1).timestamp()
        assert _timestamp_after(ts, cutoff) is False

    def test_unix_timestamp_equal(self):
        cutoff = datetime(2025, 6, 1)
        ts = cutoff.timestamp()
        assert _timestamp_after(ts, cutoff) is False

    def test_none_timestamp(self):
        cutoff = datetime(2025, 6, 1)
        assert _timestamp_after(None, cutoff) is False

    def test_iso_string_after(self):
        cutoff = datetime(2025, 6, 1)
        assert _timestamp_after("2025-07-01T00:00:00", cutoff) is True
