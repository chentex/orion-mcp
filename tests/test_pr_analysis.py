"""Tests for PR analysis percentage change calculations."""
from components.tools.pr_analysis import _add_percentage_changes


class TestAddPercentageChanges:
    """Tests for _add_percentage_changes helper."""

    def test_calculates_percentage(self):
        """Verify correct percentage change calculation."""
        pulls = [{"data": [{"metrics": {"latency": {"value": 110}}}]}]
        periodic = {"latency": {"value": 100}}
        _add_percentage_changes(pulls, periodic)
        assert pulls[0]["data"][0]["metrics"]["latency"]["percentage_change"] == 10.0

    def test_missing_periodic_metric(self):
        """Verify None when periodic metric is missing."""
        pulls = [{"data": [{"metrics": {"latency": {"value": 110}}}]}]
        periodic = {}
        _add_percentage_changes(pulls, periodic)
        assert pulls[0]["data"][0]["metrics"]["latency"]["percentage_change"] is None

    def test_zero_periodic_value(self):
        """Verify None when periodic value is zero."""
        pulls = [{"data": [{"metrics": {"latency": {"value": 110}}}]}]
        periodic = {"latency": {"value": 0}}
        _add_percentage_changes(pulls, periodic)
        assert pulls[0]["data"][0]["metrics"]["latency"]["percentage_change"] is None

    def test_periodic_scalar_value(self):
        """Verify calculation with scalar periodic value."""
        pulls = [{"data": [{"metrics": {"latency": {"value": 50}}}]}]
        periodic = {"latency": 100}
        _add_percentage_changes(pulls, periodic)
        assert pulls[0]["data"][0]["metrics"]["latency"]["percentage_change"] == -50.0

    def test_non_numeric_values(self):
        """Verify None when pull value is non-numeric."""
        pulls = [{"data": [{"metrics": {"latency": {"value": "bad"}}}]}]
        periodic = {"latency": {"value": 100}}
        _add_percentage_changes(pulls, periodic)
        assert pulls[0]["data"][0]["metrics"]["latency"]["percentage_change"] is None
