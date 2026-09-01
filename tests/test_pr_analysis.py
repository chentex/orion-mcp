from components.tools.pr_analysis import _add_percentage_changes


class TestAddPercentageChanges:
    def test_calculates_percentage(self):
        pulls = [{"data": [{"metrics": {"latency": {"value": 110}}}]}]
        periodic = {"latency": {"value": 100}}
        _add_percentage_changes(pulls, periodic)
        assert pulls[0]["data"][0]["metrics"]["latency"]["percentage_change"] == 10.0

    def test_missing_periodic_metric(self):
        pulls = [{"data": [{"metrics": {"latency": {"value": 110}}}]}]
        periodic = {}
        _add_percentage_changes(pulls, periodic)
        assert pulls[0]["data"][0]["metrics"]["latency"]["percentage_change"] is None

    def test_zero_periodic_value(self):
        pulls = [{"data": [{"metrics": {"latency": {"value": 110}}}]}]
        periodic = {"latency": {"value": 0}}
        _add_percentage_changes(pulls, periodic)
        assert pulls[0]["data"][0]["metrics"]["latency"]["percentage_change"] is None

    def test_periodic_scalar_value(self):
        pulls = [{"data": [{"metrics": {"latency": {"value": 50}}}]}]
        periodic = {"latency": 100}
        _add_percentage_changes(pulls, periodic)
        assert pulls[0]["data"][0]["metrics"]["latency"]["percentage_change"] == -50.0

    def test_non_numeric_values(self):
        pulls = [{"data": [{"metrics": {"latency": {"value": "bad"}}}]}]
        periodic = {"latency": {"value": 100}}
        _add_percentage_changes(pulls, periodic)
        assert pulls[0]["data"][0]["metrics"]["latency"]["percentage_change"] is None
