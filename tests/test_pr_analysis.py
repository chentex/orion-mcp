from components.tools.pr_analysis import _add_percentage_changes


class TestAddPercentageChanges:
    def test_basic_percentage_calculation(self):
        pulls = [
            {
                "data": [
                    {
                        "metrics": {
                            "podReadyLatency_P99": {"value": 110.0},
                        }
                    }
                ]
            }
        ]
        periodic_avg = {"podReadyLatency_P99": {"value": 100.0}}
        _add_percentage_changes(pulls, periodic_avg)
        assert pulls[0]["data"][0]["metrics"]["podReadyLatency_P99"]["percentage_change"] == 10.0

    def test_decrease(self):
        pulls = [{"data": [{"metrics": {"m": {"value": 80.0}}}]}]
        periodic_avg = {"m": {"value": 100.0}}
        _add_percentage_changes(pulls, periodic_avg)
        assert pulls[0]["data"][0]["metrics"]["m"]["percentage_change"] == -20.0

    def test_missing_metric_in_periodic(self):
        pulls = [{"data": [{"metrics": {"new_metric": {"value": 50.0}}}]}]
        periodic_avg = {}
        _add_percentage_changes(pulls, periodic_avg)
        assert pulls[0]["data"][0]["metrics"]["new_metric"]["percentage_change"] is None

    def test_zero_baseline_returns_none(self):
        pulls = [{"data": [{"metrics": {"m": {"value": 50.0}}}]}]
        periodic_avg = {"m": {"value": 0}}
        _add_percentage_changes(pulls, periodic_avg)
        assert pulls[0]["data"][0]["metrics"]["m"]["percentage_change"] is None

    def test_none_pull_value_returns_none(self):
        pulls = [{"data": [{"metrics": {"m": {"value": None}}}]}]
        periodic_avg = {"m": {"value": 100.0}}
        _add_percentage_changes(pulls, periodic_avg)
        assert pulls[0]["data"][0]["metrics"]["m"]["percentage_change"] is None

    def test_scalar_periodic_avg(self):
        pulls = [{"data": [{"metrics": {"m": {"value": 120.0}}}]}]
        periodic_avg = {"m": 100.0}
        _add_percentage_changes(pulls, periodic_avg)
        assert pulls[0]["data"][0]["metrics"]["m"]["percentage_change"] == 20.0

    def test_multiple_pulls_and_metrics(self):
        pulls = [
            {
                "data": [
                    {
                        "metrics": {
                            "a": {"value": 200.0},
                            "b": {"value": 50.0},
                        }
                    }
                ]
            },
            {
                "data": [
                    {
                        "metrics": {
                            "a": {"value": 100.0},
                        }
                    }
                ]
            },
        ]
        periodic_avg = {"a": {"value": 100.0}, "b": {"value": 100.0}}
        _add_percentage_changes(pulls, periodic_avg)
        assert pulls[0]["data"][0]["metrics"]["a"]["percentage_change"] == 100.0
        assert pulls[0]["data"][0]["metrics"]["b"]["percentage_change"] == -50.0
        assert pulls[1]["data"][0]["metrics"]["a"]["percentage_change"] == 0.0

    def test_empty_pulls_list(self):
        pulls = []
        _add_percentage_changes(pulls, {"m": {"value": 100.0}})
        assert pulls == []

    def test_empty_data_in_pull(self):
        pulls = [{"data": []}]
        _add_percentage_changes(pulls, {"m": {"value": 100.0}})
        assert pulls == [{"data": []}]
