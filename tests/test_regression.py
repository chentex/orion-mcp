import json

import pytest

from components.tools.regression import _extract_regression_details


class TestExtractRegressionDetails:
    def test_no_changepoints(self):
        data = [
            {"is_changepoint": False, "metrics": {}, "ocpVersion": "4.19.1"},
        ]
        assert _extract_regression_details(json.dumps(data)) == []

    def test_single_changepoint(self):
        data = [
            {"is_changepoint": False, "ocpVersion": "4.19.0", "prs": ["PR1"]},
            {
                "is_changepoint": True,
                "ocpVersion": "4.19.1",
                "buildUrl": "https://prow.ci/build/123",
                "prs": ["PR1", "PR2"],
                "metrics": {
                    "podReadyLatency_P99": {"percentage_change": 15.5},
                    "ovnCPU_avg": {"percentage_change": -3.2},
                },
            },
        ]
        details = _extract_regression_details(json.dumps(data))
        assert len(details) == 1
        d = details[0]
        assert d["ocpVersion"] == "4.19.1"
        assert d["previousOcpVersion"] == "4.19.0"
        assert d["buildUrl"] == "https://prow.ci/build/123"
        assert d["prs_added"] == ["PR2"]
        assert any("podReadyLatency_P99 increased" in m for m in d["metrics"])
        assert any("ovnCPU_avg decreased" in m for m in d["metrics"])

    def test_zero_percentage_change_excluded(self):
        data = [
            {"is_changepoint": False, "ocpVersion": "4.19.0"},
            {
                "is_changepoint": True,
                "ocpVersion": "4.19.1",
                "metrics": {
                    "stable_metric": {"percentage_change": 0},
                    "changed_metric": {"percentage_change": 10.0},
                },
            },
        ]
        details = _extract_regression_details(json.dumps(data))
        assert len(details) == 1
        assert len(details[0]["metrics"]) == 1
        assert "changed_metric" in details[0]["metrics"][0]

    def test_non_list_returns_empty(self):
        assert _extract_regression_details(json.dumps({"not": "a list"})) == []

    def test_first_entry_changepoint_has_no_previous(self):
        data = [
            {
                "is_changepoint": True,
                "ocpVersion": "4.19.0",
                "metrics": {"m": {"percentage_change": 5.0}},
            },
        ]
        details = _extract_regression_details(json.dumps(data))
        assert len(details) == 1
        assert details[0]["previousOcpVersion"] is None

    def test_multiple_changepoints(self):
        data = [
            {"is_changepoint": False, "ocpVersion": "4.19.0"},
            {"is_changepoint": True, "ocpVersion": "4.19.1", "metrics": {"m": {"percentage_change": 5.0}}},
            {"is_changepoint": False, "ocpVersion": "4.19.2"},
            {"is_changepoint": True, "ocpVersion": "4.19.3", "metrics": {"m": {"percentage_change": -2.0}}},
        ]
        details = _extract_regression_details(json.dumps(data))
        assert len(details) == 2
        assert details[0]["ocpVersion"] == "4.19.1"
        assert details[1]["ocpVersion"] == "4.19.3"
        assert details[1]["previousOcpVersion"] == "4.19.2"

    def test_null_prs_handled(self):
        data = [
            {"is_changepoint": False, "ocpVersion": "4.19.0", "prs": None},
            {"is_changepoint": True, "ocpVersion": "4.19.1", "prs": None, "metrics": {}},
        ]
        details = _extract_regression_details(json.dumps(data))
        assert len(details) == 1
        assert details[0]["prs_added"] == []
