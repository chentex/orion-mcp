import json

from components.tools.regression import _extract_regression_details


class TestExtractRegressionDetails:
    def test_no_changepoints(self):
        data = [{"is_changepoint": False, "metrics": {}}]
        assert _extract_regression_details(json.dumps(data)) == []

    def test_single_changepoint(self):
        data = [
            {"ocpVersion": "4.18", "prs": ["PR1"], "is_changepoint": False, "metrics": {}},
            {
                "uuid": "abc123",
                "ocpVersion": "4.19",
                "is_changepoint": True,
                "prs": ["PR1", "PR2"],
                "metrics": {
                    "latency": {"percentage_change": 15.5},
                    "cpu": {"percentage_change": -3.2},
                },
            },
        ]
        result = _extract_regression_details(json.dumps(data))
        assert len(result) == 1
        assert result[0]["uuid"] == "abc123"
        assert result[0]["ocpVersion"] == "4.19"
        assert result[0]["previousOcpVersion"] == "4.18"
        assert result[0]["prs_added"] == ["PR2"]
        assert any("increased" in m for m in result[0]["metrics"])
        assert any("decreased" in m for m in result[0]["metrics"])

    def test_first_entry_changepoint(self):
        data = [
            {
                "uuid": "first",
                "ocpVersion": "4.19",
                "is_changepoint": True,
                "prs": ["PR1"],
                "metrics": {},
            },
        ]
        result = _extract_regression_details(json.dumps(data))
        assert len(result) == 1
        assert result[0]["previousOcpVersion"] is None
        assert result[0]["prs_added"] == ["PR1"]
