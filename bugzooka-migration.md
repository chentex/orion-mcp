# BugZooka Migration Guide — New orion-mcp Compatibility

## Approach

BugZooka is deterministic Python (not LLM-driven). Keep existing behavior, same defaults, same user-facing Slack output. Just swap MCP tool calls to work with new orion-mcp.

BugZooka uses `trt-external-payload-*.yaml` configs which have **hardcoded metadata** (platform=AWS, workerNodesCount=6, etc.). These don't need `input_vars`. No changes to config lists needed.

---

## Compatibility Matrix (Verified)

| # | Tool | BugZooka file | Params match? | Response match? | Status |
|---|------|--------------|--------------|-----------------|--------|
| 1 | `get_orion_configs` | `perf_summary_analyzer.py` | Yes | Yes | OK |
| 2 | `get_orion_metrics_with_meta` | `perf_summary_analyzer.py` | Yes (`input_vars` optional, defaults to `""`) | Yes | OK |
| 3 | `get_orion_metrics` | `perf_summary_analyzer.py` | Yes (`input_vars` optional, defaults to `""`) | Yes | OK |
| 4 | `get_orion_performance_data` | `perf_summary_analyzer.py` | **REMOVED from orion-mcp** | N/A | BREAKING |
| 5 | `openshift_report_on` (fallback) | `perf_summary_analyzer.py` | **NO** — `options` param removed | **NO** — response shape changed | BREAKING |
| 6 | `has_nightly_regressed` | `nightly_regression_analyzer.py` | **NO** — passes `configs`, expects `config_name` | Yes (returns str) | BREAKING |
| 7 | `openshift_report_on_pr` | `pr_analyzer.py` (LLM agentic) | Yes (`config_name`/`input_vars` optional) | Mostly — prompt says `pulls`, actual is `runs` | MINOR |

---

## Change 1: `perf_summary_analyzer.py` — Replace data fetching with `get_performance_summary`

### What breaks

**Primary path (line 496):** Calls `get_orion_performance_data` — tool removed from orion-mcp.

```python
# FAILS — tool no longer exists
result = await _call_mcp_tool(
    "get_orion_performance_data",
    {"config_name": config, "metric": metric, "version": version, "lookback": str(lookback)},
)
```

**Fallback path (line 511):** Calls `openshift_report_on` with `options="json"` — param removed.

```python
# FAILS — "options" param does not exist in new orion-mcp
result = await _call_mcp_tool(
    "openshift_report_on",
    {"versions": version, "metric": metric, "config_name": config, "lookback": str(lookback), "options": "json"},
)
```

**Response parsing (lines 531-560):** Checks `result["values"]` and `result["data"][version][metric]["value"]`. New `openshift_report_on` returns `result["versions"][version]["values"]` — neither key matches. All metrics show "n/a".

### Fix

Replace the inner loop in `analyze_performance()` (lines 737-820) with a single `get_performance_summary` call. This tool does everything BugZooka computes manually (get metrics, get values, compute change% vs prior period) server-side.

```python
# BEFORE (lines 737-820): N configs × M metrics × 2 periods = ~100 MCP calls
for cfg in configs:
    for ver in versions:
        metrics, meta_map = await get_metrics(cfg, ver)
        for metric in metrics:
            this_period_data = await get_performance_data(config=cfg, metric=metric, version=ver, lookback=lookback_days)
            two_period_data = await get_performance_data(config=cfg, metric=metric, version=ver, lookback=lookback_window)
            # compute stats, build row...

# AFTER: 1 MCP call per version, all configs comma-joined
for ver in versions:
    result = await _call_mcp_tool(
        "get_performance_summary",
        {
            "config_name": ",".join(configs),
            "version": ver,
            "lookback": lookback_days,
            # NO input_vars needed — trt configs have hardcoded metadata
        },
    )
    for config_result in result.get("results", []):
        cfg = config_result["config"]
        if not config_result.get("success"):
            result_parts.append(f"Could not obtain data for {cfg} (version {ver})")
            continue
        rows = []
        for m in config_result.get("metrics", []):
            rows.append({
                "config": cfg,
                "metric": m["name"],
                "runs": m["runs"],
                "min": m["min"],
                "max": m["max"],
                "avg": m["avg"],
                "change": m["change_percent"],
                "meta": {"direction": m.get("direction"), "threshold": m.get("threshold")},
            })
        if rows:
            result_parts.extend(
                _split_metrics_table_for_slack(
                    title=f"*Config: {cfg}*",
                    version=ver,
                    rows=sorted(rows, key=_change_sort_key, reverse=True),
                    total_metrics=len(config_result.get("metrics", [])),
                    lookback_days=lookback_days,
                    include_config=False,
                    note_prefix="showing",
                )
            )
```

### `get_performance_summary` response format

```json
{
  "success": true,
  "results": [
    {
      "config": "trt-external-payload-cluster-density.yaml",
      "success": true,
      "metrics": [
        {
          "name": "podReadyLatency_P99",
          "runs": 12,
          "min": 13000.5,
          "max": 15200.3,
          "avg": 14100.2,
          "change_percent": 3.45,
          "direction": 1,
          "threshold": 15
        }
      ]
    }
  ]
}
```

Maps directly to BugZooka's existing row format: `name`→`metric`, `runs`→`runs`, `min`/`max`/`avg` same, `change_percent`→`change`, `direction`+`threshold`→`meta`.

### What to remove after this fix

These are no longer needed since `get_performance_summary` does the work server-side:

| What | Lines | Why |
|------|-------|-----|
| `get_performance_data()` function | 475-586 | Replaced by `get_performance_summary` |
| `PerformanceData` dataclass | 80-92 | Response already has stats |
| `_calculate_stats()` | 113-118 | Done server-side |
| `_calculate_percentage_change()` | 127-144 | Done server-side |
| `_is_no_data_fetch_result()` | 371-378 | No longer needed |
| `get_metrics()` function | 409-472 | Not needed when using `get_performance_summary` |

### What stays unchanged

| What | Why |
|------|-----|
| `_DEFAULT_CONTROL_PLANE_CONFIGS` (lines 20-28) | BugZooka's default config list — keep as-is |
| `_ALL_CONFIGS_FALLBACK` (lines 31-71) | Fallback when `get_orion_configs` fails — keep as-is |
| `parse_perf_summary_args()` (lines 589-664) | User message parsing — no change |
| Config selection logic (lines 702-721) | Same logic: user-specified / default / all |
| `get_configs()` (line 395) | Calls `get_orion_configs` — tool unchanged |
| All Slack formatting functions | `_format_metrics_table`, `_split_metrics_table_for_slack`, `_render_table`, `_change_hint`, etc. — no change |
| `_coerce_mcp_result()` | MCP result coercion — no change |
| `_call_mcp_tool()` | MCP tool caller — no change |

---

## Change 2: `nightly_regression_analyzer.py` — Fix param name

### What breaks

**Line 174:** Passes `configs` but new orion-mcp expects `config_name`.

```python
# CURRENT — config silently ignored by orion-mcp
if parsed.config:
    tool_args["configs"] = parsed.config
```

When a user says `inspect <nightly> for config node-density.yaml`, the config is sent under wrong key and silently dropped. orion-mcp falls back to default config.

### Fix

```python
# FIX — one line change
if parsed.config:
    tool_args["config_name"] = parsed.config
```

### Everything else in this file works as-is

| Param | BugZooka passes | orion-mcp expects | Match |
|-------|----------------|-------------------|-------|
| `nightly_version` | `parsed.nightly_version` | `nightly_version: str` (required) | Yes |
| `lookback` | `parsed.lookback_days` (str) | `lookback: str("15")` | Yes |
| `previous_nightly` | `parsed.previous_nightly` (conditional) | `previous_nightly: str("")` | Yes |
| `config_name` | **missing** (sent as `configs`) | `config_name: str\|None(None)` | **Fix above** |
| `input_vars` | not passed | `input_vars: str("")` | OK — defaults to empty, trt configs don't need it |

Response: orion-mcp returns `str`, BugZooka wraps it in Slack message. No change needed.

---

## Change 3: `pr_analyzer.py` — No code change needed

PR analyzer uses Gemini agentic loop — the LLM calls `openshift_report_on_pr` directly via tool-calling.

| Param | Old orion-mcp | New orion-mcp | Impact |
|-------|-------------|---------------|--------|
| `version` | `str("4.20")` | `str("4.20")` | Same |
| `lookback` | `str("15")` | `str("15")` | Same |
| `organization` | `str("openshift")` | `str("openshift")` | Same |
| `repository` | `str("ovn-kubernetes")` | `str("ovn-kubernetes")` | Same |
| `pull_request` | `str("2841")` | `str("2841")` | Same |
| `pull_requests` | `str("")` | `str("")` | Same |
| `config_name` | did not exist | `str\|None(None)` | New optional param, defaults to None |
| `input_vars` | did not exist | `str("")` | New optional param, defaults to empty |

New `config_name` and `input_vars` params are optional with defaults. LLM won't pass them → tool falls back to internal resolution using hardcoded TRT configs. Works as-is.

Return type changed from `dict` to `list[dict]`. Since the LLM interprets the response (not code-parsed), it adapts. No code change needed.

---

## Change 4: `prompts.py` — Update PR prompt

### What's wrong

**Line 87:** Prompt tells LLM the tool returns results "under the `pulls` key":

```python
"The tool returns results for each PR under the `pulls` key"
```

Actual response uses `runs` key, not `pulls`:

```json
[{"config": "...", "runs": [{"pull_number": "3169", "metrics": [...]}]}]
```

### Fix

```python
# BEFORE
"The tool returns results for each PR under the `pulls` key"

# AFTER  
"The tool returns a list of config entries, each with a `runs` key containing per-PR results"
```

### Also update config path reference

**Line 115:**

```python
# BEFORE
"- Transform config name to readable format: \"/orion/examples/trt-external-payload-cluster-density.yaml\" → \"cluster-density\""

# AFTER
"- Transform config name to readable format: \"trt-external-payload-cluster-density.yaml\" → \"cluster-density\""
```

---

## Full Change Summary

| # | File | Change | Effort |
|---|------|--------|--------|
| 1 | `perf_summary_analyzer.py` | Replace `analyze_performance()` inner loop (lines 737-820) with `get_performance_summary` call | ~80 lines removed, ~30 added |
| 2 | `perf_summary_analyzer.py` | Remove dead code: `get_performance_data()`, `PerformanceData`, `_calculate_stats()`, `_calculate_percentage_change()`, `_is_no_data_fetch_result()`, `get_metrics()` | ~180 lines removed |
| 3 | `nightly_regression_analyzer.py:174` | `tool_args["configs"]` → `tool_args["config_name"]` | 1 line |
| 4 | `prompts.py:87` | `"pulls" key` → `"runs" key` in prompt text | 1 line |
| 5 | `prompts.py:115` | Remove `/orion/examples/` prefix from config path example | 1 line |

### No changes needed

| File | Why |
|------|-----|
| `pr_analyzer.py` | Tool params backward-compatible, LLM adapts to response |
| `mcp_client.py` | Generic MCP plumbing, no tool-specific code |
| `prow_analyzer.py` | Doesn't call orion-mcp tools |
| `log_analyzer.py` | Doesn't call orion-mcp tools |
| Hardcoded config lists | trt configs have baked-in metadata, no `input_vars` needed |
