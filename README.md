# Orion MCP

[![License](https://img.shields.io/github/license/jtaleric/orion-mcp)](LICENSE)
[![Python Version](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)  
Orion MCP is a Model Context Protocol (MCP) server for performance regression analysis powered by the [cloud-bulldozer/orion](https://github.com/cloud-bulldozer/orion) library.

---

## Key Features

* **Regression Detection** – Automatically detects performance regressions in OpenShift & Kubernetes clusters.
* **Interactive MCP API** – Exposes a set of composable tools & resources that can be consumed via HTTP or by other MCP agents.
* **Visual Reporting** – Generates publication-ready plots (PNG/JPEG) for trends, multi-version comparisons and metric correlations.
* **Container-first** – Ships with a lightweight OCI image and an example OpenShift deployment manifest.

---

## Table of Contents

1. [Getting Started](#getting-started)
2. [Quick Start](#quick-start)
3. [Available Tools](#available-tools)
4. [Deployment](#deployment)
5. [Development](#development)
6. [Contributing](#contributing)
7. [License](#license)

---

## Getting Started

### Prerequisites

* **Python** 3.11 or newer
* An **OpenSearch** (or Elasticsearch ≥7.17) endpoint with Orion-indexed benchmark results
* **Podman** or **Docker** (optional – for containerised execution)

### Installation (virtual-env)

```bash
# Clone repository
$ git clone https://github.com/YOUR_ORG/orion-mcp.git && cd orion-mcp

# Create & activate a virtual environment
$ python3.11 -m venv .venv
$ source .venv/bin/activate

# Install Python dependencies
$ pip install -r requirements.txt
```

---

## Quick Start

Set the data-source endpoint and launch the server locally:

```bash
export ES_SERVER="https://opensearch.example.com:9200"
python orion_mcp.py  # listens on 0.0.0.0:3030 by default
```

---

## Available Tools

### Discovery
| Tool | Description |
|------|-------------|
| `discover_jobs` | Find CI perf jobs by version/platform/workload/scale with pre-resolved Orion configs |
| `get_orion_configs` | List available Orion configuration files |
| `get_orion_metrics` | List metric names for a config (live ES query) |
| `get_orion_metrics_with_meta` | List metrics with direction, threshold, and labels from config YAML |
| `get_release_date` | Get GA release date for a version |

### Regression Detection
| Tool | Description |
|------|-------------|
| `has_openshift_regressed` | Changepoint detection across one or more configs (comma-separated) |
| `has_networking_regressed` | Same as above, scoped to networking configs |
| `has_nightly_regressed` | Check a specific nightly build for regressions |

### Analysis
| Tool | Description |
|------|-------------|
| `openshift_report_on` | Per-run metric values across one or more versions |
| `get_performance_summary` | Health check — aggregated stats (min/max/avg/change%) across all metrics, supports comma-separated configs |
| `metrics_correlation` | Correlate two metrics with scatter plot |
| `openshift_report_on_pr` | Compare PR performance against periodic baseline |

All tools take explicit `config_name` and `input_vars`. Use `discover_jobs` to resolve these from ES metadata.

---

## Deployment

### Container Image

```bash
podman build -t quay.io/YOUR_ORG/orion-mcp:latest .
```

### OpenShift

To deploy to an OpenShift cluster, specify the ES_SERVER in kustomize/base/.env, e.g.:

```bash
ES_SERVER=https://USER:PASSWORD@SERVER:443
```

To deploy the application:

```bash
# Expose your quay credentials to fetch the container image
export QUAY_CRED='<base64 encoded pull secret>'

# Build and apply the manifests
kustomize build --load-restrictor=LoadRestrictionsNone ./kustomize/base | envsubst | oc apply -f -
```

To verify any changes to manifests, you can render them locally, e.g.:
```bash
kustomize build  ./kustomize/base | envsubst > manifests.yaml 
```

To access the service externally, expose it using an **OpenShift Route** and point your MCP client to `http://<host>:3030`.

---

## Development

```bash
# Run linters & tests
flake8
pytest

# Auto-format with black & isort
black . && isort .
```

---

## Contributing

Pull requests are very welcome! Please ensure you have read and adhere to the [Code of Conduct](https://www.contributor-covenant.org/version/2/1/code_of_conduct/).

1. Fork the repository
2. Create a new branch for your feature or bugfix
3. Make your changes and add tests if applicable
4. Submit a pull request with a clear description of your changes


---

## License

Orion-MCP is distributed under the **Apache 2.0** License. See the [LICENSE](LICENSE) file for full text.

