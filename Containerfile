# ============================================================
# Stage 1: Build stage - compile and install dependencies
# ============================================================
FROM python:3.11.8-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    gcc \
    build-essential \
    curl \
    jq \
    && rm -rf /var/lib/apt/lists/*

# Clone Orion repository: use ORION_RELEASE_TAG if set, else fetch latest release from GitHub API.
ARG ORION_RELEASE_TAG=
ENV ORION_REPO_URL="https://github.com/cloud-bulldozer/orion"
RUN if [ -n "${ORION_RELEASE_TAG}" ]; then \
      LATEST_TAG="${ORION_RELEASE_TAG}"; \
      echo "Using build-arg ORION_RELEASE_TAG=${LATEST_TAG}"; \
    else \
      LATEST_TAG=$(curl -s "https://api.github.com/repos/cloud-bulldozer/orion/releases/latest" | jq -r '.tag_name') || LATEST_TAG="v0.1.5"; \
      echo "Using latest release: ${LATEST_TAG}"; \
    fi && \
    echo "${LATEST_TAG}" > /tmp/orion-version.txt && \
    git clone --depth 1 --branch "${LATEST_TAG}" "${ORION_REPO_URL}" /app/orion-repo

# Create and populate Orion virtual environment
RUN python -m venv /app/orion-venv
RUN /app/orion-venv/bin/pip install --no-cache-dir --upgrade pip setuptools && \
    /app/orion-venv/bin/pip install --no-cache-dir -r /app/orion-repo/requirements.txt && \
    /app/orion-venv/bin/pip install --no-cache-dir /app/orion-repo

# Copy examples
RUN mkdir -p /orion && cp -r /app/orion-repo/examples /orion/examples

# Create orion-mcp virtual environment
RUN python -m venv /app/orion-mcp-venv
COPY requirements.txt /app/orion-mcp/requirements.txt
RUN /app/orion-mcp-venv/bin/pip install --no-cache-dir --upgrade pip && \
    /app/orion-mcp-venv/bin/pip install --no-cache-dir -r /app/orion-mcp/requirements.txt

# ============================================================
# Stage 2: Runtime stage - minimal final image
# ============================================================
FROM python:3.11.8-slim-bookworm

ENV PYTHONUNBUFFERED="1"
ENV ORION_VENV="/app/orion-venv"
ENV ORION_MCP_VENV="/app/orion-mcp-venv"
ENV PATH="/app/orion-mcp-venv/bin:$PATH"

# Copy virtual environments from builder
COPY --from=builder /app/orion-venv /app/orion-venv
COPY --from=builder /app/orion-mcp-venv /app/orion-mcp-venv
COPY --from=builder /orion/examples /orion/examples
COPY --from=builder /tmp/orion-version.txt /app/orion-version.txt

# Create symlink for orion command
RUN ln -sf /app/orion-venv/bin/orion /usr/local/bin/orion

# Copy orion-mcp source code
COPY . /app/orion-mcp/

# Create wrapper script
RUN printf '%s\n' '#!/bin/sh' '/app/orion-mcp-venv/bin/python /app/orion-mcp/server.py "$@"' \
    > /usr/local/bin/orion-mcp && \
    chmod +x /usr/local/bin/orion-mcp

WORKDIR /app/orion-mcp

CMD ["/app/orion-mcp-venv/bin/python", "server.py"]
