"""Orion MCP server entry point using FastMCP with FileSystemProvider."""
import os
import sys
from pathlib import Path

from fastmcp import FastMCP
from fastmcp.server.providers import FileSystemProvider

provider = FileSystemProvider(Path(__file__).parent / "components")

mcp = FastMCP(name="orion-mcp", providers=[provider])


if __name__ == "__main__":
    if os.getenv("ES_SERVER") is None:
        print("ES_SERVER environment variable is not set")
        sys.exit(1)
    TRANSPORT = "streamable-http"
    mcp.run(transport=TRANSPORT, host="0.0.0.0", port=3030)
