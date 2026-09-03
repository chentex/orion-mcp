"""Orion MCP Server entry point."""

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

from fastmcp import FastMCP
from fastmcp.server.providers import FileSystemProvider

logger = logging.getLogger(__name__)

provider = FileSystemProvider(
    root=Path(__file__).parent / "components",
)

mcp = FastMCP(
    name="orion-mcp",
    providers=[provider],
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Orion MCP Server")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)",
    )
    args = parser.parse_args()

    log_level = getattr(logging, args.log_level)
    logging.basicConfig(level=log_level, format="%(levelname)s:     %(message)s", force=True)

    if os.getenv("ES_SERVER") is None:
        logger.error("ES_SERVER environment variable is not set")
        sys.exit(1)
    TRANSPORT = os.getenv("MCP_TRANSPORT", "streamable-http")
    HOST = os.getenv("MCP_HOST", "0.0.0.0")
    PORT = int(os.getenv("MCP_PORT", "3030"))
    logger.info("Running MCP server with transport: %s", TRANSPORT)
    asyncio.run(mcp.run(transport=TRANSPORT, host=HOST, port=PORT))
