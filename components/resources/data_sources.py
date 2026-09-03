"""MCP resources providing static data (release dates, data source URL)."""

from fastmcp.resources import resource

from utils.constants import RELEASE_DATES
from utils.utils import get_data_source


@resource("orion-mcp://release_dates")
def release_dates_resource() -> dict[str, str]:
    """Provides the release dates for the different OpenShift versions."""
    return RELEASE_DATES


@resource("orion-mcp://get_data_source")
def get_data_source_resource() -> str:
    """Provides the data source URL for Orion analysis.

    User must launch MCP server with the environment variable ES_SERVER
    set to the OpenSearch URL.
    """
    return get_data_source()
