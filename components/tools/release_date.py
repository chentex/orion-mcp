"""Tool for looking up OpenShift release dates."""

from typing import Annotated

from fastmcp.tools import tool
from pydantic import Field

from utils.constants import RELEASE_DATES


@tool
async def get_release_date(
    version: Annotated[str, Field(description="OCP Version to get Release date")] = "4.20",
) -> str:
    """Look up when an OpenShift version was released (GA date). Use when a user asks "when did X release" or "what is the release date for X".

    Triggers: "when did 4.19 release", "release date for 4.20", "when was 5.0 GA".

    Args:
        version: OpenShift version (default: '4.20').

    Returns:
        Release date string or "Invalid version".
    """
    if version in RELEASE_DATES:
        return RELEASE_DATES[version]
    return f"Invalid version: {version}"
