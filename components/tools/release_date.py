"""Tool for looking up OpenShift version release dates."""
from typing import Annotated

from pydantic import Field
from fastmcp.tools import tool

from utils.constants import RELEASE_DATES


@tool
async def get_release_date(
    version: Annotated[str, Field(description="OCP Version to get Release date")] = "4.20",
) -> str:
    """Get the release date for a given OpenShift version."""
    if version in RELEASE_DATES:
        return RELEASE_DATES[version]
    return f"Invalid version: {version}"
