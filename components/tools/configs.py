"""Tool for listing available Orion configuration files."""
from fastmcp.tools import tool

from utils.utils import orion_configs, list_orion_configs
from utils.constants import DEFAULT_ORION_CONFIGS

_configs = list_orion_configs()
ORION_CONFIGS = _configs if _configs else DEFAULT_ORION_CONFIGS


@tool
def get_orion_configs() -> list[str]:
    """Return the list of Orion config filenames (not full paths)."""
    return orion_configs(ORION_CONFIGS)
