"""Tool for listing available Orion benchmark config files."""

from fastmcp.tools import tool

from utils.utils import list_orion_configs, orion_configs

ORION_CONFIGS = list_orion_configs()


@tool
def get_orion_configs() -> list[str]:
    """List all available benchmark config files. Use when a user asks "what benchmarks exist", "list configs", or "what workloads can I test".

    Triggers: "what configs are available", "list benchmarks", "show all workloads".

    Returns:
        List of config filenames (e.g. ['cluster-density.yaml', 'node-density.yaml', ...]).
    """
    return orion_configs(ORION_CONFIGS)
