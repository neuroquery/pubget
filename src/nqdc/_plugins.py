"""Utilities for loading nqdc plug-in functionality."""

from typing import Dict, List, Any

import importlib_metadata


def get_plugin_processing_steps() -> Dict[str, List[Any]]:
    """Load entry points from all nqdc plugins.

    See https://setuptools.pypa.io/en/latest/userguide/entry_point.html
    """
    all_steps: Dict[str, List[Any]] = {
        "pipeline_steps": [],
        "standalone_steps": [],
    }
    for entry_point in importlib_metadata.entry_points().select(
        group="nqdc.plugin_processing_steps"
    ):
        plugin_steps = entry_point.load()()
        for kind, steps in all_steps.items():
            steps.extend(plugin_steps.get(kind, []))
    return all_steps
