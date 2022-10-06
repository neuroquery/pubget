"""Utilities for loading pubget plug-in functionality."""

from typing import Any, Dict, List

import importlib_metadata


def get_plugin_actions() -> Dict[str, List[Any]]:
    """Load entry points from all pubget plugins.

    See https://setuptools.pypa.io/en/latest/userguide/entry_point.html
    """
    all_actions: Dict[str, List[Any]] = {
        "pipeline_steps": [],
        "commands": [],
    }
    for entry_point in importlib_metadata.entry_points().select(
        group="pubget.plugin_actions"
    ):
        plugin_actions = entry_point.load()()
        for kind, steps in all_actions.items():
            steps.extend(plugin_actions.get(kind, []))
    return all_actions
