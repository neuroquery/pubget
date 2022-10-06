"""Pipeline: chain processing steps (used for the pubget run command)."""
import argparse
import logging
from pathlib import Path
from typing import Dict, Sequence

from pubget import _utils
from pubget._typing import (
    ArgparseActions,
    Command,
    ExitCode,
    PipelineStep,
    StopPipeline,
)

_LOG = logging.getLogger(__name__)
_STEP_NAME = "run"
_STEP_DESCRIPTION = "Run full pubget pipeline."


_FULL_PIPELINE_DESCRIPTION = (
    "Download and process full-text articles from PubMed Central "
    "for the given query or list of PMCIDs. Articles are downloaded and "
    "stored in individual files. Then, their text and "
    "stereotactic coordinates are extracted and stored in csv files. "
    "Finally, the text is vectorized by computing word counts "
    "and TFIDF features."
)


class Pipeline(Command):
    """Chaining several processing steps."""

    name = _STEP_NAME
    short_description = _STEP_DESCRIPTION

    def __init__(self, steps: Sequence[PipelineStep]):
        self.steps = steps

    def edit_argument_parser(self, argument_parser: ArgparseActions) -> None:
        _utils.add_n_jobs_argument(argument_parser)
        argument_parser.description = _FULL_PIPELINE_DESCRIPTION
        for step in self.steps:
            step_group = argument_parser.add_argument_group(
                f"{step.name} step", step.short_description
            )
            step.edit_argument_parser(step_group)

    def run(
        self,
        args: argparse.Namespace,
    ) -> ExitCode:
        total_code = ExitCode.COMPLETED
        outputs: Dict[str, Path] = {}
        for step in self.steps:
            try:
                step_output, code = step.run(args, outputs)
            except StopPipeline as stop_pipeline:
                _LOG.error(
                    "Interrupting pubget run after "
                    f"'{step.name}' step: {stop_pipeline.reason}"
                )
                return ExitCode.ERROR
            else:
                if step_output is not None:
                    outputs[step.name] = step_output
                total_code = max(total_code, code)
        return ExitCode(total_code)
