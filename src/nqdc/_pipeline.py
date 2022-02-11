"""Pipeline: chain processing steps (used for the nqdc run command)."""
import argparse
from pathlib import Path
from typing import Sequence, Mapping, Tuple, Dict, Optional

from nqdc._typing import BaseProcessingStep, ArgparseActions
from nqdc import _utils

_STEP_NAME = "run"
_STEP_DESCRIPTION = "Run full nqdc pipeline."


_FULL_PIPELINE_DESCRIPTION = (
    "Download and process full-text articles from PubMed Central "
    "for the given query. Articles are downloaded and stored in "
    "individual files. Then, their text and stereotactic coordinates "
    "are extracted and stored in csv files. Finally, the text is "
    "vectorized by computing word counts and TFIDF features."
)


class Pipeline(BaseProcessingStep):
    """Chaining several processing steps."""

    name = _STEP_NAME
    short_description = _STEP_DESCRIPTION

    def __init__(self, steps: Sequence[BaseProcessingStep]):
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
        previous_steps_output: Mapping[str, Path],
    ) -> Tuple[Optional[Path], int]:
        total_code = 0
        outputs: Dict[str, Path] = {}
        for step in self.steps:
            step_output, code = step.run(args, outputs)
            if step_output is not None:
                outputs[step.name] = step_output
            total_code = max(total_code, code)
        return None, total_code
