"""Example nqdc plugin: plots the number of articles per publication year."""
import argparse
import logging
from pathlib import Path
from typing import Tuple, Mapping, Optional, Union

import pandas as pd

ArgparseActions = Union[argparse.ArgumentParser, argparse._ArgumentGroup]

_LOG = logging.getLogger(__name__)
_STEP_NAME = "plot_pub_dates"
_STEP_DESCRIPTION = "Example plugin: plot histogram of publication years."


def plot_publication_dates(extracted_data_dir: Path) -> Tuple[Path, int]:
    """Make a bar plot of the number of articles per year.

    Parameters
    ----------
    extracted_data_dir
        The directory containing the articles' metadata. It is a directory
        created by `nqdc.extract_data_to_csv`: it contains a file named
        `metadata.csv`.

    Returns
    -------
    output_dir
        The directory where the plot is stored.
    exit_code
        Always 0, used by nqdc command-line interface.

    """
    output_dir = extracted_data_dir.with_name(
        extracted_data_dir.name.replace(
            "_extractedData", "_examplePluginPubDatesPlot"
        )
    )
    output_dir.mkdir(exist_ok=True)
    meta_data = pd.read_csv(str(extracted_data_dir.joinpath("metadata.csv")))
    min_year, max_year = (
        meta_data["publication_year"].min(),
        meta_data["publication_year"].max(),
    )
    years = list(range(min_year, max_year + 2))
    ax = meta_data["publication_year"].hist(
        bins=years, grid=False, rwidth=0.5, align="left"
    )
    ax.set_xticks(years[:-1])
    ax.set_xlabel("Publication year")
    ax.set_ylabel("Number of articles")
    output_file = output_dir.joinpath("plot.png")
    ax.figure.savefig(str(output_file))
    _LOG.info(f"Publication dates histogram saved in {output_file}.")
    return output_dir, 0


class PlotPubDatesStep:
    """Plot publication dates as part of a pipeline (nqdc run)."""

    name = _STEP_NAME
    short_description = _STEP_DESCRIPTION

    def edit_argument_parser(self, argument_parser: ArgparseActions) -> None:
        argument_parser.add_argument(
            "--plot_pub_dates",
            action="store_true",
            help="Save a histogram plot of publication years of "
            "downloaded articles.",
        )

    def run(
        self,
        args: argparse.Namespace,
        previous_steps_output: Mapping[str, Path],
    ) -> Tuple[Optional[Path], int]:
        if not args.plot_pub_dates:
            return None, 0
        return plot_publication_dates(previous_steps_output["extract_data"])


class StandalonePlotPubDatesStep:
    """Plot publication dates as a standalone step (nqdc plot_pub_dates)."""

    name = _STEP_NAME
    short_description = _STEP_DESCRIPTION

    def edit_argument_parser(self, argument_parser: ArgparseActions) -> None:
        argument_parser.add_argument(
            "extracted_data_dir",
            help="Directory containing extracted data CSV files."
            "It is a directory created by nqdc whose name ends "
            "with 'extractedData'.",
        )

    def run(
        self,
        args: argparse.Namespace,
        previous_steps_output: Mapping[str, Path],
    ) -> Tuple[Path, int]:
        return plot_publication_dates(args.extracted_data_dir)


def get_nqdc_processing_steps():
    """Endpoint used by nqdc.

    Needed to discover the plugin steps and add them to the command-line
    interface. See the `[options.entry_points]` section in `setup.cfg`.
    """
    return {
        "pipeline_steps": [PlotPubDatesStep()],
        "standalone_steps": [StandalonePlotPubDatesStep()],
    }
