import subprocess
import tempfile
from pathlib import Path

import pandas as pd

with tempfile.TemporaryDirectory("_nqdc_test") as tmp_dir:
    data_dir = Path(tmp_dir)
    query = (
        "fMRI[Title] AND memory[Abstract] AND (2020[PubDate] : 2020[PubDate])"
    )
    nqdc_args = [
        "nqdc",
        "run",
        str(data_dir),
        "-q",
        query,
    ]
    subprocess.run(nqdc_args, check=True)
    metadata_file = data_dir.joinpath(
        "query-d3b30e14cf943416f1129eb1f111e8bc",
        "subset_allArticles_extractedData",
        "metadata.csv",
    )
    pmcids = pd.read_csv(metadata_file, usecols=["pmcid"])[
        "pmcid"
    ].sort_values()
    pmcids_file = data_dir.joinpath("found_pmcids.txt")
    pmcids.to_csv(pmcids_file, index=False, header=None)
    new_nqdc_args = [
        "nqdc",
        "run",
        str(data_dir),
        "--pmcids_file",
        str(pmcids_file),
    ]
    subprocess.run(new_nqdc_args, check=True)
    new_metadata_file = data_dir.joinpath(
        "pmcidList-0594dba48803670013a89c740bf50934",
        "subset_allArticles_extractedData",
        "metadata.csv",
    )
    new_pmcids = pd.read_csv(new_metadata_file, usecols=["pmcid"])[
        "pmcid"
    ].sort_values()
    assert (new_pmcids.values == pmcids.values).all()

    print("PMCIDs downloaded with query and with PMCID list match.")
