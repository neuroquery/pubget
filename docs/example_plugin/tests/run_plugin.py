#! /usr/bin/env python3

import subprocess
import tempfile
from pathlib import Path

with tempfile.TemporaryDirectory(suffix="_pubget") as tmp_dir:
    subprocess.run(
        [
            "pubget",
            "run",
            "--plot_pub_dates",
            "-q",
            "fMRI[Abstract] AND aphasia[Title] "
            "AND (2017[PubDate] : 2019[PubDate])",
            tmp_dir,
        ]
    )
    assert (
        Path(tmp_dir)
        .joinpath(
            "query_49e0abb9869a532a31d37ed788c76780",
            "subset_allArticles_examplePluginPubDatesPlot",
            "plot.png",
        )
        .is_file()
    ), "Plugin output not found!"

print("pubget and plugin ran successfully")
