import subprocess
import argparse
from pathlib import Path
from datetime import datetime

from scipy import sparse
import pandas as pd


def run_and_check():
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output_dir", type=str, default=".")
    args = parser.parse_args()

    data_dir = Path(args.output_dir).joinpath(
        "nqdc_full_pipeline_run_"
        f"{datetime.now().isoformat().replace(':', '-')}"
    )

    subprocess.run(
        [
            "nqdc",
            "run",
            str(data_dir),
            "-q",
            "fMRI[title]",
            "--log_dir",
            str(data_dir.joinpath("log")),
            "--n_jobs",
            "4",
            "--labelbuddy",
        ],
        check=True,
    )

    print("\n")

    query_dir = data_dir.joinpath("query-1856490e8aca377ff8e6c38e84a77112")
    vectorized_dir = query_dir.joinpath(
        "subset_allArticles-voc_"
        "e6f7a7e9c6ebc4fb81118ccabfee8bd7_vectorizedText"
    )
    coords_dir = query_dir.joinpath("subset_allArticles_extractedData")

    tfidf = sparse.load_npz(str(vectorized_dir.joinpath("merged_tfidf.npz")))
    assert tfidf.shape[1] == 6308
    assert tfidf.shape[0] >= 3100
    print(f"n articles: {tfidf.shape[0]}")

    coords = pd.read_csv(coords_dir.joinpath("coordinates.csv"))
    assert coords.shape[1] == 6
    assert coords.shape[0] >= 3800
    print(f"n coordinates: {coords.shape[0]}")

    print(f"\nnqdc pipeline ran successfully\nresults saved in {query_dir}")


if __name__ == "__main__":
    run_and_check()
