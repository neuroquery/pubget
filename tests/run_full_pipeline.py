import argparse
import os
import subprocess
from datetime import datetime
from pathlib import Path

import pandas as pd
from scipy import sparse


def _get_n_jobs(args):
    if args.n_jobs is not None:
        return args.n_jobs
    return os.environ.get("NQDC_N_JOBS", 4)


def run_and_check():
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--output_dir", type=str, default=".")
    parser.add_argument("--n_jobs", type=int, default=None)
    parser.add_argument("--fit_neurosynth", action="store_true")
    args = parser.parse_args()

    data_dir = Path(args.output_dir).joinpath(
        "pubget_full_pipeline_run_"
        f"{datetime.now().isoformat().replace(':', '-')}"
    )
    pubget_args = [
        "pubget",
        "run",
        str(data_dir),
        "-q",
        "fMRI[title]",
        "--log_dir",
        str(data_dir.joinpath("log")),
        "--n_jobs",
        str(_get_n_jobs(args)),
        "--extract_vocabulary",
        "--labelbuddy",
        "--fit_neuroquery",
    ]
    if args.fit_neurosynth:
        pubget_args.append("--fit_neurosynth")
    subprocess.run(pubget_args, check=True)

    print("\n")

    query_dir = data_dir.joinpath("query_1856490e8aca377ff8e6c38e84a77112")
    vectorized_dir = list(
        query_dir.glob("subset_allArticles-voc_*_vectorizedText")
    )[0]
    coords_dir = query_dir.joinpath("subset_allArticles_extractedData")

    tfidf = sparse.load_npz(str(vectorized_dir.joinpath("merged_tfidf.npz")))
    assert tfidf.shape[1] >= 300_000
    assert tfidf.shape[0] >= 3_100
    print(f"n articles: {tfidf.shape[0]}")

    coords = pd.read_csv(coords_dir.joinpath("coordinates.csv"))
    assert coords.shape[1] == 6
    assert coords.shape[0] >= 30_000
    print(f"n coordinates: {coords.shape[0]}")

    print(f"\npubget pipeline ran successfully\nresults saved in {query_dir}")


if __name__ == "__main__":
    run_and_check()
