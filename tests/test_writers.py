import numpy as np
import pandas as pd

from nqdc import _writers


def test_csv_writer(tmp_path):
    output_file = tmp_path.joinpath("myoutput.csv")
    writer = _writers.CSVWriter(("A", "B", "C"), "mydata", output_file)
    with writer:
        writer.write({"mydata": {"A": "a1", "B": 1}})
        df = pd.DataFrame(
            [{"A": "a2", "C": 20.5}, {"A": "a3", "B": 3, "C": 30.5}]
        )
        assert np.isnan(df.at[0, "B"])
        assert df.at[0, "B"] is not None
        writer.write({"mydata": df})
        writer.write({"otherdata": df})
    result = pd.read_csv(output_file, na_values=[""], keep_default_na=False)
    assert result.shape == (3, 3)
    assert result.isnull().sum().sum() == 2
    assert result.at[2, "C"] == 30.5
