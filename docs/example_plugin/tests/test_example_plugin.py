import pandas as pd
import nqdc_example_plugin


def test_example_plugin(tmp_path):
    meta_data = pd.DataFrame(
        {"pmcid": [1, 2, 3], "publication_year": [2018, 2020, 2020]}
    )
    extracted_data = tmp_path.joinpath("nqdc_extractedData")
    extracted_data.mkdir()
    meta_data.to_csv(extracted_data.joinpath("metadata.csv"), index=False)
    out_dir, code = nqdc_example_plugin.plot_publication_dates(extracted_data)
    assert out_dir.joinpath("plot.png").is_file()
