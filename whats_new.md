# `nqdc` releases

## 0.0.3.dev

- external links are now extracted from articles and stored in `links.csv` during the `extract_data` step.

## 0.0.2

- Changes to the command-line interface; now all in one command `nqdc`; `nqdc_full_pipeline` becomes `nqdc run`.

- Add several commands/steps:

  - Creating a NiMARE dataset.
  - Preparing documents for annotation with labelbuddy.
  - Extracting a new vocabulary.
  - Fitting neuroquery.
  - Running neurosynth analysis.
  - Possibility to create plugins.

- Parallelize data extraction; several improvements to text & coordinate extraction

## 0.0.1

First release; tentative API for downloading PMC data, extracting articles,
extracting data, and vectorizing text.
