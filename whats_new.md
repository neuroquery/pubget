# `pubget` releases

## 0.0.9

- The `"table_foot"` key has been added to table info JSON files. It holds the contents of the `table-wrap-foot` element for that table.

## 0.0.8

- NeuroVault image and collection IDs are extracted and stored in the `extractedData` directory.
- When a request fails, the request and response (if there is one) are dumped in the `articlesets/failed_requests_dumps/` directory for easier diagnosis of issues with Entrez.
- The download step is more robust to malformed XML files sent by efetch.fcgi
- An `--alias` command-line option has been added to create a symlink with a human-readable name to a query's output directory.

## 0.0.7

- `query.txt` and `requested_pmcids.txt` have moved from the `articlesets/` subdirectory to the root of the query or pmcid list's directory.
- `labelbuddy` output now contains a `batch_info.csv` file providing the `.jsonl` file and line position of each `pmcid`.
- The environment variable to provide the NCBI API key has been renamed from `PUBGET_API_KEY` to `NCBI_API_KEY`.
- When downloading a list of PMCIDs (with `--pmcids_file` option), the list is now filtered to keep only articles that are in the PMC Open Access subset.

## 0.0.6

- The text vectorization (TFIDF) step is now optional. 
  It is run when using the options `--vectorize_text` or `--vocabulary_file`, or when subsequent steps that need the TFIDF (neurosynth, neuroquery, nimare) are requested.

## 0.0.5

- `nqdc` renamed `pubget`; all symbols, paths, environment variables etc. have been adapted accordingly.


# `nqdc` releases

`pubget` used to be called `nqdc`.
The `nqdc` package is deprecated and should no longer be used.

## 0.0.3

- downloading batches of articles now a bit more robust because response content is check and nqdc retries up to 4 times if a request fails.
- query output directories renamed from query-<md5> to query_<md5> to follow a bids-like convention.
- It is now possible to download an explicit list of PMCIDs rather than a PMC query to select the articles to download.
  See the `--pmcids_file` parameter or the "Usage/Step 1" part of the documentation.
- Articles in `query_dir/articles` are now each stored in a separate subdirectory; that also contains a `tables` subdirectory with the tables extracted from the article.
- External links are now extracted from articles and stored in `links.csv` during the `extract_data` step.

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
