[![build](https://github.com/neuroquery/nqdc/actions/workflows/testing.yml/badge.svg)](https://github.com/neuroquery/nqdc/actions/workflows/testing.yml)
[![codecov](https://codecov.io/gh/neuroquery/nqdc/branch/main/graph/badge.svg?token=8KEBP2EN3A)](https://codecov.io/gh/neuroquery/nqdc)
[![nqdc on GitHub](https://img.shields.io/static/v1?label=&message=nqdc%20on%20GitHub&color=black&style=flat&logo=github)](https://github.com/neuroquery/nqdc)


# NeuroQuery Data Collection

**Warning: `nqdc` is in early development; its commands and functions are
subject to change.**

`nqdc` is a command-line tool for collecting data for large-scale
coordinate-based neuroimaging meta-analysis. It exposes some of the machinery
that was used to create the [neuroquery
dataset](https://github.com/neuroquery/neuroquery_data), which powers
[neuroquery.org](https://neuroquery.org).

`nqdc` downloads full-text articles from [PubMed
Central](https://www.ncbi.nlm.nih.gov/pmc/) and extracts their text and
stereotactic coordinates. It also computes [TFIDF
features](https://en.wikipedia.org/wiki/Tf%E2%80%93idf) for the extracted text.

Besides the command-line interface, `nqdc`'s functionality is also exposed
through its [Python API](https://neuroquery.github.io/nqdc/#python-api).

# Installation

You can install `nqdc` by running:
```
pip install nqdc
```

This will install the `nqdc` Python package, as well as the `nqdc` command.

# Quick Start

Once `nqdc` is installed, we can download and process neuroimaging articles so
that we can later use them for meta-analysis.

```
nqdc run ./nqdc_data -q 'fMRI[title]'
```

See `nqdc run --help` for a description of this command. In
particular, the `--n_jobs` option allows running the data extraction in
parallel, which can significantly speed up the pipeline.

# Usage

The creation of a dataset happens in four steps:
- Downloading the articles in bulk from the
  [PMC](https://www.ncbi.nlm.nih.gov/pmc/) API.
- Extracting the articles from the bulk download
- Extracting text, stereotactic coordinates and metadata from the articles, and
  storing this information in CSV files.
- Vectorizing the text: transforming it into vectors of
  [TFIDF](https://en.wikipedia.org/wiki/Tf%E2%80%93idf) features.

Each of these steps stores its output in a separate directory. Normally, you
will run the whole procedure in one command by invoking `nqdc run`. However,
separate commands are also provided to run each step separately. Below, we
describe each step and its output. Use `nqdc -h` to see a list of all available
commands.

All articles downloaded by `nqdc` come from [PubMed
Central](https://www.ncbi.nlm.nih.gov/pmc/), and are therefore identified by
their PubMed Central ID (`pmcid`). Note this is not the same as the PubMed ID
(`pmid`). Not all articles in PMC have a `pmid`.

## Step 1: Downloading articles from PMC

This step is executed by the `nqdc download` command.

We must first define our query, with which Pubmed Central will be searched for
articles. It can be simple such as `'fMRI'`, or more specific such as
`'fMRI[Abstract] AND ("2000"[PubDate] : "2022"[PubDate])'`. You can build the
query using the [PMC advanced search
interface](https://www.ncbi.nlm.nih.gov/pmc/advanced). For more information see
[the E-Utilities help](https://www.ncbi.nlm.nih.gov/books/NBK3837/). The query
can be passed either as a string on the command-line or by passing the path of a
text file containing the query.

If we have an Entrez API key (see details in the [E-utilities
documentation](https://www.ncbi.nlm.nih.gov/books/NBK25497/)), we can provide it
through the `NQDC_API_KEY` environment variable or through the `--api_key`
command line argument (the latter has higher precedence).

We must also specify the directory in which all `nqdc` data will be stored.
Subdirectories will be created for each different query. In the following we
suppose we are storing our data in a directory called `nqdc_data`.

We can thus download all articles with "fMRI" in their title published in 2019 by running:
```
nqdc download -q 'fMRI[Title] AND ("2019"[PubDate] : "2019"[PubDate])' nqdc_data
```

After running this command, these are the contents of our data directory:
```
· nqdc_data
  └── query-10c72245c52d7d4e6f535e2bcffb2572
      └── articlesets
          ├── articleset_00000.xml
          └── info.json
```

`nqdc` has created a subdirectory for this query. If we run the download again
for the same query, the same subdirectory will be reused
(`10c72245c52d7d4e6f535e2bcffb2572` is the md5 checksum of the query).

Inside the query directory, the results of the bulk download are stored in the
`articlesets` directory. The articles themselves are in XML files bundling up to
500 articles called `articleset_*.xml`. Here there is only one because the
search returned less than 500 articles.

Some information about the download is stored in `info.json`. In particular,
`is_complete` indicates if all articles matching the search have been
downloaded. If the download was interrupted, some batches failed to download, or
the number of results was limited by using the `--n_docs` parameter,
`is_complete` will be `false` and the exit status of the program will
be 1. You may want to re-run the command before moving on to the next step if
the download is incomplete.

If we run the same query again, only missing batches will be downloaded. If we
want to force re-running the search and downloading the whole data we need to
remove the `articlesets` directory.


## Step 2: extracting articles from bulk download

This step is executed by the `nqdc extract_articles` command.

Once our download is complete, we extract articles directory and store them in
individual XML files. To do so, we pass the `articlesets` directory created by
the `nqdc download` command in step 1:

```
nqdc extract_articles nqdc_data/query-10c72245c52d7d4e6f535e2bcffb2572/articlesets
```

This creates an `articles` subdirectory in the query directory, containing the
articles. To avoid having a large number of files in a single directory when
there are many articles, which can be problematic on some filesystems, the
articles are spread over many subdirectories. The names of these subdirectories
range from `000` to `fff` and an article goes in the subdirectory that matches
the first 3 hexidecimal digits of the md5 hash of its `pmcid`.

Our data directory now looks like:

```
· nqdc_data
  └── query-10c72245c52d7d4e6f535e2bcffb2572
      ├── articlesets
      │   ├── articleset_00000.xml
      │   └── info.json
      └── articles
          ├── 019
          │   └── pmcid_6759467.xml
          ├── 01f
          │   └── pmcid_6781806.xml
          ├── 03f
          │   └── pmcid_6625472.xml
          ├── ...
          ├── 27d
          │   ├── pmcid_6657681.xml
          │   └── pmcid_6790327.xml
          ├── ...
          │
          └── info.json
```

If the download and article extraction were successfully run and we run the same
query again, the article extraction is skipped. If we want to force re-running
the article extraction we need to remove the `articles` directory (or the
`info.json` file it contains).


## Step 3: extracting data from articles

This step is executed by the `nqdc extract_data` command.

It creates another directory that contains CSV files, containing the text,
metadata and coordinates extracted from all the articles.

If we use the `--articles_with_coords_only` option, only articles in which
`nqdc` finds stereotactic coordinates are kept. The name of the resulting
directory will reflect that choice.

We pass the path of the `articles` directory created by `nqdc extract_articles`
in the previous step to the `nqdc extract_data` command:

```
nqdc extract_data --articles_with_coords_only nqdc_data/query-10c72245c52d7d4e6f535e2bcffb2572/articles/
```

Our data directory now contains (ommitting the contents of the previous steps):

```
· nqdc_data
  └── query-10c72245c52d7d4e6f535e2bcffb2572
      ├── articles
      ├── articlesets
      └── subset_articlesWithCoords_extractedData
          ├── authors.csv
          ├── coordinates.csv
          ├── coordinate_space.csv
          ├── info.json
          ├── metadata.csv
          └── text.csv
```

If we had not used `--articles_with_coords_only`, the new subdirectory would be
named `subset_allArticles_extractedData` instead.

- `metadata.csv` contains one row per article, with some metadata: `pmcid`
  (PubMed Central ID), `pmid` (PubMed ID), `doi`, `title`, `journal`,
  `publication_year` and `license`. Note some values may be missing (for example
  not all articles have a `pmid` or `doi`).
- `authors.csv` contains one row per article per author. Fields are `pmcid`,
  `surname`, `given-names`.
- `text.csv` contains one row per article. The first field is the `pmcid`, and
  the other fields are `title`, `keywords`, `abstract`, and `body`, and contain
  the text extracted from these parts of the article.
- `coordinates.csv` contains one row for each `(x, y, z)` stereotactic
  coordinate found in any article. Its fields are the `pmcid` of the article,
  the table label and id the coordinates came from, and `x`, `y`, `z`.
- `coordinate_space.csv` has fields `pmcid` and `coordinate_space`. It contains
  a guess about the stereotactic space coordinates are reported in, based on a
  heuristic derived from [neurosynth](https://github.com/neurosynth/ACE).
  Possible values for the space are the terms used by `neurosynth`: "MNI", "TAL"
  (for Talairach space), and "UNKNOWN".

The different files can be joined on the `pmcid` field.

If all steps up to data extraction were successfully run and we run the same
query again, the data extraction is skipped. If we want to force re-running the
data extraction we need to remove the corresponding directory (or the
`info.json` file it contains).

## Step 4: vectorizing (computing TFIDF features)

This step is executed by the `nqdc vectorize` command.

Some large-scale meta-analysis methods such as
[neurosynth](https://neurosynth.org/) and [neuroquery](https://neuroquery.org)
rely on [TFIDF features](https://en.wikipedia.org/wiki/Tf%E2%80%93idf) to
represent articles' text. The last step before we can apply these methods is
therefore to extract TFIDF features from the text we obtained in the previous
step.

TFIDF features rely on a predefined vocabulary (set of terms or phrases). Each
dimension of the feature vector corresponds to a term in the vocabulary and
represents the importance of that term in the encoded text. This importance is
an increasing function of the *term frequency* (the number of time the term
occurs in the text divided by the length of the text) and a decreasing function
of the *document frequency* (the total number of times the term occurs in the
whole corpus or dataset).

To extract the TFIDF features we must therefore choose a vocabulary.
- By default, `nqdc` will download and use the vocabulary used by
[neuroquery.org](https://neuroquery.org).
- If we use the `--extract_vocabulary` option, a new vocabulary is created from
  the downloaded text and used for computing TFIDF features (see "extracting a
  new vocabulary" below).
- If we want to use a different vocabulary we can specify it with the
`--vocabulary_file` option. This file will be parsed as a CSV file with no
header, whose first column contains the terms. Other columns are ignored.

We also pass to `nqdc vectorize` the directory containing the text we want to
vectorize, created by `nqdc extract_data` in step 3 (here we are using the
default vocabulary):

```
nqdc vectorize nqdc_data/query-10c72245c52d7d4e6f535e2bcffb2572/subset_articlesWithCoords_extractedData/
```

This creates a new directory whose name reflects the data source (whether all
articles are kept or only those with coordinates) and the chosen vocabulary
(`e6f7a7e9c6ebc4fb81118ccabfee8bd7` is the md5 checksum of the contents of the
vocabulary file, concatenated with those of the vocabulary mapping file, see
"vocabulary mapping" below):

```
· nqdc_data
  └── query-10c72245c52d7d4e6f535e2bcffb2572
      ├── articles
      ├── articlesets
      ├── subset_articlesWithCoords_extractedData
      └── subset_articlesWithCoords-voc_e6f7a7e9c6ebc4fb81118ccabfee8bd7_vectorizedText
          ├── abstract_counts.npz
          ├── abstract_tfidf.npz
          ├── body_counts.npz
          ├── body_tfidf.npz
          ├── feature_names.csv
          ├── info.json
          ├── keywords_counts.npz
          ├── keywords_tfidf.npz
          ├── merged_tfidf.npz
          ├── pmcid.txt
          ├── title_counts.npz
          ├── title_tfidf.npz
          ├── vocabulary.csv
          └── vocabulary.csv_voc_mapping_identity.json
```

The extracted features are stored in `.npz` files that can be read for example
with
[`scipy.sparse.load_npz`](https://docs.scipy.org/doc/scipy/reference/generated/scipy.sparse.load_npz.html).

These files contain matrices of shape `(n_docs, n_features)`, where `n_docs` is
the number of documents and `n_features` the number of terms in the vocabulary.
The `pmcid` corresponding to each row is found in `pmcid.txt`, and the term
corresponding to each column is found in the first column of
`feature_names.csv`.

`feature_names.csv` has no header; the first column contains terms and the
second one contains their document frequency.

For each article part ("title", "keywords", "abstract" and "body"), we get the
`counts` which hold the raw counts (the number of times each word occurs in that
section), and the `tfidf` which hold the TFIDF features (the counts divided by
article length and log document frequency). Moreover, `merged_tfidf` contains
the mean TFIDF computed across all article parts.

If all steps up to vectorization were successfully run and we run the same query
again, the vectorization is skipped. If we want to force re-running the
vectorization we need to remove the corresponding directory (or the `info.json`
file it contains).

### Vocabulary mapping: collapsing redundant words

It is possible to instruct the tokenizer (that extracts words from text) to
collapse some pairs of terms that have the same meaning but different spellings,
such as "brainstem" and "brain stem".

This is done through a JSON file that contains a mapping of the form `{term:
replacement}`. For example if it contains `{"brain stem": "brainstem"}`, "brain
stem" will be discarded from the vocabulary and every occurrence of "brain stem"
will be counted as an occurrence of "brainstem" instead. To be found by `nqdc`,
this vocabulary mapping file must be in the same directory as the vocabulary
file, and its name must be the vocabulary file's name with
`_voc_mapping_identity.json` appended: for example `vocabulary.csv`,
`vocabulary.csv_voc_mapping_identity.json`.

When a vocabulary mapping is provided, a shorter vocabulary is therefore created
by removing redundant words. The TFIDF and word counts computed by `nqdc`
correspond to the shorter vocabulary, which is stored along with its document
frequencies in `feature_names.csv`.

`vocabulary.csv` contains the document frequencies of the original (full,
longer) vocabulary. A `vocabulary.csv_voc_mapping_identity.json` file is always
created by `nqdc`, but if no vocabulary mapping was used, that file contains an
empty mapping (`{}`) and `vocabulary.csv` and `feature_names.csv` are identical.

The vocabulary mapping is primarily used by the `neuroquery` package and its
tokenization pipeline, and you can safely ignore this – just remember that the
file providing the terms corresponding to the TFIDF *features* is
`feature_names.csv`.

## Optional step: extracting a new vocabulary

This step is executed by the `nqdc extract_vocabulary` command.
When running the full pipeline this step is optional: we must use
the `--extract_vocabulary` option for it to be executed.

It builds a vocabulary of all the words and 2-grams (groups of 2 
words) that appear in the downloaded text, and computes their document frequency
(the proportion of documents in which a term appears).
```
nqdc extract_vocabulary nqdc_data/query-10c72245c52d7d4e6f535e2bcffb2572/subset_articlesWithCoords_extractedData
```

The vocabulary is stored in a csv file in a new directory. There is no header
and the 2 columns are the term and its document frequency.

```
· nqdc_data
  └── query-10c72245c52d7d4e6f535e2bcffb2572
      ├── articles
      ├── articlesets
      ├── subset_articlesWithCoords_extractedData
      ├── subset_articlesWithCoords_extractedVocabulary
      │   ├── info.json
      │   └── vocabulary.csv
      └── subset_articlesWithCoords-voc_e6f7a7e9c6ebc4fb81118ccabfee8bd7_vectorizedText
```

When running the whole pipeline (`nqdc run`), if we use the
`--extract_vocabulary` option and don't provide an explicit value for
`--vocabulary_file`, the freshly-extracted vocabulary is used instead of the
default `neuroquery` one for computing TFIDF features.

## Optional step: fitting a NeuroQuery encoding model

This step is executed by the `nqdc fit_neuroquery` command. When running the
full pipeline it is optional: we must use the `--fit_neuroquery` option
for it to be executed.

In this step, once the TFIDF features and the coordinates have been extracted
from downloaded articles, they are used to train a NeuroQuery encoding model --
the same type of model that is exposed at
[neuroquery.org](https://neuroquery.org). Details about this model are provided
in [the NeuroQuery paper](https://elifesciences.org/articles/53385) and the
documentation for the [neuroquery
package](https://github.com/neuroquery/neuroquery).

Note: for this model to give good results a large dataset is needed, ideally close to 10,000 articles (with coordinates).

We pass the `_vectorizedText` directory created by `nqdc vectorize`:
```
nqdc fit_neuroquery nqdc_data/query-10c72245c52d7d4e6f535e2bcffb2572/subset_articlesWithCoords-voc_e6f7a7e9c6ebc4fb81118ccabfee8bd7_vectorizedText
```

This creates a directory whose name ends with `_neuroqueryModel`:

```
· nqdc_data
  └── query-10c72245c52d7d4e6f535e2bcffb2572
      ├── articles
      ├── articlesets
      ├── subset_articlesWithCoords_extractedData
      ├── subset_articlesWithCoords-voc_e6f7a7e9c6ebc4fb81118ccabfee8bd7_neuroqueryModel
      │   ├── app.py
      │   ├── info.json
      │   ├── neuroquery_model
      │   │   ├── corpus_metadata.csv
      │   │   ├── corpus_tfidf.npz
      │   │   ├── mask_img.nii.gz
      │   │   ├── regression
      │   │   │   ├── coef.npy
      │   │   │   ├── intercept.npy
      │   │   │   ├── M.npy
      │   │   │   ├── original_n_features.npy
      │   │   │   ├── residual_var.npy
      │   │   │   └── selected_features.npy
      │   │   ├── smoothing
      │   │   │   ├── smoothing_weight.npy
      │   │   │   └── V.npy
      │   │   ├── vocabulary.csv
      │   │   └── vocabulary.csv_voc_mapping_identity.json
      │   ├── README.md
      │   └── requirements.txt
      └── subset_articlesWithCoords-voc_e6f7a7e9c6ebc4fb81118ccabfee8bd7_vectorizedText
```

You do not need to care about the contents of the `neuroquery_model` subdirectory, that is data used by the `neuroquery` package.
Just know that it can be used to initialize a `neuroquery.NeuroQueryModel` with:
```python
from neuroquery import NeuroQueryModel
model = NeuroQueryModel.from_data_dir("neuroquery_model")
```
The `neuroquery` documentation provides information and examples on how to use this model.

### Visualizing the newly trained model in an interactive web page

It is easy to interact with the model through a small web (Flask) application.
From inside the `[...]_neuroqueryModel` directory, just run `pip install -r requirements.txt` to install `flask`, `nilearn` and `neuroquery`.
Then run `flask run` and point your web browser to `https://localhost:5000`: you can play with a local, simplified version of [neuroquery.org](https://neuroquery.org) built with the data we just downloaded.


## Optional step: preparing articles for annotation with `labelbuddy`

This step is executed by the `nqdc extract_labelbuddy_data` command.
When running the full pipeline this step is optional: we must use
the `--labelbuddy` or `--labelbuddy_part_size` option for it to be executed.

It prepares the articles whose data was extracted for annotation with
[labelbuddy](https://jeromedockes.github.io/labelbuddy/).

We pass the `_extractedData` directory created by `nqdc extract_data`:

```
nqdc extract_labelbuddy_data nqdc_data/query-10c72245c52d7d4e6f535e2bcffb2572/subset_articlesWithCoords_extractedData
```

This creates a directory whose name ends with `labelbuddyData` containing the batches of documents in JSONL format (in this case there is a single batch):

```
· nqdc_data
  └── query-10c72245c52d7d4e6f535e2bcffb2572
      ├── articles
      ├── articlesets
      ├── subset_articlesWithCoords_extractedData
      ├── subset_articlesWithCoords_labelbuddyData
      │   ├── documents_00001.jsonl
      │   └── info.json
      └── subset_articlesWithCoords-voc_e6f7a7e9c6ebc4fb81118ccabfee8bd7_vectorizedText
```

The documents can be imported into `labelbuddy` using the GUI or with:

```
labelbuddy mydb.labelbuddy --import-docs documents_00001.jsonl
```

See the [labelbuddy
documentation](https://jeromedockes.github.io/labelbuddy/labelbuddy/current/documentation/)
for details.

## Optional step: creating a NiMARE dataset

This step is executed by the `nqdc extract_nimare_data` command. When running
the full pipeline this step is optional: we must use the `--nimare` option for
it to be executed.

It creates a [NiMARE](https://nimare.readthedocs.io/) dataset for the extracted
data in JSON format. See the NiMARE
[documentation](https://nimare.readthedocs.io/en/latest/generated/nimare.dataset.Dataset.html#nimare.dataset.Dataset)
for details.

We pass the `_vectorizedText` directory created by `nqdc vectorize`:

```
nqdc extract_nimare_data nqdc_data/query-10c72245c52d7d4e6f535e2bcffb2572/subset_articlesWithCoords-voc_e6f7a7e9c6ebc4fb81118ccabfee8bd7_vectorizedText
```

The resulting directory contains a `nimare_dataset.json` file that can be used to initialize a `nimare.Dataset`. 

```
· nqdc_data
  └── query-10c72245c52d7d4e6f535e2bcffb2572
      ├── articles
      ├── articlesets
      ├── subset_articlesWithCoords_extractedData
      ├── subset_articlesWithCoords-voc_e6f7a7e9c6ebc4fb81118ccabfee8bd7_nimareDataset
      │   ├── info.json
      │   └── nimare_dataset.json
      └── subset_articlesWithCoords-voc_e6f7a7e9c6ebc4fb81118ccabfee8bd7_vectorizedText
```

Using this option requires installing NiMARE, which is not installed by default
with `nqdc`. To use this option, install NiMARE separately with
```
pip install nimare
```
or install `nqdc` with
```
pip install 'nqdc[nimare]'
```

## Full pipeline

We can run all steps in one command by using `nqdc run`.

The full procedure described above could be run by executing:

```
nqdc run -q 'fMRI[Title] AND ("2019"[PubDate] : "2019"[PubDate])' \
    --articles_with_coords_only                                   \
    nqdc_data
```

If we also want to apply the optional steps:
```
nqdc run -q 'fMRI[Title] AND ("2019"[PubDate] : "2019"[PubDate])' \
    --articles_with_coords_only                                   \
    --fit_neuroquery                                              \
    --labelbuddy                                                  \
    --nimare                                                      \
    nqdc_data
```
(remember that `--nimare` requires NiMARE to be installed).

Here also, steps that had already been completed are skipped; we need to remove
the corresponding directories if we want to force running these steps again.

See `nqdc run --help` for a description of all options.


## Logging

By default `nqdc` commands report their progress by writing to the standard
streams. In addition, they can write log files if we provide the `--log_dir`
command-line argument, or if we define the `NQDC_LOG_DIR` environment variable
(the command-line argument has higher precedence). If this log directory is
specified, a new log file with a timestamp is created and all the output is
written there as well.

# Writing plugins

It is possible to write plugins and define [entry
points](https://setuptools.pypa.io/en/latest/userguide/entry_point.html) to add
functionality that is automatically executed when `nqdc` is run.

The name of the entry point should be `nqdc.plugin_processing_steps`. It must be
a function taking no arguments and returning a dictionary with keys
`pipeline_steps` and `standalone_steps`. The corresponding values must be lists
of processing step objects, that must implement the interface defined by
`nqdc.BaseProcessingStep` (their types do not need to inherit from
`nqdc.BaseProcessingStep`).

All steps in `pipeline_steps` will be run when `nqdc run` is used. All steps in
`standalone_steps` will be added as additional nqdc commands; for example if the
`name` of a standalone step is `my_plugin`, the `nqdc my_plugin` command will
become available.

# Contributing

Feedback and contributions are welcome. Development happens at the
[nqdc GitHub repositiory](https://github.com/neuroquery/nqdc).
To install the dependencies required for development, from the directory where you cloned `nqdc`, run:
```
pip install -e '.[dev]'
```

The tests can be run with `make test_all`, or `make test_coverage` to report
test coverage. The documentation can be rendered with `make doc`. `make
run_full_pipeline` runs the full `nqdc` pipeline on a query returning a
realistic number of results (`'fMRI[title]'`).

# Python API

`nqdc` is mostly intended for use as a command-line tool. However, it is also a
Python package and its functionality can be used in Python programs. The Python
API closely reflects the command-line programs described above.

The Python API is described on the `nqdc` [website](https://neuroquery.github.io/nqdc/#python-api).
