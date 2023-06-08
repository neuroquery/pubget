Example queries for pubget.

The query can be provided directly on the command line or read from a file with
`pubget --query_file /path/to/query_file.txt`. This directory provides some
examples.

- `fmri_2019.txt`: the example used in the documentation -- articles with "fMRI"
  in their title published in 2019.
- `journal_list_fmri_vbm.txt`: downloads all articles published in a long list
  of journals related to psychology, cognitive science or neuroscience, or
  referencing fMRI, VBM or "neural correlates". This is very roughly similar to
  what was used to create the NeuroQuery dataset and is meant to produce many
  articles that contain stereotactic coordinates; as of June 2023 it should yield
  over 10,000 articles with coordinates.
- `pmcids.txt`: contains around 11,000 PMCIDS for which stereotactic coordinates
  were found. It was built as the union of PMCIDs for articles in NeuroQuery,
  NeuroSynth, and the results of `journal_list_fmri_vbm.txt` that contain
  stereotactic coordinates (`pubget --articles_with_coords_only`). It can be used
  as `pubget --pmcids_file pmcids.txt`.
