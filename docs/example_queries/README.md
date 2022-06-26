Example queries for nqdc.

The query can be provided directly on the command line or read from a file with
`nqdc --query_file /path/to/query_file.txt`. This directory provides some
examples.

- `fmri_2019.txt`: the example used in the documentation -- articles with "fMRI"
  in their title published in 2019.
- `journal_list_fmri_vbm.txt`: downloads all articles published in a long list
  of journals related to psychology, cognitive science or neuroscience, or
  referencing fMRI, VBM or "neural correlates". This is very roughly similar to
  what was used to create the NeuroQuery dataset and is meant to produce many
  articles that contain stereotactic coordinates; as of May 2022 it should yield
  over 9,000 articles with coordinates.
