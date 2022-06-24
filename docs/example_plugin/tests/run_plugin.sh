#! /bin/bash

nqdc_dir=$(mktemp -d)
if [[ ! "$nqdc_dir" || ! -d "$nqdc_dir" ]]; then
    echo "failed to create temp dir"
    exit 1
fi

trap "rm -r ${nqdc_dir}" EXIT

nqdc run \
     --plot_pub_dates \
     -q 'fMRI[Abstract] AND aphasia[Title] AND ("2017"[PubDate] : "2019"[PubDate])' \
     "${nqdc_dir}"

if [ ! -f "${nqdc_dir}/query-f66b29adbde5cfae6e3b57cafcf93891/subset_allArticles_examplePluginPubDatesPlot/plot.png" ]; then
    echo "plot not found"
    exit 1
fi
