#!/usr/bin/env python3

from pathlib import Path

import flask
import numpy as np
from scipy import sparse
import pandas as pd
from nilearn import image, datasets

try:
    from nilearn import maskers
except ImportError:
    from nilearn import input_data as maskers
from nilearn.plotting import view_img
from nilearn.glm import fdr_threshold

template = """<!doctype html>
<head>
    <title>NeuroSynth encoder</title>
</head>
<body>
    <div>
        <form method="get">
            <label for="term">Select term:</label>
            <input name="term" id="term" value="{{ term }}" list="all-terms"/>
            <datalist id="all-terms">
                {% for term in all_terms %}
                <option value="{{ term }}"/>
                {% endfor %}
            </datalist>
            <input type="submit" value="Run query"/>
        </form>
    </div>
    {% if not term_missing %}
    <div>{{ img_viewer | safe }}</div>
    <div>
        <a href="{{ url_for("download_image", term=term) }}">Download image</a>
    </div>
    <div><h3>Similar documents</h3>{{similar_documents | safe}}</div>
    {% else %}
    <div>
        <p> Selected term is not in the vocabulary,
            please choose a different one.</p>
    </div>
    {% endif %}
</body>
"""

pmc_url = "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC{}"
data_dir = Path(__file__).resolve().parent
maps_dir = data_dir.joinpath("neurosynth_maps")
terms_info = pd.read_csv(str(data_dir.joinpath("terms.csv")), index_col=0)
terms_info["pos"] = np.arange(terms_info.shape[0])
metadata = pd.read_csv(str(data_dir.joinpath("metadata.csv")))
tfidf = sparse.load_npz(str(data_dir.joinpath("tfidf.npz")))
masker = maskers.NiftiMasker(str(data_dir.joinpath("brain_mask.nii.gz"))).fit()

app = flask.Flask(__name__)


def title_as_link(df):
    urls = []
    for _, row in df.iterrows():
        paper_url = pmc_url.format(row["pmcid"])
        link = f"""<a href="{paper_url}" target="_blank">{row["title"]}</a>"""
        urls.append(link)
    return urls


@app.route("/", methods=["GET"])
def index():
    default_term = "default mode"
    return flask.redirect(flask.url_for("query", term=default_term))


@app.route("/download_image", methods=["GET"])
def download_image():
    term = flask.request.args.get("term", "")
    if term not in terms_info.index:
        flask.abort(404)
    file_name = terms_info.loc[term]["file_name"]
    return flask.send_from_directory(
        maps_dir,
        f"{file_name}.nii.gz",
        as_attachment=True,
        mimetype="application/octet-stream",
    )


def _get_image_viewer(term):
    term_file_name = terms_info.loc[term, "file_name"]
    img_path = maps_dir.joinpath(f"{term_file_name}.nii.gz")
    img = image.load_img(str(img_path))
    threshold = fdr_threshold(masker.transform(img).ravel(), 0.01)
    img_viewer = view_img(img, threshold=threshold).get_iframe()
    return img_viewer


def _get_similar_docs_table(term):
    loadings = tfidf[:, terms_info.loc[term, "pos"]].A.ravel()
    order = np.argpartition(-loadings, np.arange(20))[:20]
    similar_docs = metadata.iloc[order].copy()
    similar_docs["similarity"] = loadings[order]
    similar_docs["title"] = title_as_link(similar_docs)
    docs_style = similar_docs[["title", "similarity"]].style
    try:
        docs_style = docs_style.hide(axis="index")
    except AttributeError:
        # pandas older than 1.4
        docs_style = docs_style.hide_index()
    similar_docs_table = docs_style.bar(color="lightgreen").to_html()
    return similar_docs_table


@app.route("/query", methods=["GET"])
def query():
    term = flask.request.args.get("term", "").strip()
    if term not in terms_info.index:
        return flask.render_template_string(
            template,
            term_missing=True,
            term="",
            all_terms=terms_info.index.values,
        )
    img_viewer = _get_image_viewer(term)
    similar_docs_table = _get_similar_docs_table(term)
    return flask.render_template_string(
        template,
        term_missing=False,
        term=term,
        all_terms=terms_info.index.values,
        img_viewer=img_viewer,
        similar_documents=similar_docs_table,
    )


if __name__ == "__main__":
    app.run()
