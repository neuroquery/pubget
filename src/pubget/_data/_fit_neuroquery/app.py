#!/usr/bin/env python3

from pathlib import Path
import tempfile
import base64
import re

import flask
from neuroquery import NeuroQueryModel
from neuroquery.tokenization import get_html_highlighted_text
from nilearn.plotting import view_img

template = """<!doctype html>
<head>
    <title>NeuroQuery encoder</title>
</head>
<body>
    <div>
        <form method="get">
            <label for="term">Enter query:</label><br/>
            <textarea name="term" id="term" cols="60" rows="3" value={{ term }}></textarea>
            <input type="submit" value="Run query"/>
        </form>
    </div>
    <div><p>{{highlighted_text | safe}}</p></div>
    <div>{{ img_viewer | safe }}</div>
    <div>{{ download_link | safe }}</div>
    <div><h3>Similar words</h3>{{similar_words | safe}}</div>
    <div><h3>Similar documents</h3>{{similar_documents | safe}}</div>
</body>
"""

pmc_url = "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC{}"
encoder = NeuroQueryModel.from_data_dir(
    Path(__file__).resolve().parent.joinpath("neuroquery_model")
)

app = flask.Flask(__name__)


def query_map_filename(s):
    s = re.sub(r"([^\sa-zA-Z0-9])+", "", s).strip()
    s = re.sub(r"\s+", "_", s).lower()[:72]
    s = s or "map"
    return f"{s}.nii.gz"


def download_img_link(img, description):
    file_name = query_map_filename(description)
    with tempfile.TemporaryDirectory(suffix="_pubget") as tmp_dir:
        img_path = Path(tmp_dir).joinpath(file_name)
        img.to_filename(str(img_path))
        img_data = img_path.read_bytes()
    b64_data = base64.b64encode(img_data).decode("utf-8")
    return (
        f"<a href='data:application/octet-stream;base64,{b64_data}' "
        f"download='{file_name}'>Download brain map</a>"
    )


def title_as_link(df):
    urls = []
    for _, row in df.iterrows():
        paper_url = pmc_url.format(row["pmcid"])
        link = f"""<a href="{paper_url}" target="_blank">{row["title"]}</a>"""
        urls.append(link)
    return urls


@app.route("/", methods=["GET"])
def index():
    default_term = "default mode :) please input your query here"
    return flask.redirect(flask.url_for("query", term=default_term))


@app.route("/query", methods=["GET"])
def query():
    term = flask.request.args.get("term", "")
    result = encoder(term)
    highlighted_text = get_html_highlighted_text(result["highlighted_text"])
    img_viewer = view_img(result["brain_map"], threshold=3.1).get_iframe()
    download_link = download_img_link(result["brain_map"], term)
    words = result["similar_words"].head(12).drop("weight_in_query", axis=1)
    words_table = words.style.bar(
        subset=["similarity", "weight_in_brain_map"],
        color="lightgreen",
        width=95,
    ).to_html()
    similar_docs = result["similar_documents"].head(20).copy()
    similar_docs["title"] = title_as_link(similar_docs)
    docs_style = similar_docs[["title", "similarity"]].style
    try:
        docs_style = docs_style.hide(axis="index")
    except AttributeError:
        # pandas older than 1.4
        docs_style = docs_style.hide_index()
    similar_docs_table = docs_style.bar(color="lightgreen").to_html()
    return flask.render_template_string(
        template,
        term=term,
        highlighted_text=highlighted_text,
        img_viewer=img_viewer,
        download_link=download_link,
        similar_words=words_table,
        similar_documents=similar_docs_table,
    )


if __name__ == "__main__":
    app.run()
