import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

from nqdc._entrez import EntrezClient
from nqdc import _utils
from nqdc._typing import PathLikeOrStr

_LOG = logging.getLogger(__name__)


def download_articles_for_search_term(
    term: str,
    data_dir: PathLikeOrStr,
    n_docs: Optional[int] = None,
    retmax: int = 500,
    email: Optional[str] = None,
) -> Path:
    data_dir = Path(data_dir)
    output_dir = data_dir.joinpath(f"download-{_utils.hash(term)}")
    if output_dir.is_dir():
        _LOG.warning(f"{output_dir} already exists")
    output_dir.mkdir(exist_ok=True, parents=True)
    info: Dict[str, Any] = {"term": term, "date": datetime.now().isoformat()}
    _LOG.info(f"Downloading data in {output_dir}")
    _LOG.info("Performing search")
    client = EntrezClient(email=email)
    info["search_result"] = client.esearch(term)
    output_dir.joinpath("info.json").write_text(json.dumps(info), "utf-8")
    for i, batch in enumerate(client.efetch(n_docs=n_docs, retmax=retmax)):
        with open(output_dir / f"batch_{i:0>5}.xml", "wb") as f:
            f.write(batch)
    _LOG.info("Finished downloading articles")
    return output_dir
