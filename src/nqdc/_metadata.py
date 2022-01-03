class MetadataExtractor:
    def __call__(self, article):
        metadata = {}
        for article_id in article.iterfind(
            "/front/article-meta/article-id"
        ):
            self._add_id(article_id, metadata)
        return metadata

    def _add_id(self, article_id, metadata):
        id_type = article_id.get("pub-id-type")
        if id_type not in ["pmc", "pmid", "doi"]:
            return
        if id_type == "pmc":
            id_type = "pmcid"
        value = article_id.text
        if id_type in ["pmid", "pmcid"]:
            value = int(value)
        metadata[id_type] = value
