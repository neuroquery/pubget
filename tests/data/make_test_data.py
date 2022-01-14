from pathlib import Path
import argparse

from lxml import etree

from nqdc._entrez import EntrezClient

strip_text_xsl = b"""<?xml version="1.0" encoding="UTF-8"?>

<xsl:transform version="1.0"
               xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
               xmlns:mml="http://www.w3.org/1998/Math/MathML"
               xmlns:oasis="http://www.niso.org/standards/z39-96/ns/oasis-exchange/table" >

  <xsl:output method="xml" version="1.0" encoding="UTF-8" omit-xml-declaration="no"/>

  <xsl:template match="@*|node()">
    <xsl:copy>
      <xsl:apply-templates select="@*|node()"/>
    </xsl:copy>
  </xsl:template>

  <xsl:template match="body">
    <xsl:copy>
    <xsl:text>The text of the article</xsl:text>
    </xsl:copy>
  </xsl:template>

</xsl:transform>
"""
strip_text_transform = etree.XSLT(etree.fromstring(strip_text_xsl))

parser = argparse.ArgumentParser()
parser.add_argument("output_dir")
args = parser.parse_args()

output_dir = Path(args.output_dir)
output_dir.mkdir(exist_ok=True, parents=True)

client = EntrezClient()
client.esearch("fMRI")
for i, batch in enumerate(client.efetch(n_docs=7, retmax=3)):
    stripped = etree.tostring(
        strip_text_transform(etree.fromstring(batch)), xml_declaration=True
    )
    output_dir.joinpath(f"batch_{i}.xml").write_bytes(stripped)
