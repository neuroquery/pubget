#! /usr/bin/env python3

import tempfile
from pathlib import Path
import argparse

from lxml import etree

from nqdc._entrez import EntrezClient

strip_text_xsl = b"""<?xml version="1.0" encoding="UTF-8"?>

<xsl:transform version="1.0"
   xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
   xmlns:mml="http://www.w3.org/1998/Math/MathML"
   xmlns:oasis="http://www.niso.org/standards/z39-96/ns/oasis-exchange/table" >

 <xsl:output method="xml" version="1.0"
           encoding="UTF-8" omit-xml-declaration="no"/>

  <xsl:template match="@*|node()">
    <xsl:copy>
      <xsl:apply-templates select="@*|node()"/>
    </xsl:copy>
  </xsl:template>

  <xsl:template match="abstract">
    <xsl:copy>
    <xsl:text>The abstract of the article</xsl:text>
    </xsl:copy>
  </xsl:template>

  <xsl:template
      match="/pmc-articleset/article/body">
    <xsl:copy>
    <xsl:text>The text of the article with coordinates,
brains, auditory cortex, memory, memory</xsl:text>
    <table-wrap>
    <table>
    <tr><th>X</th><th>Y</th><th>Z</th></tr>
    <tr><td>10</td><td>20</td><td>30</td></tr>
    <tr><td>-10</td><td>-20</td><td>-30</td></tr>
    </table>
    </table-wrap>
    </xsl:copy>
  </xsl:template>

  <xsl:template
      match="/pmc-articleset/article[position()=last()]/body">
    <xsl:copy>
    <xsl:text>The text of the last article
has no coordinates, brains</xsl:text>
    <table-wrap>
    <table>
    <tr><th>a</th><th>b</th></tr>
    <tr><td>10</td><td>20</td></tr>
    <tr><td>-10</td><td>-20</td></tr>
    </table>
    </table-wrap>
    </xsl:copy>
  </xsl:template>

  <xsl:template match="sub-article" />

</xsl:transform>
"""
strip_text_transform = etree.XSLT(etree.fromstring(strip_text_xsl))

parser = argparse.ArgumentParser()
parser.add_argument("-o", "--output_dir", default=".")
args = parser.parse_args()

output_dir = Path(args.output_dir)
output_dir.mkdir(exist_ok=True, parents=True)

client = EntrezClient()
client.esearch("fMRI")
with tempfile.TemporaryDirectory() as tmp_dir:
    client.efetch(tmp_dir, n_docs=7, retmax=7)
    stripped = etree.tostring(
        strip_text_transform(
            etree.parse(str(Path(tmp_dir).joinpath("articleset_00000.xml")))
        ),
        xml_declaration=True,
    )
    output_dir.joinpath("articleset.xml").write_bytes(stripped)
