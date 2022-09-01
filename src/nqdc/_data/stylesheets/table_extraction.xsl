<?xml version="1.0"?>
<xsl:transform version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform" >

  <xsl:include href="docbook/xhtml-1_1/docbook.xsl"/>

  <xsl:output method="xml" version="1.0" encoding="UTF-8" omit-xml-declaration="no" />

  <xsl:strip-space elements="*"/>

  <xsl:template match="/">
    <extracted-tables-set>
      <pmcid>
        <xsl:value-of
            select="/article/front/
                    article-meta/article-id[@pub-id-type='pmc']"/>
      </pmcid>
      <all-ids>
        <xsl:copy-of select="/article/front/article-meta/article-id"/>
      </all-ids>
      <xsl:apply-templates mode="extract-tables"/>
    </extracted-tables-set>
  </xsl:template>

  <xsl:template match="text() | comment() | processing-instruction()"
                mode="extract-tables"/>

  <xsl:template match="table-wrap" mode="extract-tables">
    <extracted-table>
      <table-id>
        <xsl:value-of select="attribute::id"/>
      </table-id>
      <table-label>
        <xsl:value-of select="label"/>
      </table-label>
      <table-caption>
        <xsl:value-of select="caption"/>
      </table-caption>
      <original-table>
        <xsl:copy-of select="."/>
      </original-table>
      <transformed-table>
        <xsl:apply-templates select="."/>
      </transformed-table>
    </extracted-table>
  </xsl:template>

  <xsl:template match="*">
    <xsl:apply-templates/>
  </xsl:template>

</xsl:transform>
