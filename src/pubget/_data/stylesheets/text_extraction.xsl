<?xml version="1.0" encoding="UTF-8"?>

<xsl:transform version="1.0"
  xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
  xmlns:mml="http://www.w3.org/1998/Math/MathML"
  xmlns:oasis="http://www.niso.org/standards/z39-96/ns/oasis-exchange/table" >

  <xsl:output method="xml" version="1.0" encoding="UTF-8" omit-xml-declaration="no"/>
  <xsl:strip-space elements="*"/>

  <xsl:template match="/">
    <extracted-text>
      <pmcid>
        <xsl:value-of select="/article/front /article-meta/article-id[@pub-id-type='pmc']"/>
      </pmcid>
      <title>
        <xsl:value-of select="/article/front/article-meta/title-group/article-title"/>
      </title>
      <keywords>
        <xsl:apply-templates select="/article/front/article-meta/kwd-group" mode="keywords"/>
      </keywords>
      <abstract>
        <xsl:apply-templates select="/article/front/article-meta/abstract" />
      </abstract>
      <body>
        <xsl:apply-templates select="/article/body" />
      </body>
    </extracted-text>
  </xsl:template>

  <xsl:template match="*" >
    <xsl:text> </xsl:text>
    <xsl:apply-templates />
    <xsl:text> </xsl:text>
  </xsl:template>

  <xsl:template match="/article/front/article-meta/abstract/title[text()='Abstract']" />

<xsl:template match="p|sec" >
<xsl:text>
</xsl:text>
<xsl:apply-templates />
<xsl:text>
</xsl:text>
</xsl:template>


<xsl:template name="start-heading">
  <xsl:param name="n"/>
  <xsl:if test="$n &gt; 0">
    <xsl:text>#</xsl:text>
    <xsl:call-template name="start-heading">
      <xsl:with-param name="n" select="$n - 1"/>
    </xsl:call-template>
  </xsl:if>
</xsl:template>

<xsl:template match="sec/title" >
  <xsl:call-template name="start-heading">
    <xsl:with-param name="n" select="count(ancestor::sec) + 1"/>
  </xsl:call-template>
  <xsl:text> </xsl:text>
  <xsl:apply-templates />
  <xsl:text>
  </xsl:text>
</xsl:template>

<xsl:template match="text()" >
  <xsl:copy-of select="."/>
  <xsl:text> </xsl:text>
</xsl:template>

  <xsl:template match="text() | comment() | processing-instruction()"
                mode="keywords"/>

  <xsl:template match="kwd" mode="keywords">
    <xsl:value-of select="." />
    <xsl:text>
</xsl:text>
  </xsl:template>

  <xsl:template match="*[local-name() = free_to_read]" />
  <xsl:template match="*[local-name() = license_ref]" />
  <xsl:template match="abbrev" />
  <xsl:template match="abbrev-journal-title" />
  <xsl:template match="access-date" />
  <xsl:template match="ack" />
  <xsl:template match="addr-line" />
  <xsl:template match="address" />
  <xsl:template match="aff" />
  <xsl:template match="aff-alternatives" />
  <xsl:template match="anonymous" />
  <xsl:template match="app" />
  <xsl:template match="app-group" />
  <xsl:template match="array" />
  <xsl:template match="article-id" />
  <xsl:template match="attrib" />
  <xsl:template match="author-comment" />
  <xsl:template match="author-notes" />
  <xsl:template match="award-group" />
  <xsl:template match="award-id" />
  <xsl:template match="back" />
  <xsl:template match="bio" />
  <xsl:template match="break" />
  <xsl:template match="chem-struct" />
  <xsl:template match="chem-struct-wrap" />
  <xsl:template match="citation-alternatives" />
  <xsl:template match="city" />
  <xsl:template match="code" />
  <xsl:template match="col" />
  <xsl:template match="colgroup" />
  <xsl:template match="collab" />
  <xsl:template match="collab-alternatives" />
  <xsl:template match="comment" />
  <xsl:template match="compound-kwd" />
  <xsl:template match="compound-kwd-part" />
  <xsl:template match="compound-subject" />
  <xsl:template match="compound-subject-part" />
  <xsl:template match="conf-acronym" />
  <xsl:template match="conf-date" />
  <xsl:template match="conf-loc" />
  <xsl:template match="conf-name" />
  <xsl:template match="conf-num" />
  <xsl:template match="conf-sponsor" />
  <xsl:template match="conf-theme" />
  <xsl:template match="conference" />
  <xsl:template match="contrib" />
  <xsl:template match="contrib-group" />
  <xsl:template match="contrib-id" />
  <xsl:template match="copyright-holder" />
  <xsl:template match="copyright-statement" />
  <xsl:template match="copyright-year" />
  <xsl:template match="corresp" />
  <xsl:template match="count" />
  <xsl:template match="country" />
  <xsl:template match="counts" />
  <xsl:template match="custom-meta" />
  <xsl:template match="custom-meta-group" />
  <xsl:template match="data-title" />
  <xsl:template match="date" />
  <xsl:template match="date-in-citation" />
  <xsl:template match="day" />
  <xsl:template match="def-head" />
  <xsl:template match="degrees" />
  <xsl:template match="disp-formula" />
  <xsl:template match="disp-formula-group" />
  <xsl:template match="edition" />
  <xsl:template match="element-citation" />
  <xsl:template match="elocation-id" />
  <xsl:template match="email" />
  <xsl:template match="equation-count" />
  <xsl:template match="era" />
  <xsl:template match="etal" />
  <xsl:template match="ext-link" />
  <xsl:template match="fax" />
  <xsl:template match="fig-count" />
  <xsl:template match="fn" />
  <xsl:template match="fn-group" />
  <xsl:template match="fpage" />
  <xsl:template match="front-stub" />
  <xsl:template match="funding-group" />
  <xsl:template match="funding-source" />
  <xsl:template match="funding-statement" />
  <xsl:template match="given-names" />
  <xsl:template match="glossary" />
  <xsl:template match="glyph-data" />
  <xsl:template match="glyph-ref" />
  <xsl:template match="gov" />
  <xsl:template match="graphic" />
  <xsl:template match="history" />
  <xsl:template match="hr" />
  <xsl:template match="inline-formula" />
  <xsl:template match="inline-graphic" />
  <xsl:template match="inline-supplementary-material" />
  <xsl:template match="institution" />
  <xsl:template match="institution-id" />
  <xsl:template match="institution-wrap" />
  <xsl:template match="isbn" />
  <xsl:template match="issn" />
  <xsl:template match="issn-l" />
  <xsl:template match="issue" />
  <xsl:template match="issue-id" />
  <xsl:template match="issue-part" />
  <xsl:template match="issue-sponsor" />
  <xsl:template match="issue-title" />
  <xsl:template match="journal-id" />
  <xsl:template match="journal-meta" />
  <xsl:template match="journal-subtitle" />
  <xsl:template match="journal-title" />
  <xsl:template match="journal-title-group" />
  <xsl:template match="label" />
  <xsl:template match="license" />
  <xsl:template match="license-p" />
  <xsl:template match="media" />
  <xsl:template match="meta-name" />
  <xsl:template match="meta-value" />
  <xsl:template match="milestone-end" />
  <xsl:template match="milestone-start" />
  <xsl:template match="mixed-citation" />
  <xsl:template match="mml:math" />
  <xsl:template match="month" />
  <xsl:template match="name" />
  <xsl:template match="name-alternatives" />
  <xsl:template match="named-content" />
  <xsl:template match="nlm-citation" />
  <xsl:template match="note" />
  <xsl:template match="notes" />
  <xsl:template match="oasis:table" />
  <xsl:template match="object-id" />
  <xsl:template match="on-behalf-of" />
  <xsl:template match="open-access" />
  <xsl:template match="overline-end" />
  <xsl:template match="overline-start" />
  <xsl:template match="page-count" />
  <xsl:template match="page-range" />
  <xsl:template match="patent" />
  <xsl:template match="permissions" />
  <xsl:template match="person-group" />
  <xsl:template match="phone" />
  <xsl:template match="postal-code" />
  <xsl:template match="prefix" />
  <xsl:template match="price" />
  <xsl:template match="principal-award-recipient" />
  <xsl:template match="principal-investigator" />
  <xsl:template match="private-char" />
  <xsl:template match="product" />
  <xsl:template match="pub-date" />
  <xsl:template match="pub-id" />
  <xsl:template match="publisher" />
  <xsl:template match="publisher-loc" />
  <xsl:template match="publisher-name" />
  <xsl:template match="rb" />
  <xsl:template match="ref" />
  <xsl:template match="ref-count" />
  <xsl:template match="ref-list" />
  <xsl:template match="related-article" />
  <xsl:template match="related-object" />
  <xsl:template match="response" />
  <xsl:template match="role" />
  <xsl:template match="roman" />
  <xsl:template match="rp" />
  <xsl:template match="rt" />
  <xsl:template match="ruby" />
  <xsl:template match="sc" />
  <xsl:template match="season" />
  <xsl:template match="sec-meta" />
  <xsl:template match="self-uri" />
  <xsl:template match="series" />
  <xsl:template match="series-text" />
  <xsl:template match="series-title" />
  <xsl:template match="sig" />
  <xsl:template match="sig-block" />
  <xsl:template match="size" />
  <xsl:template match="source" />
  <xsl:template match="speaker" />
  <xsl:template match="state" />
  <xsl:template match="std" />
  <xsl:template match="std-organization" />
  <xsl:template match="strike" />
  <xsl:template match="string-conf" />
  <xsl:template match="string-date" />
  <xsl:template match="string-name" />
  <xsl:template match="sub" />
  <xsl:template match="suffix" />
  <xsl:template match="sup" />
  <xsl:template match="supplement" />
  <xsl:template match="supplementary-material" />
  <xsl:template match="surname" />
  <xsl:template match="table" />
  <xsl:template match="table-count" />
  <xsl:template match="target" />
  <xsl:template match="tbody" />
  <xsl:template match="td" />
  <xsl:template match="term-head" />
  <xsl:template match="tex-math" />
  <xsl:template match="tfoot" />
  <xsl:template match="th" />
  <xsl:template match="thead" />
  <xsl:template match="time-stamp" />
  <xsl:template match="tr" />
  <xsl:template match="trans-abstract" />
  <xsl:template match="trans-source" />
  <xsl:template match="trans-subtitle" />
  <xsl:template match="trans-title" />
  <xsl:template match="trans-title-group" />
  <xsl:template match="underline-end" />
  <xsl:template match="underline-start" />
  <xsl:template match="uri" />
  <xsl:template match="version" />
  <xsl:template match="volume" />
  <xsl:template match="volume-id" />
  <xsl:template match="volume-issue-group" />
  <xsl:template match="volume-series" />
  <xsl:template match="word-count" />
  <xsl:template match="x" />
  <xsl:template match="xref" />
  <xsl:template match="year" />
</xsl:transform>
