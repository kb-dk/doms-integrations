<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet
        xmlns:dobundle="http://ecm.sourceforge.net/types/digitalobjectbundle/0/2/#"
        xmlns:foxml="info:fedora/fedora-system:def/foxml#"
        xmlns:pbcore="http://www.pbcore.org/PBCore/PBCoreNamespace.html"
        xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
        xmlns:Index="http://statsbiblioteket.dk/summa/2008/Document"
        xmlns:xs="http://www.w3.org/2001/XMLSchema-instance"
        xmlns:xalan="http://xml.apache.org/xalan"
        xmlns:java="http://xml.apache.org/xalan/java"
        xmlns:mc="http://www.loc.gov/MARC21/slim"
        exclude-result-prefixes="java xs xalan xsl mc" version="1.0">
    <xsl:include href="domsRadioTV_short_format.xsl"/>
    <xsl:output version="1.0" encoding="UTF-8" indent="yes" method="xml"/>
    <xsl:template match="/">
        <Index:SummaDocument version="1.0" Index:boost="1">
            <xsl:attribute name="Index:id"><!-- Optional ? -->
                <xsl:value-of select="dobundle:digitalObjectBundle/foxml:digitalObject/@PID"/>
            </xsl:attribute>
            <xsl:for-each select="dobundle:digitalObjectBundle">
                <Index:fields>

                    <xsl:for-each select="foxml:digitalObject/
                            foxml:datastream[@ID='PBCORE']/
                            foxml:datastreamVersion[last()]/
                            foxml:xmlContent/
                            pbcore:PBCoreDescriptionDocument/
                            pbcore:pbcoreTitle">
                        <Index:field Index:name="titel"><xsl:value-of
                                select="pbcore:title"/></Index:field>
                    </xsl:for-each>
                    <xsl:for-each select="foxml:digitalObject/
                            foxml:datastream[@ID='PBCORE']/
                            foxml:datastreamVersion[last()]/
                            foxml:xmlContent/
                            pbcore:PBCoreDescriptionDocument/
                            pbcore:pbcoreDescription">
                        <Index:field Index:name="freetext"><xsl:value-of
                                select="pbcore:description"/></Index:field>
                    </xsl:for-each>
                    <xsl:call-template name="doms_shortformat"/>
                </Index:fields>
            </xsl:for-each>
        </Index:SummaDocument>
    </xsl:template>
</xsl:stylesheet>