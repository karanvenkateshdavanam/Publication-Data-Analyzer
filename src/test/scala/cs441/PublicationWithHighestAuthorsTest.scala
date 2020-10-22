package cs441
import cs441.core.{PublicationWithHighestAuthors}
import cs441.utils.ParserUtil
import javax.xml.parsers.SAXParserFactory
import org.scalatest.funsuite.AnyFunSuite

import scala.xml.XML

class PublicationWithHighestAuthorsTest extends AnyFunSuite{
  private val parser = SAXParserFactory.newInstance().newSAXParser()
  private val dtdFilePath = ParserUtil.getDtdFile()

  test("check if title array length  created from xml element is equal to number of author tags"){
    val titleMapper = new PublicationWithHighestAuthors.Map
    val xmlString = "<article mdate=\"2020-06-25\" key=\"tr/meltdown/s18\" publtype=\"informal\">\n <author>Paul Kocher</author>\n <author>Daniel Genkin</author>\n <author>Daniel Gruss</author>\n <author>Werner Haas 0004</author>\n <author>Mike Hamburg</author>\n <author>Moritz Lipp</author>\n <author>Stefan Mangard</author>\n <author>Thomas Prescher 0002</author>\n <author>Michael Schwarz 0001</author>\n <author>Yuval Yarom</author>\n <title>Spectre Attacks: Exploiting Speculative Execution.</title>\n <journal>meltdownattack.com</journal>\n <year>2018</year>\n <ee type=\"oa\">https://spectreattack.com/spectre.pdf</ee>\n </article>"
    val validXML = s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$dtdFilePath"><dblp>$xmlString</dblp>"""
    val valueXML = XML.withSAXParser(parser).loadString(validXML)
    val titleList = titleMapper.getPublicationList(valueXML)

    assert(titleList.length == 10)

  }

}
