package cs441
import cs441.core.PublicationSingleAuthor
import cs441.utils.ParserUtil
import javax.xml.parsers.SAXParserFactory
import org.scalatest.funsuite.AnyFunSuite

import scala.xml.XML

class PublicationSingleAuthorTest extends AnyFunSuite{
  private val parser = SAXParserFactory.newInstance().newSAXParser()
  private val dtdFilePath = ParserUtil.getDtdFile()

  test("check if correct publication name/title is extracted from xml element for single authors"){
    val nameMapper = new PublicationSingleAuthor.Map
    val xmlString = "<phdthesis mdate=\"2017-04-25\" key=\"phd/Ghemawat95\">\n <author>Sanjay Ghemawat</author>\n  <title>The Modified Object Buffer: A Storage Management Technique for Object-Oriented Databases</title>\n <year>1995</year>\n <school>MIT Laboratory for Computer Science, Cambridge, MA, USA</school>\n <ee>http://hdl.handle.net/1721.1/37012</ee>\n </phdthesis>"
    val validXML = s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$dtdFilePath"><dblp>$xmlString</dblp>"""
    val valueXML = XML.withSAXParser(parser).loadString(validXML)
    val title = nameMapper.getPublicationName(valueXML)

    assert(title == "The Modified Object Buffer: A Storage Management Technique for Object-Oriented Databases")

  }

  test("check if no name/title is extracted from xml element for multiple authors"){
    val nameListMapper = new PublicationSingleAuthor.Map
    val xmlString = "<article mdate=\"2020-06-25\" key=\"tr/meltdown/s18\" publtype=\"informal\">\n <author>Paul Kocher</author>\n <author>Daniel Genkin</author>\n <author>Daniel Gruss</author>\n <author>Werner Haas 0004</author>\n <author>Mike Hamburg</author>\n <author>Moritz Lipp</author>\n <author>Stefan Mangard</author>\n <author>Thomas Prescher 0002</author>\n <author>Michael Schwarz 0001</author>\n <author>Yuval Yarom</author>\n <title>Spectre Attacks: Exploiting Speculative Execution.</title>\n <journal>meltdownattack.com</journal>\n <year>2018</year>\n <ee type=\"oa\">https://spectreattack.com/spectre.pdf</ee>\n </article>"
    val validXML = s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$dtdFilePath"><dblp>$xmlString</dblp>"""
    val valueXML = XML.withSAXParser(parser).loadString(validXML)
    val title = nameListMapper.getPublicationName(valueXML)

    assert(title == "")

  }

}
