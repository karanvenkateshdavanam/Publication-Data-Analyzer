package cs441.core

import com.typesafe.scalalogging.LazyLogging
import cs441.utils.{ParserUtil, WriteToCsv}
import cs441.xml.XmlPublicationInputFormat
import javax.xml.parsers.SAXParserFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}

import scala.collection.mutable.ArrayBuffer
import scala.xml.{Elem, XML}
import scala.jdk.CollectionConverters._
import scala.collection.mutable

object PublicationSingleAuthor extends LazyLogging {

  class Map extends Mapper[LongWritable,Text,Text,Text] with LazyLogging{
    private val parser = SAXParserFactory.newInstance().newSAXParser()
    private val dtdFilePath = ParserUtil.getDtdFile()


    override def map(key:LongWritable, value:Text, context:Mapper[LongWritable,Text,Text,Text]#Context):Unit ={
      val publicationElement = getXmlElement(value.toString)
      val publicationTitle = getPublicationName(publicationElement)
      val venue =  getVenue(publicationElement)
      if (venue != "" && publicationTitle != ""){
          context.write(new Text(venue),new Text(publicationTitle))
      }

    }

    /**
     * Returns a valid xml element with dtd for validity checking
     * @param xmlValue
     * @return
     */
    def getXmlElement(xmlValue: String):Elem ={
      val validXML = s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$dtdFilePath"><dblp>$xmlValue</dblp>"""
      val valueXML = XML.withSAXParser(parser).loadString(validXML)
      valueXML
    }

    /**
     * Return the name in the title tag for each element.
     * @param xmlElementAuthor
     * @return
     */
    def getPublicationName(xmlElementAuthor:Elem):String = {
      val authorArray = new ArrayBuffer[String]()
      var author = ""
      var title = ""
      xmlElementAuthor.child.head.label match {
        case "book"|"proceedings" => author = "editor"
        case _ => author = "author"
      }
      (xmlElementAuthor \\ author).foreach{tag =>
        if(tag.text != null){
          authorArray += tag.text
        }
      }
      if(authorArray.length == 1){
        (xmlElementAuthor \\ "title").foreach{ name =>
          if (name.text != null) {
            title = name.text
          }
        }
      }
      title
    }

    /**
     * Returns the venue name for each xml element given as input.
     * @param xmlElementVenue
     * @return
     */
    def getVenue(xmlElementVenue:Elem):String ={
      var venue = ""
      var venueName = ""
      xmlElementVenue.child.head.label match {
        case "article" => venue = "journal"
        case "inproceedings" => venue = "booktitle"
        case "proceedings" => venue = "booktitle"
        case "book" => venue = "publisher"
        case "incollection" => venue = "booktitle"
        case "phdthesis" => venue = "school"
        case "mastersthesis" => venue = "school"
      }
      (xmlElementVenue \\ venue).foreach { tag =>
        if (tag.text != null) {
          venueName = tag.text
        }
      }
      venueName
    }
  }
  class Reduce extends Reducer[Text,Text,Text,Text] with LazyLogging{
    override def reduce(key:Text,values: java.lang.Iterable[Text],context: Reducer[Text,Text,Text,Text]#Context):Unit={
      val scalaValues = values.asScala
      scalaValues.foreach{value =>
        context.write(key,value)
      }
    }
  }

  def jobRun(input:String,output:String,name:String): Unit = {
    val configuration = new Configuration
    configuration.set("mapred.textoutputformat.separator", ",")
    val job = Job.getInstance(configuration,name)
    job.setJarByClass(PublicationSingleAuthor.getClass)
    job.setMapperClass(classOf[PublicationSingleAuthor.Map])
    job.setReducerClass(classOf[PublicationSingleAuthor.Reduce])
    job.setInputFormatClass(classOf[XmlPublicationInputFormat])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    FileInputFormat.addInputPath(job, new Path(input))
    FileOutputFormat.setOutputPath(job, new Path(output))
    if(job.waitForCompletion(true)){
      logger.info("Job publication with one author completed")
      WriteToCsv.writeCsv(output)
    }else{
      System.exit(1)
    }
  }

}
