package cs441.core

import com.typesafe.scalalogging.LazyLogging
import cs441.utils.ParserUtil
import cs441.xml.XmlPublicationInputFormat
import javax.xml.parsers.SAXParserFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import cs441.utils.WriteToCsv

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.xml.{Elem, XML}
import scala.jdk.CollectionConverters._

object AuthorConsecutiveYears extends LazyLogging {

  class Map extends Mapper[LongWritable,Text,Text,IntWritable] with LazyLogging{
    private val parser = SAXParserFactory.newInstance().newSAXParser()
    private val dtdFilePath = ParserUtil.getDtdFile()


    override def map(key:LongWritable, value:Text, context:Mapper[LongWritable,Text,Text,IntWritable]#Context):Unit ={
      val publicationElement = getXmlElement(value.toString)
      val authors = getAuthorList(publicationElement)
      val year =  getPublicationYear(publicationElement)
      if (authors.nonEmpty && year != ""){
        authors.foreach{author =>
          context.write(new Text(author),new IntWritable(year.toInt))
        }
      }

    }

    /**
     * This method gets the valid XML for parsing.
     * @param xmlValue
     * @return
     */
    def getXmlElement(xmlValue: String):Elem ={
      val validXML = s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$dtdFilePath"><dblp>$xmlValue</dblp>"""
      val valueXML = XML.withSAXParser(parser).loadString(validXML)
      valueXML
    }

    /**
     * This method returns a list of authors
     * @param xmlElementAuthor
     * @return
     */
    def getAuthorList(xmlElementAuthor:Elem):ArrayBuffer[String] = {
      val authorArray = new ArrayBuffer[String]()
      var author = ""
      xmlElementAuthor.child.head.label match {
        case "book"|"proceedings" => author = "editor"
        case _ => author = "author"
      }
      (xmlElementAuthor \\ author).foreach{tag =>
        if(tag.text != null){
          authorArray += tag.text
        }
      }
      authorArray
    }

    /**
     * This method returns the publication year
     * @param xmlElementVenue
     * @return
     */
    def getPublicationYear(xmlElementVenue:Elem):String ={
      var publicationYear = ""
      (xmlElementVenue \\ "year").foreach { tag =>
        if (tag.text != null) {
          publicationYear = tag.text
        }
      }
      publicationYear
    }
  }
  class Reduce extends Reducer[Text,IntWritable,Text,IntWritable] with LazyLogging{
    override def reduce(key:Text,values: java.lang.Iterable[IntWritable],context: Reducer[Text,IntWritable,Text,IntWritable]#Context):Unit={
      val scalaValues = values.asScala
      val ranges = mutable.ArrayBuffer[Int](1)
      val intArray = mutable.ArrayBuffer[Int]()
      scalaValues.foreach{vp =>
          intArray.addOne(vp.get())
      }

      intArray.toSeq.sorted.distinct.sliding(2).foreach{
        case y1 :: tail =>
          if(tail.nonEmpty){
            val y2 = tail.head
            if(y2 - y1 == 1) ranges(ranges.size - 1) += 1
            else ranges += 1
          }
      }
      if (ranges.max >= 10){
        context.write(key,new IntWritable(ranges.max))
      }



    }
  }

  def jobRun(input:String,output:String,name:String): Unit = {
    val configuration = new Configuration
    configuration.set("mapred.textoutputformat.separator", ",")
    val job = Job.getInstance(configuration,name)
    job.setJarByClass(AuthorConsecutiveYears.getClass)
    job.setMapperClass(classOf[AuthorConsecutiveYears.Map])
    job.setReducerClass(classOf[AuthorConsecutiveYears.Reduce])
    job.setInputFormatClass(classOf[XmlPublicationInputFormat])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])
    FileInputFormat.addInputPath(job, new Path(input))
    FileOutputFormat.setOutputPath(job, new Path(output))
    if(job.waitForCompletion(true)){
      logger.info("Job Authors published for more than 10 consecutive years completed successfully")
      WriteToCsv.writeCsv(output)
    }else{
      System.exit(1)
    }
  }

}
