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

object CoAuthorCount extends LazyLogging {

  class Map extends Mapper[LongWritable,Text,Text,Text] with LazyLogging{
    private val parser = SAXParserFactory.newInstance().newSAXParser()
    private val dtdFilePath = ParserUtil.getDtdFile()


    override def map(key:LongWritable, value:Text, context:Mapper[LongWritable,Text,Text,Text]#Context):Unit ={
      val publicationElement = getXmlElement(value.toString)
      val author_list = getAuthorCoList(publicationElement)
      if(author_list.length == 1){
        context.write(new Text(author_list.head),new Text(""))
      }
      else{
        if (author_list.nonEmpty){
          author_list.foreach{author =>
            author_list.filter( _ != author).foreach{ co_author =>
              context.write(new Text(author),new Text(co_author))
            }
          }
        }
      }
    }

    /**
     * Returns valid XML in correct format with the dtd file validity checking
     * @param xmlValue
     * @return
     */
    def getXmlElement(xmlValue: String):Elem ={
      val validXML = s"""<?xml version="1.0" encoding="ISO-8859-1"?><!DOCTYPE dblp SYSTEM "$dtdFilePath"><dblp>$xmlValue</dblp>"""
      val valueXML = XML.withSAXParser(parser).loadString(validXML)
      valueXML
    }

    /**
     * Returns the list of co authors
     * @param xmlElementAuthor
     * @return
     */
    def getAuthorCoList(xmlElementAuthor:Elem):ArrayBuffer[String] = {
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

  }
  class Reduce extends Reducer[Text,Text,Text,IntWritable] with LazyLogging{
    private val authorCoHashMap = new mutable.HashMap[String, Int]()
    override def reduce(key:Text,values: java.lang.Iterable[Text],context: Reducer[Text,Text,Text,IntWritable]#Context):Unit={
      val authorCoSet =  scala.collection.mutable.Set[String]()
      val coAuthorList = values.asScala
      var coAuthorCount = 0
      coAuthorList.foreach{authorValue => authorCoSet += authorValue.toString}
      authorCoSet.foreach{coAuthorName =>
        if(coAuthorName == ""){
          coAuthorCount += 0
        }else{
          coAuthorCount += 1
        }
      }
      authorCoHashMap += (key.toString -> coAuthorCount)
    }

    override def cleanup(context: Reducer[Text, Text, Text, IntWritable]#Context): Unit ={
      val authorCoHashMapSorted = authorCoHashMap.toSeq.sortWith(_._2 > _._2).take(100)
      val authorCoHashMapSortedLast = authorCoHashMap.toSeq.sortWith(_._2 > _._2).takeRight(100)
      authorCoHashMapSorted.foreach(authorTuple => context.write(new Text(authorTuple._1),new IntWritable(authorTuple._2)))
      authorCoHashMapSortedLast.foreach(authorTuple => context.write(new Text(authorTuple._1),new IntWritable(authorTuple._2)))

    }


  }

  def jobRun(input:String,output:String,name:String): Unit = {
    val configuration = new Configuration
    configuration.set("mapred.textoutputformat.separator", ",")
    val job = Job.getInstance(configuration,name)
    job.setNumReduceTasks(1)
    job.setJarByClass(CoAuthorCount.getClass)
    job.setMapperClass(classOf[CoAuthorCount.Map])
    job.setReducerClass(classOf[CoAuthorCount.Reduce])
    job.setInputFormatClass(classOf[XmlPublicationInputFormat])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])
    FileInputFormat.addInputPath(job, new Path(input))
    FileOutputFormat.setOutputPath(job, new Path(output))
    //System.exit(if(job.waitForCompletion(true))  0 else 1)
    if(job.waitForCompletion(true)){
      logger.info("Job co-author top 100 with co-author and list of 100 with no co-author completed")
      WriteToCsv.writeCsv(output)
      System.exit(0)
    }else{
      System.exit(1)
    }
  }

}

