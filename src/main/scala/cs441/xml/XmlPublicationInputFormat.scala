package cs441.xml

import java.io.IOException
import java.nio.charset.StandardCharsets

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.io.{DataOutputBuffer, LongWritable, Text}
import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}

import scala.collection.mutable.ArrayBuffer

class XmlPublicationInputFormat extends TextInputFormat with LazyLogging{

  override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[LongWritable, Text] = new XmlPublicationRecordReader

  class XmlPublicationRecordReader extends RecordReader[LongWritable,Text]{
    private val startTags = new ArrayBuffer[Array[Byte]]
    private val endTags = new ArrayBuffer[Array[Byte]]
    private var start = 0L
    private var end = 0L
    private var fsin:FSDataInputStream = null
    private val buffer:DataOutputBuffer = new DataOutputBuffer()
    private val key:LongWritable = new LongWritable()
    private val value:Text = new Text()

    @throws[IOException]
    override def initialize(inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext): Unit ={
      val fileSplit = inputSplit.asInstanceOf[FileSplit]
      val conf = taskAttemptContext.getConfiguration
      val multiTagList:List[(String,String)] = List(("<phdthesis ", "</phdthesis>"), ("<mastersthesis ", "</mastersthesis>"), ("<article ", "</article>"), ("<inproceedings ", "</inproceedings>"), ("<proceedings ", "</proceedings>"), ("<book ", "</book>"), ("<incollection ", "</incollection>"))
      multiTagList.map{tuple_value=>
        startTags.addOne(tuple_value._1.getBytes(StandardCharsets.UTF_8))
        endTags.addOne(tuple_value._2.getBytes(StandardCharsets.UTF_8))
      }
      start = fileSplit.getStart
      end = start + fileSplit.getLength
      val file = fileSplit.getPath
      val fs = file.getFileSystem(conf)
      fsin = fs.open(fileSplit.getPath)
      fsin.seek(start)

    }

    @throws[IOException]
    override def getCurrentKey: LongWritable = key

    @throws[IOException]
    override def getCurrentValue: Text = value

    @throws[IOException]
    override def close(): Unit = fsin.close()

    @throws[IOException]
    override def getProgress: Float = (fsin.getPos - start) / (end - start).toFloat

    @throws[IOException]
    override def nextKeyValue(): Boolean = {
      if(fsin.getPos < end){
        val matchedIndex = readUntilMatch(startTags,false)
        if(matchedIndex != -1)try{
          buffer.write(startTags(matchedIndex))
          if(readUntilMatch(endTags(matchedIndex),true)){
            key.set(fsin.getPos)
            value.set(buffer.getData,0,buffer.getLength)
            return true
          }
        }finally{
          buffer.reset()
        }
      }
      return false

    }


    @throws[IOException]
    def readUntilMatch(matcher: ArrayBuffer[Array[Byte]], withinBlock: Boolean): Int = {
      val defaultMatchedIndices = new ArrayBuffer[Integer]
      for (j <- 0 to startTags.size - 1) {
        defaultMatchedIndices.addOne(j)
      }
      var matchedIndices = defaultMatchedIndices
      var i = 0
      while (true) {
        val b = fsin.read()

        if (b == -1) return -1

        if (withinBlock) buffer.write(b)
        val tempMatchedIndices = new ArrayBuffer[Integer]


        var iFlag = false

        matchedIndices.foreach(index => {
          if (b == startTags(index)(i)) {
            iFlag = true
            tempMatchedIndices.addOne(index)
          }
        })

        if (iFlag) {
          i += 1

          tempMatchedIndices.foreach(matchIndex => {
            if (i >= startTags(matchIndex).length) {
              return matchIndex
            }
          })
        }
        else i = 0
        if (tempMatchedIndices.size == 0) matchedIndices = defaultMatchedIndices
        else matchedIndices = tempMatchedIndices

        if (!withinBlock && i == 0 && fsin.getPos >= end) return -1
      }
      return -1
    }

    @throws[IOException]
    def readUntilMatch(matcher: Array[Byte], withinBlock: Boolean): Boolean = {
      var i = 0
      while (true) {
        val b = fsin.read()
        if (b == -1) return false
        if (withinBlock) buffer.write(b)

        if (b == matcher(i)) {
          i += 1
          if (i >= matcher.length) return true
        }
        else i = 0
        if (!withinBlock && i == 0 && fsin.getPos >= end) return false
      }
      true
    }
  }


}
