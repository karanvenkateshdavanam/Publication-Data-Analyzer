package cs441.utils

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.IOUtils

object WriteToCsv extends LazyLogging{
  /**
   * Utility function to read the data from hdfs output file and write to csv file
   * @param filePath
   */
  def writeCsv(filePath:String):Unit={
    val conf = new Configuration()
    val fs = FileSystem.get(conf)
    val status: Array[FileStatus] = fs.listStatus(new Path(filePath))
    val files = status.filter(f => f.getPath.getName.startsWith("part-"))
    val hdfsPath = new Path(filePath + ".csv")
    val outStream = fs.create(hdfsPath)
    for (k <- files.indices) {
      val stream = fs.open(files(k).getPath)
      IOUtils.copyBytes(stream,outStream,conf,false)
    }
    logger.info("Finished copying data from normal output file to csv file")
    outStream.close()
    fs.close()
  }
}
