package cs441.utils

import com.typesafe.scalalogging.LazyLogging

object ParserUtil extends LazyLogging{

  /**
   * Utility function to fetch the dtd file path for validating the input xml string.
   * @return
   */
  def getDtdFile():String={
    getClass.getClassLoader.getResource("dblp.dtd").toURI.toString
  }
}
