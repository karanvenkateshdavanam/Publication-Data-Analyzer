package cs441.utils

import com.typesafe.config._

/**
 *Single instance which provides a method to read configuration file details
 */
object ConfigReader {
  /**
   *
   * @param configFileName config file name
   * @return static instance of Config
   */
  def getConfigDetails(configFileName:String):Config={
    ConfigFactory.load(configFileName)
  }



}
