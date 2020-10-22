package cs441

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging
import cs441.core.{AuthorConsecutiveYears, AuthorVenueCount, CoAuthorCount, PublicationSingleAuthor, PublicationWithHighestAuthors}
import cs441.utils.ConfigReader.getConfigDetails

import scala.jdk.CollectionConverters._

object JobsDriver extends LazyLogging{

  def main(args:Array[String]):Unit={
    val config:Config = getConfigDetails("application.conf")
    val configInputs = config.getConfigList("jobs").asScala
    //if(args.length < 2){
      //logger.error("No input path given for reading the Input File")
      //System.exit(-1)
    //}
    //val inputPath = args(0)
    val inputPath = config.getString("jobInputPath")
    logger.info("Input path received from the from arg input")

    val outputPath =  config.getString("jobOutputPath")
    //val outputPath = args(1)
      configInputs.foreach{configInput =>
        val jobName = configInput.getString("name")
      jobName match {
        case "author_consecutive_years" =>
          logger.info("Job author_consecutive_years")
          val jobOutputPath1 = outputPath + "/" + jobName
          AuthorConsecutiveYears.jobRun(inputPath,jobOutputPath1,jobName)
        case "author_venue_count" =>
          logger.info("Job author_venue_count")
          val jobOutputPath2 = outputPath + "/" + jobName
          AuthorVenueCount.jobRun(inputPath,jobOutputPath2,jobName)
        case "co_author_count_highest_lowest" =>
          logger.info("Job co_author_count_highest_lowest")
          val jobOutputPath3 = outputPath + "/" + jobName
          CoAuthorCount.jobRun(inputPath,jobOutputPath3,jobName)
        case "publications_with_highest_authors" =>
          logger.info("Job publications_with_highest_authors")
          val jobOutputPath4 = outputPath + "/" + jobName
          PublicationWithHighestAuthors.jobRun(inputPath,jobOutputPath4,jobName)
        case "publication_single_author" =>
          logger.info("Job publication_single_author")
          val jobOutputPath5 = outputPath + "/" + jobName
          PublicationSingleAuthor.jobRun(inputPath,jobOutputPath5,jobName)
      }

    }

  }

}
