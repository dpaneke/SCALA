package AiJobsIndustry

import org.apache.spark.sql.{Dataset, SparkSession}

import scala.util.matching.Regex

trait AiJobsIndustryTransformationsDs {
  val spark: SparkSession

  import spark.implicits._

  def dropNaDataDs(ds: Dataset[AiJobsRaw]): Dataset[AiJobsRaw] =
    ds.filter(record =>
      record.jobTitle.nonEmpty &
        record.company.nonEmpty &
        record.location.nonEmpty &
        record.companyReviews.nonEmpty)

  def convertToAiJobs(ds: Dataset[AiJobsRaw]): Dataset[AiJobs] =
    ds.map(record => {
      val numberPattern: Regex = raw"(,|\.| |\d)*".r
      val replacePattern: Regex = raw"([,. ])".r

      val numberExtraCharacters: String = numberPattern
        .findFirstIn(record.companyReviews.get)
        .get

      val numberStr: String = replacePattern
        .replaceAllIn(numberExtraCharacters, "")

      val reviewsInt: Int = numberStr match {
        case "" => 0
        case num => num.toInt
      }

      AiJobs(
        record.jobTitle.get,
        record.company.get,
        record.location.get,
        reviewsInt)
    })

  def withTrimmedCapitalizedColumnsDs(ds: Dataset[AiJobsRaw]): Dataset[AiJobsRaw] =
    ds.map(record => {
      val trimStrVal: String => String = origStrVal =>
        origStrVal
          .replace('\n', ' ')
          .trim()

      val capitalizeStrVal: String => String = origStrVal =>
        origStrVal
          .split(' ')
          .map(_.capitalize)
          .mkString(" ")

      val processStrVal: String => String = origStrVal =>
        capitalizeStrVal(trimStrVal(origStrVal))

      AiJobsRaw(
        Some(processStrVal(record.jobTitle.get)),
        Some(processStrVal(record.company.get)),
        Some(processStrVal(record.location.get)),
        Some(processStrVal(record.companyReviews.get)),
        Some(processStrVal(record.link.get)))
    })

  def withDistinctRecordsDs(ds: Dataset[AiJobs]): Dataset[AiJobs] =
    ds.distinct()
}
