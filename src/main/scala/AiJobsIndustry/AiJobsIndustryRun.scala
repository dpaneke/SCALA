package AiJobsIndustry

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

import scala.util.matching.Regex

object AiJobsIndustryRun
  extends App
    with Context
    with AiJobsIndustryTransformationsDf
    with AiJobsIndustryCalculationsDf
    with AiJobsIndustryTransformationsDs
    with AiJobsIndustryCalculationsDs {

  override val appName: String = "AiJobsIndustry"

  import spark.implicits._

  val aiJobsIndustrySchema = StructType(Seq(
    StructField("JobTitle", StringType),
    StructField("Company", StringType),
    StructField("Location", StringType),
    StructField("CompanyReviews", StringType),
    StructField("Link", StringType)))

  val aiJobsIndustryData: DataFrame =
    spark.read
      .option("multiLine", "true")
      .option("header", "true")
      .option("delimiter", ",")
      .schema(aiJobsIndustrySchema)
      .csv("src/main/resources/AIJobsIndustry.csv")

  val aiJobsIndustryDf = aiJobsIndustryData
    .transform(extractJobReviews)
    .transform(dropNaData)
    .transform(withTrimmedCapitalizedColumns)
    .transform(withReviewsInt)
    .transform(
      withDistinctRecords(
        col("JobTitle"),
        col("Company"),
        col("Location"),
        col("reviewsInt")))

  val statsCalculationDf = aiJobsIndustryDf.transform(calcStatsTotal)

  val aiJobsIndustryRawDs: Dataset[AiJobsRaw] = aiJobsIndustryData.as[AiJobsRaw]

  val aiJobsIndustryDs = aiJobsIndustryRawDs
    .transform(dropNaDataDs)
    .transform(withTrimmedCapitalizedColumnsDs)
    .transform(convertToAiJobs)
    .transform(withDistinctRecordsDs)

  val statsCalculationDs = aiJobsIndustryDs.transform(calcStatsTotalDs)

  statsCalculationDf.show()
  statsCalculationDs.show()
}

