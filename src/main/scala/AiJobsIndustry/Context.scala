package AiJobsIndustry

import org.apache.spark.sql.SparkSession

trait Context {
  lazy val spark: SparkSession = createSparkSession(appName)
  val appName: String

  def createSparkSession(str: String) =
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
}
