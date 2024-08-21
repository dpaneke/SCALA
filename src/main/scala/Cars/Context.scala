package Cars

import org.apache.spark.sql.SparkSession

trait Context {
  val appName: String
  lazy val spark: SparkSession = createSparkSession(appName)

  def createSparkSession(appName: String): SparkSession =
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
}
