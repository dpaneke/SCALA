package AthleticShoes

import org.apache.spark.sql.SparkSession

trait Context {
  val appName: String

  lazy val spark = createSession(appName: String)

  def createSession(str: String): SparkSession =
    SparkSession.builder()
      .appName(str)
      .master("local[*]")
      .getOrCreate()
}
