package SeasonWordCount

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SeasonWordCountRun extends App with Context {

  override val appName: String = "SeasonWordCount"

  val subFirstDf = spark.read
    .option("inferSchema", "true")
    .csv("src/main/resources/subtitles_s1.json")
    .toDF("word")

  val subSecondDf = spark.read
    .option("inferSchema", "true")
    .csv("src/main/resources/subtitles_s2.json")
    .toDF("word")

  val seasonWordCountFirst =
    subFirstDf.transform(
      SeasonWordCounter.withSeasonWordCount("w_s1", "cnt_s1"))

  val seasonWordCountSecond =
    subSecondDf.transform(
      SeasonWordCounter.withSeasonWordCount("w_s2", "cnt_s2"))

  val seasonsJoined = seasonWordCountFirst.join(seasonWordCountSecond, "id")

  seasonsJoined.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/wordcount")
}