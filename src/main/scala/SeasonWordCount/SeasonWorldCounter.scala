package SeasonWordCount

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, explode, lower, row_number, split}

object SeasonWordCounter {
  private def explodeWords(df: DataFrame): DataFrame =
    df.withColumn("word", explode(split(col("word"), "\\W+")))

  private def filterBlank(df: DataFrame): DataFrame =
    df.where(col("word") =!= "")

  private def lowerWords(df: DataFrame): DataFrame =
    df.withColumn("word", lower(col("word")))

  private def dropNumbers(df: DataFrame): DataFrame =
    df.where(col("word").cast("integer").isNull)

  private def withWordCount(wordColName: String, cntColName: String)(df: DataFrame): DataFrame = {
    df
      .groupBy(col("word"))
      .agg(count("*").as(cntColName))
      .withColumnRenamed("word", wordColName)
      .orderBy(col(cntColName).desc)
  }

  private def withId(orderCol: String)(df: DataFrame): DataFrame = {
    val window = Window.orderBy(col(orderCol).desc)
    df.withColumn("id", row_number().over(window) - 1)
  }

  private def filterId(df: DataFrame): DataFrame =
    df.where(col("id") < 20)

  def withSeasonWordCount(wordColName: String, cntColName: String)(df: DataFrame): DataFrame =
    df
      .transform(explodeWords)
      .transform(filterBlank)
      .transform(lowerWords)
      .transform(dropNumbers)
      .transform(withWordCount(wordColName, cntColName))
      .transform(withId(cntColName))
      .transform(filterId)
}
