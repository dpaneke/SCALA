package AiJobsIndustry

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit, max, max_by, min, min_by, sum}

trait AiJobsIndustryCalculationsDf {
  def calcStatsTotal(df: DataFrame): DataFrame =
    AiCalculations.uniteStats(
      df.transform(calcStat("company")),
      df.transform(calcStat("job")))

  def calcStat(statsType: String)(df: DataFrame): DataFrame = {
    val statsCol: Column = statsType match {
      case "company" => col("Company")
      case "job" => col("JobTitle")
    }

    val summedByTypeDf: DataFrame = df.transform(withReviewsSum(statsCol))

    val summedByTypeWithLocationDf: DataFrame =
      df.transform(withReviewsSum(statsCol, col("Location")))

    val leaderTitle: String = getLeaderTitle(statsCol, summedByTypeDf)

    val leaderWithLocations = summedByTypeWithLocationDf
      .transform(filteredLeaderTitle(statsCol, leaderTitle))

    val statsMax = leaderWithLocations
      .transform(
        aggReviewsByLocation(
          leaderTitle,
          statsType,
          "max",
          max_by,
          max))

    val statsMin = leaderWithLocations
      .transform(
        aggReviewsByLocation(
          leaderTitle,
          statsType,
          "min",
          min_by,
          min))

    AiCalculations.uniteStats(statsMax, statsMin)
  }

  def withReviewsSum(by: Column*)(df: DataFrame): DataFrame =
    df.groupBy(by: _*)
      .agg(sum("reviewsInt").as("reviews_sum"))

  def getLeaderTitle(statsCol: Column, df: DataFrame): String =
    df.agg(max_by(statsCol, col("reviews_sum")))
      .first()(0)
      .toString

  def filteredLeaderTitle(statsCol: Column, leaderTitle: String)(df: DataFrame): DataFrame =
    df.where(statsCol === lit(leaderTitle))

  def aggReviewsByLocation(
                            leaderTitle: String,
                            statsType: String,
                            calcType: String,
                            aggByFunc: (Column, Column) => Column,
                            aggFunc: Column => Column)(df: DataFrame): DataFrame = {
    df.agg(
        aggByFunc(col("Location"), col("reviews_sum")).as("location"),
        aggFunc(col("reviews_sum")).as("count"))
      .select(
        lit(leaderTitle).as("name"),
        lit(statsType).as("stats_type"),
        col("location"),
        col("count"),
        lit(calcType).as("count_type"))
  }
}
