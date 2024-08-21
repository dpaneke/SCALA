package AiJobsIndustry

import org.apache.spark.sql.{Dataset, SparkSession}

trait AiJobsIndustryCalculationsDs {
  val spark: SparkSession

  import spark.implicits._

  def calcStatsTotalDs(ds: Dataset[AiJobs]): Dataset[StatsResult] =
    (calcStat("company", ds) ++ calcStat("job", ds))
      .toDS()

  def calcStat(statsType: String, ds: Dataset[AiJobs]): Seq[StatsResult] = {
    val statsFunc: AiJobs => String = statsType match {
      case "company" => (record: AiJobs) => record.company
      case "job" => (record: AiJobs) => record.jobTitle
    }

    val summedByTypeDs: Dataset[StatsSum] =
      ds.groupByKey(statsFunc)
        .mapGroups {
          (title, records) => {
            val reviewsSum: Int = records.map(record => record.reviewsInt).sum
            StatsSum(title, reviewsSum)
          }
        }

    val summedByTypeWithLocationDs: Dataset[StatsWithLocationSum] = {
      val groupFunc = (record: AiJobs) => (statsFunc(record), record.location)
      ds.groupByKey(groupFunc)
        .mapGroups {
          (titleWithLocation, records) => {
            val title: String = titleWithLocation._1
            val location: String = titleWithLocation._2
            val reviewsSum: Int = records.map(_.reviewsInt).sum
            StatsWithLocationSum(title, location, reviewsSum)
          }
        }
    }

    val leaderTitle: String = getLeaderTitleDs(statsFunc, summedByTypeDs)

    val leaderWithLocations: Dataset[StatsWithLocationSum] = summedByTypeWithLocationDs
      .transform(filteredLeaderTitleDs(leaderTitle))

    val statsMax: StatsResult = aggReviewsByLocation(
      leaderTitle,
      statsType,
      "max",
      StatsWithLocationSum.max,
      leaderWithLocations)

    val statsMin: StatsResult = aggReviewsByLocation(
      leaderTitle,
      statsType,
      "min",
      StatsWithLocationSum.min,
      leaderWithLocations)

    Seq(statsMax, statsMin)
  }

  def getLeaderTitleDs(statsFunc: AiJobs => String, ds: Dataset[StatsSum]): String =
    ds.reduce((firstStat, secondStat) => StatsSum.max(firstStat, secondStat))
      .title

  def filteredLeaderTitleDs(
                             leaderTitle: String)(
                             ds: Dataset[StatsWithLocationSum]): Dataset[StatsWithLocationSum] =
    ds.filter(_.title == leaderTitle)

  def aggReviewsByLocation(
                            leaderTitle: String,
                            statsType: String,
                            calcType: String,
                            aggFunc: (StatsWithLocationSum, StatsWithLocationSum) => StatsWithLocationSum,
                            ds: Dataset[StatsWithLocationSum]): StatsResult = {
    val targetStatWithLocation: StatsWithLocationSum =
      ds.reduce((first, second) => aggFunc(first, second))

    val location: String = targetStatWithLocation.location
    val countTotal: Int = targetStatWithLocation.reviewsSum

    StatsResult(leaderTitle, statsType, location, countTotal, calcType)
  }
}
