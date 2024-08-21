package AiJobsIndustry

case class StatsSum(
  title: String,
  reviewsSum: Int)

object StatsSum {
  def max(first: StatsSum, second: StatsSum): StatsSum =
    if (first.reviewsSum >= second.reviewsSum) first else second
}
