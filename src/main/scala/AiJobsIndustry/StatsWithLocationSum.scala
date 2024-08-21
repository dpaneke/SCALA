package AiJobsIndustry

case class StatsWithLocationSum(
  title: String,
  location: String,
  reviewsSum: Int)

object StatsWithLocationSum {
  def max(first: StatsWithLocationSum, second: StatsWithLocationSum): StatsWithLocationSum =
    if (first.reviewsSum >= second.reviewsSum) first else second

  def min(first: StatsWithLocationSum, second: StatsWithLocationSum): StatsWithLocationSum =
    if (first.reviewsSum <= second.reviewsSum) first else second
}
