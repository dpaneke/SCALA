package AiJobsIndustry

import org.apache.spark.sql.Dataset

object AiCalculations {
  def uniteStats[T](dataFrames: Dataset[T]*): Dataset[T] =
    dataFrames.reduce((df1, df2) => df1.union(df2))
}
