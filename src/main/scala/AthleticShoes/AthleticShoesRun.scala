package AthleticShoes

import org.apache.spark.sql.{DataFrame, Dataset}

object AthleticShoesRun extends App with Context with AthleticShoesTransformer {
  override val appName: String = "AthleticShoes"

  import spark.implicits._

  val athleticShoesRawDF: DataFrame = spark.read
    .options(Map("inferSchema" -> "true", "sep" -> ",", "header" -> "true"))
    .csv("src/main/resources/athletic_shoes.csv")

  val athleticShoesDF =
    athleticShoesRawDF
      .transform(withDroppedNullNameAndCategory)
      .transform(fillNullDiscountWithPrice)
      .transform(fillNullRatingWithZero)
      .transform(fillNullGender)
      .transform(fillNullPercentageSolds)
      .transform(fillNullWithNA)

  val athleticShoesDS: Dataset[Shoes] = athleticShoesDF.as[Shoes]
  athleticShoesDS.show()
}
