package AthleticShoes

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{coalesce, col}

trait AthleticShoesTransformer {
  def withDroppedNullNameAndCategory(df: DataFrame): DataFrame =
    df.na.drop("any", Seq("item_category", "item_name"))

  def fillNullDiscountWithPrice(df: DataFrame): DataFrame =
    df.withColumn("item_after_discount", coalesce(col("item_after_discount"), col("item_price")))

  def fillNullRatingWithZero(df: DataFrame): DataFrame =
    df.na.fill(0, Seq("item_rating"))

  def fillNullGender(df: DataFrame): DataFrame =
    df.na.fill("unknown", Seq("buyer_gender"))

  def fillNullPercentageSolds(df: DataFrame): DataFrame =
    df.na.fill(-1, Seq("percentage_solds"))

  def fillNullWithNA(df: DataFrame): DataFrame =
    df.na.fill("n/a")
}
