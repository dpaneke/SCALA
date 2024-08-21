package AiJobsIndustry

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, initcap, lit, lower, regexp_extract, regexp_replace, trim}
import org.apache.spark.sql.types.IntegerType

trait AiJobsIndustryTransformationsDf {
  def extractJobReviews(df: DataFrame): DataFrame =
    df.select("JobTitle", "Company", "Location", "CompanyReviews")

  def dropNaData(df: DataFrame): DataFrame =
    df.na.drop

  def withTrimmedCapitalizedColumns(df: DataFrame): DataFrame = {
    df.columns.foldLeft(df)(
      (df, field) => {
        val trimmedColumn = trim(regexp_replace(col(field), lit("\n"), lit("")))
        val trimmedCapitalizedColumn = initcap(lower(trimmedColumn))
        df.withColumn(field, trimmedCapitalizedColumn)
      })
  }

  def withReviewsInt(df: DataFrame): DataFrame = {
    val reviewsStr: Column = regexp_extract(col("CompanyReviews"), raw"(,|\.| |\d)*", 0)
    val reviewsInt: Column = regexp_replace(reviewsStr, lit(raw"(,|\.| )"), lit("")).cast(IntegerType)
    df.withColumn("reviewsInt", reviewsInt)
  }

  def withDistinctRecords(cols: Column*)(df: DataFrame): DataFrame = df
    .select(cols: _*)
    .dropDuplicates
}
