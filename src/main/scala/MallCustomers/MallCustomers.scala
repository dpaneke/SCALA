package MallCustomers

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object MallCustomers extends App{
  val spark = SparkSession.builder()
    .appName("mallCustomers")
    .master("local")
    .getOrCreate()

  val mallSchema = StructType(Seq(
    StructField("CustomerID", IntegerType),
    StructField("Gender", StringType),
    StructField("Age", IntegerType),
    StructField("Annual Income (k$)", IntegerType),
    StructField("Spending Score (1-100)", IntegerType)))

  val mallDF = spark.read
    .option("header", "true")
    .schema(mallSchema)
    .csv("src/main/resources/mall_customers.csv")

  val mallFixedAgeDF = mallDF.withColumn("Age", col("Age") + 2)

  val incomeDF =
    mallFixedAgeDF
      .where(col("Age").between(30, 35))
      .groupBy(col("Gender"), col("Age"))
      .agg(round(mean(col("Annual Income (k$)")), 1).as("mean_annual_income"))
      .orderBy(col("Gender").asc, col("Age").asc)
      .withColumn("gender_code",
        when(col("Gender") === "Male", lit(1))
          .when(col("Gender") === "Female", lit(0))
          .otherwise(lit(null)))

  incomeDF
    .write
    .mode("overwrite")
    .save("src/main/resources/data/customers")
}
