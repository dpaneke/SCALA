package Cars

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types._
import java.time.{LocalDate, Period}
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.util.Locale

object CarsRun
  extends App
  with Context
  with CarsCalculation {

  override val appName: String = "CarsApp"

  import spark.implicits._

  val carsSchema = StructType(Seq(
    StructField("id", IntegerType, nullable = false),
    StructField("price", IntegerType, nullable = false),
    StructField("brand", StringType, nullable = false),
    StructField("category", StringType, nullable = false),
    StructField("mileage", DoubleType, nullable = true),
    StructField("color", StringType, nullable = false),
    StructField("date_of_purchase", StringType, nullable = false)))

  val CarsRawDs: Dataset[CarsRaw] = spark.read
    .option("header", "true")
    .option("sep", ",")
    .schema(carsSchema)
    .format("csv")
    .load("src/main/resources/cars.csv")
    .as[CarsRaw]

  val CarsDs: Dataset[Cars] = toCars(CarsRawDs)
  val carsWithCalculationDs: Dataset[CarsWithCalculation] = withCalculations(CarsDs)

  carsWithCalculationDs.show()
}





