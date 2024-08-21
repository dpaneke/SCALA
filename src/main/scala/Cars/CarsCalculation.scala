package Cars

import org.apache.spark.sql.{Dataset, SparkSession}

import java.time.{LocalDate, Period}
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.util.Locale

trait CarsCalculation {
  val spark: SparkSession
  import spark.implicits._

  def withCalculations(ds: Dataset[Cars]): Dataset[CarsWithCalculation] = {
    val avgMileage: Double = calculateAvgMileage(ds)

    def calculateYearsSincePurchase(date: LocalDate): Int  = {
      val dateToday: LocalDate = LocalDate.now()
      Period.between(date, dateToday).getYears
    }

    ds.map(car =>
      CarsWithCalculation(
        car.id,
        car.price,
        car.brand,
        car.category,
        car.mileage,
        car.color,
        car.date_of_purchase,
        avgMileage,
        calculateYearsSincePurchase(car.date_of_purchase))
    )
  }

  def toCars(ds: Dataset[CarsRaw]): Dataset[Cars] = {
    def parseDate(possibleFormats: List[DateTimeFormatter], date: String) =
      possibleFormats
        .flatMap(format =>
          try {
            Some(LocalDate.parse(date, format))
          } catch {
            case e: DateTimeParseException => None
          })
        .head

    ds.map(carRaw => {
      val possibleFormats: List[DateTimeFormatter] = List(
        DateTimeFormatter.ofPattern("yyyy-MM-dd"),
        DateTimeFormatter.ofPattern("yyyy MM dd"),
        DateTimeFormatter.ofPattern("yyyy MMM dd", Locale.ENGLISH))

      val parsedDate: LocalDate = parseDate(possibleFormats, carRaw.date_of_purchase)

      Cars(
        carRaw.id,
        carRaw.price,
        carRaw.brand,
        carRaw.category,
        carRaw.mileage,
        carRaw.color,
        parsedDate)
    })
  }

  def calculateAvgMileage(ds: Dataset[Cars]): Double = {
    val (mileageSum: Double, dsLength: Int) = ds
      .map(record => (record.mileage.getOrElse(0.0), 1))
      .reduce((recFirst, recSecond) =>
        (recFirst._1 + recSecond._1, recFirst._2 + recSecond._2))

    mileageSum / dsLength
  }
}
