package Cars

import java.time.LocalDate

case class CarsWithCalculation(
  id: Int,
  price: Int,
  brand: String,
  category: String,
  mileage: Option[Double],
  color: String,
  date_of_purchase: LocalDate,
  avg_mileage: Double,
  years_since_purchase: Int)
