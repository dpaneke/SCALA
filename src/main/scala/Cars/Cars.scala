package Cars

import java.time.LocalDate

case class Cars(
  id: Int,
  price: Int,
  brand: String,
  category: String,
  mileage: Option[Double],
  color: String,
  date_of_purchase: LocalDate)
