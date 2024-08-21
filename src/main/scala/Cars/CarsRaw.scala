package Cars

case class CarsRaw(
  id: Int,
  price: Int,
  brand: String,
  category: String,
  mileage: Option[Double],
  color: String,
  date_of_purchase: String)
