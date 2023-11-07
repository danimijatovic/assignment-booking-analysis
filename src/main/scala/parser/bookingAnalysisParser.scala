package parser

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{ col, date_format, explode}
import schemas. Booking

import java.time.{DayOfWeek, LocalDate}
import java.time.format.DateTimeFormatter


object bookingAnalysisParser {


    def flattenBookingDS(booking: Dataset[Booking]): Dataset[_] =

      booking
        .withColumn("passenger", explode(col("event.DataElement.travelrecord.passengersList")))
        .withColumn("product", explode(col("event.DataElement.travelrecord.productsList")))
        .select(
          col("timestamp"),
          col("passenger.uci"),
          col("passenger.passengerType"),
          col("product.bookingStatus"),
          col("product.flight.destinationAirport").as("destinationAirport"),
          col("product.flight.originAirport").as("originAirport"),
          col("product.flight.departureDate").as("departureDate"),
          col("product.flight.operatingAirline").as("Airline"),
          col("product.flight.operatingFlightNumber").as("operatingFlightNumber")
        )
        .withColumn("departureDate", date_format(col("departureDate"), "yyyy-MM-dd"))



  def parserPassengerType(pType : String): String = pType match {
    case "ADT" => "Adult"
    case "CHD" => "Child"
    case _ => "undefinded"

  }

  def parseSeason(depatureDate: String): String = {

  val Array(year, month, day) = depatureDate.split("-").map(_.toInt)
        month match {
          case 3 | 4 | 5 => "Spring"
          case 6 | 7 | 8 => "Summer"
          case 9 | 10 | 11 => "Autumn"
          case 12 | 1 | 2 => "Winter"
          case _ => "Unknown"
        }}
//    (month, day) match {
//      case (12, _) if day >= 21 || month <= 2 => "Winter"
//      case (3, _) if day >= 21 || month <= 5 => "Spring"
//      case (6, _) if day >= 21 || month <= 8 => "Summer"
//      case (9, _) if day >= 21 || month <= 11 => "Autumn"
//      case _ => "Unknown"
//    }


  def parseDayOfWeek(depatureDate: String): String ={
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val localDate = LocalDate.parse(depatureDate, formatter)
    val dayOfWeek = localDate.getDayOfWeek

    dayOfWeek match {
      case DayOfWeek.MONDAY    => "Monday"
      case DayOfWeek.TUESDAY   => "Tuesday"
      case DayOfWeek.WEDNESDAY => "Wednesday"
      case DayOfWeek.THURSDAY  => "Thursday"
      case DayOfWeek.FRIDAY    => "Friday"
      case DayOfWeek.SATURDAY  => "Saturday"
      case DayOfWeek.SUNDAY    => "Sunday"
    }

  }}


