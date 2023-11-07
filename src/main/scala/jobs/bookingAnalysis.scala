package jobs


import com.typesafe.config.ConfigFactory
import conf.AppConfig
import org.apache.log4j.Level
import org.apache.spark.sql.functions.{broadcast, col, count, desc, lower,  when}
import schemas.{Airport, Booking, BookingEnriched, BookingWCountry}
import util.spark.SparkJob
import parser.bookingAnalysisParser._
import util.writer.writer

object bookingAnalysis extends App with SparkJob {

  /** Load Configurations  */
  val config = ConfigFactory.load()
  val appConfig = AppConfig.fromConfig(config).getOrElse {
    throw new RuntimeException("Failed to load application configuration")
  }

  import spark.implicits._

  setSparkExecutorLogLevel(spark, Level.WARN)

  /** Load Source Data */

  val bookings = spark.read
    .json(appConfig.sourceBooking.inputPath)
    .as[Booking]

  val airportDS = spark.read
    .option("header", "false")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .schema(Airport.schema)
    .csv(appConfig.sourceAirports.inputPath)
    .as[Airport]


  //flatten Booking DS for better readability

  val flattenBooking = flattenBookingDS(bookings)

  //filter booking based on timerange, booking status and airline and count each passenger once per flight leg

  val confirmedKLBookings = flattenBooking
    .filter(col("departureDate").between(appConfig.startDate, appConfig.endDate))
    .filter(lower(col("bookingStatus")) === "confirmed"
    && lower(col("Airline")) === "kl")
    .dropDuplicates("uci", "operatingFlightnumber")
    .cache()

  //Join BookingDS with AirportDS to map the Airports to country

  var KLBookingsWCountry = confirmedKLBookings
    .join(broadcast(airportDS),
      confirmedKLBookings("destinationAirport") === airportDS("iata"), "left_outer")
    .withColumnRenamed("Country", "destinationCountry")
    .withColumnRenamed("City", "destinationCity")
    .drop("iata")
    .join(broadcast(airportDS),
      confirmedKLBookings("originAirport") === airportDS("iata"), "left_outer")
    .withColumnRenamed("Country", "originCountry")
    .withColumnRenamed("City", "originCity")
    .as[BookingWCountry].map(identity)

  /** collect discharges of unmapped countries for further investigation
   and just used mapped countries */

  val dischargesCountry = KLBookingsWCountry.where(col("destinationCountry").isNull &&
    col("destinationCountry").isNull)

  val KLBookingsWCountryMapped = KLBookingsWCountry.where(col("destinationCountry").isNotNull &&
    col("destinationCountry").isNotNull)


  //filter flights with the origin Netherlands

  val KLBookingsNL = KLBookingsWCountryMapped.filter(col("originCountry") === "Netherlands")


  val KLBookingNLEnriched = KLBookingsNL.map(r => BookingEnriched(
    r.timestamp,
    r.uci,
    parserPassengerType(r.passengerType),
    r.destinationAirport,
    r.destinationCity,
    r.destinationCountry,
    r.originAirport,
    r.originCity,
    r.originCountry,
    r.departureDate,
    parseSeason(r.departureDate),
    parseDayOfWeek(r.departureDate)
  ))


  /** Create the final reports and write them to sink */
  val reportTopDestination = KLBookingNLEnriched.groupBy("Season", "dayOfWeek", "destinationCountry")
    .agg(count("*").alias("totalPassengers"))
    .orderBy(desc("totalPassengers"))

  val reportTopDestinationCity = KLBookingNLEnriched.groupBy("Season", "dayOfWeek", "destinationCountry","destinationCity")
    .agg(count("*").alias("totalPassengers"))
    .orderBy(desc("totalPassengers"))

  val reportTopDestinationPassengerType = KLBookingNLEnriched.groupBy("Season", "dayOfWeek", "destinationCountry")
  .agg(
    count(when(col("passengerType") === "Adult", true)).alias("NumberOfAdults"),
  count(when(col("passengerType") === "Child", true)).alias("NumberOfChildren"),
  count("*").alias("totalPassengers"))
    .orderBy(desc("totalPassengers"))

  writer.writeToSink(
    reportTopDestination,
    "booking-analysis-top-destinations",
    appConfig.sink.outputPath
  )

  writer.writeToSink(
    reportTopDestinationPassengerType,
    "booking-analysis-top-destinations-Adult-Child",
    appConfig.sink.outputPath
  )

  writer.writeToSink(
    reportTopDestinationCity,
    "booking-analysis-top-destinations-City",
    appConfig.sink.outputPath
  )

  writer.writeToSink(
    dischargesCountry.toDF(),
    "discharge-iata-country-mapping",
    appConfig.dischargeSinks.outputPath
  )


  spark.stop()
}

