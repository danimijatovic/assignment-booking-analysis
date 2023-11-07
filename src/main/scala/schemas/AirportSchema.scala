package schemas

import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

case class Airport(airportId: Int, name: String, city: String, country: String, iata: String, icao: String,
                   latitude: Double, longitude: Double, altitude: Int, timezone: Double, dst: String, tz: String,
                   airportType: String, source: String)

object Airport{
val schema = StructType(Seq(
StructField("AirportID", IntegerType, nullable = false),
StructField("Name", StringType, nullable = false),
StructField("City", StringType, nullable = false),
StructField("Country", StringType, nullable = false),
StructField("iata", StringType, nullable = true),
StructField("icao", StringType, nullable = true),
StructField("Latitude", DoubleType, nullable = false),
StructField("Longitude", DoubleType, nullable = false),
StructField("Altitude", IntegerType, nullable = false),
StructField("Timezone", DoubleType, nullable = false),
StructField("DST", StringType, nullable = false),
StructField("tz", StringType, nullable = false),
StructField("airportType", StringType, nullable = false),
StructField("Source", StringType, nullable = false)
))}
