package conf

import com.typesafe.config.Config
import pureconfig.{CamelCase, ConfigFieldMapping, ConfigSource, KebabCase}
import pureconfig.generic.ProductHint
import scala.util.Try
import pureconfig.generic.auto._
import pureconfig.error.ConfigReaderException


case class SinkConfig(outputPath: String, provider: String, `type`: String)
case class SourceConfig(inputPath: String, provider: String, `type`: String)
case class AppConfig(sink: SinkConfig, dischargeSinks: SinkConfig,
                     sourceBooking: SourceConfig, sourceAirports: SourceConfig,
                     startDate: String, endDate: String)


object AppConfig {

  import pureconfig.generic.auto._

  def fromConfig(config: Config): Try[AppConfig] = {

    // map the configuration keys in kebab-case to the corresponding fields in the case classes in camelCase
    implicit def hint[A]: ProductHint[A] = ProductHint[A](ConfigFieldMapping(CamelCase, KebabCase))

    ConfigSource
      .fromConfig(config)
      .load[AppConfig]
      .left
      .map(err => new ConfigReaderException[AppConfig](err))
      .toTry
  }
}