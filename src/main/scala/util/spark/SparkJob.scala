package util.spark

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkJob {

  def sparkConf: SparkConf = {
    new SparkConf()
      .setIfMissing("spark.debug.maxToStringFields", "1000")
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.driver.host", "127.0.0.1")
  }
  lazy val appName: String = getClass.getSimpleName.replace("$", "")
  lazy val className: String = getClass.getName.replace("$", "")

  //Initialize the implicit SparkSession available for all Spark Jobs in the Project
  implicit lazy val spark: SparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .appName(appName)
    .getOrCreate()

  // define logger
  @transient implicit lazy val logger: Logger = org.apache.log4j.LogManager.getLogger(className)


  def setSparkExecutorLogLevel(spark: SparkSession, level: Level): Unit = {
    spark.sparkContext.parallelize(Seq("")).foreachPartition(_ => {
      LogManager.getRootLogger().setLevel(level)
    })
  }
}

