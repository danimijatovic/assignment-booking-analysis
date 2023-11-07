package util.writer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit


object writer {

    def writeToSink(dataset: DataFrame, topic: String, basePath: String): Unit = {
      // Check if the dataset contains year, month, and day columns and use them for partitioning
      if (dataset.columns.contains("year") && dataset.columns.contains("month") && dataset.columns.contains("day")) {
        dataset.write.partitionBy("year", "month", "day").parquet(s"$basePath/$topic")
      } else {
        // If the columns don't exist, use today's date for partitioning
        val currentDate = java.time.LocalDate.now()
        val year = currentDate.getYear
        val month = currentDate.getMonthValue
        val day = currentDate.getDayOfMonth
        dataset.withColumn("year", lit(year)).withColumn("month", lit(month)).withColumn("day", lit(day))
          .write.mode("append").partitionBy("year", "month", "day").parquet(s"$basePath/$topic")
  }
}
}
