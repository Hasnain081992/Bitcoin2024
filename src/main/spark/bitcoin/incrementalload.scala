import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object IncrementalLoad {
  def main(args: Array[String]): Unit = {
    // Step 1: Initialize Spark session
    val spark = SparkSession.builder()
      .appName("IncrementalLoad")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    // Step 2: Set the last Cumulative_Volume value manually (this can be dynamic in production)
    val lastCumulativeVolume = 36805902.179584436
    println(s"Max Cumulative_Volume: $lastCumulativeVolume")

    // Step 3: Build the query to get data from PostgreSQL
    val query = s"""SELECT * FROM bitcoinsca_2025 WHERE "Cumulative_Volume" > $lastCumulativeVolume"""

    // Step 4: Read data from PostgreSQL using the query
    val newData: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://18.132.73.146:5432/testdb")
      .option("driver", "org.postgresql.Driver")
      .option("user", "consultants")
      .option("password", "WelcomeItc@2022")
      .option("query", query)
      .load()

    // Show the new data
    newData.show()

    // Step 5: Write the new data to Hive
    newData.write
      .mode("append")
      .saveAsTable("project2024.bitcoin_scala_inc_2025")

    println("Successfully loaded data into Hive")

    // Stop Spark Session
    spark.stop()
  }
}

