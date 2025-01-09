import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object bitcoscala_2024 {
  def main(args: Array[String]): Unit = {
    // Step 1: Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("bitcoscala_2024")
      .master("local[*]")
      .getOrCreate()

    // Step 2: Load the Data
    val filePath = "C://Users//44754//Downloads//newdoc//btcusd.csv"
    val btcData = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

    // Repartition the data to 100 partitions
    val repartitionedData = btcData.repartition(100)

    // Step 3: Data Transformation

    // Convert timestamp to a readable datetime format
    val withDatetime = repartitionedData.withColumn("Datetime", from_unixtime(col("Timestamp")))

    // Fill missing values with the forward-fill method
    val filledData = withDatetime.na.fill(0)  // Fill missing values with 0

    // Calculate price range (High - Low) and add it as a new column
    val withPriceRange = filledData.withColumn("Price_Range", col("High") - col("Low"))

    // Add a 10-period moving average of the Close price
    val windowSpec10 = Window.orderBy("Datetime").rowsBetween(-9, 0)
    val withMA10 = withPriceRange.withColumn("MA_Close_10", avg(col("Close")).over(windowSpec10))

    // Add a 30-period moving average of the Close price
    val windowSpec30 = Window.orderBy("Datetime").rowsBetween(-29, 0)
    val withMA30 = withMA10.withColumn("MA_Close_30", avg(col("Close")).over(windowSpec30))

    // Calculate daily return percentage
    val windowSpecLag = Window.orderBy("Datetime")
    val withDailyReturn = withMA30.withColumn("Daily_Return",
      ((col("Close") - lag("Close", 1).over(windowSpecLag)) / lag("Close", 1).over(windowSpecLag)) * 100)

    // Add a column indicating if the Close price increased (1) or decreased (0)
    val withCloseIncreased = withDailyReturn.withColumn("Close_Increased", when(col("Daily_Return") > 0, 1).otherwise(0))

    // Add a column for cumulative sum of Volume
    val withCumulativeVolume = withCloseIncreased.withColumn("Cumulative_Volume", sum("Volume").over(windowSpecLag))

    // Step 4: Write Data to PostgreSQL
    val jdbcUrl = "jdbc:postgresql://18.132.73.146:5432/testdb"
    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", "consultants")
    connectionProperties.put("password", "WelcomeItc@2022")

    withCumulativeVolume.write
      .jdbc(jdbcUrl, "bitcoinsca_2025", connectionProperties)

    // Stop Spark Session
    spark.stop()
  }
}
