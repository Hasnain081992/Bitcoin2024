import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Step 1: Initialize Spark session
val spark = SparkSession.builder
  .master("local")
  .appName("IncrementalLoad")
  .enableHiveSupport()
  .getOrCreate()

// Step 2: Set the last Cumulative_Volume value manually (this can be dynamic in production)
val lastCumulativeVolume = 36805900.826118246
println(s"Max Cumulative_Volume: $lastCumulativeVolume") // Print the last Cumulative_Volume value

// Step 3: Build the query to fetch data from PostgreSQL
val query = s"""SELECT * FROM bitcoin_2025 WHERE "Cumulative_Volume" > $lastCumulativeVolume"""

// Step 4: Read data from PostgreSQL using the query
val newData = spark.read
  .format("jdbc")
  .option("url", "jdbc:postgresql://18.132.73.146:5432/testdb")
  .option("driver", "org.postgresql.Driver")
  .option("user", "consultants")
  .option("password", "WelcomeItc@2022")
  .option("query", query)
  .load()

// Show the new data that was loaded
newData.show()

// Step 5: Write the new data to Hive
newData.write
  .mode("append")
  .saveAsTable("project2024.bitcoin_inc_team")

println("Successfully loaded data into Hive")

// Optionally: Additional transformations or actions can be performed here

