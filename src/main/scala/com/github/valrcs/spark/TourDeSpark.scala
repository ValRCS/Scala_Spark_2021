package com.github.valrcs.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.{Encoder, Encoders}

import scala.io.StdIn

object TourDeSpark extends App {
  println("Overview of Spark capabilities")

  println(s"Reading CSVs with Scala version: ${util.Properties.versionNumberString}")

  val spark = SparkSession.builder().appName("tourDeSpark").master("local").getOrCreate()
  spark.conf.set("spark.sql.shuffle.partitions", "5") //recommended for local, default is 200?
  println(s"Session started on Spark version ${spark.version}")

  // in Scala
  case class Flight(DEST_COUNTRY_NAME: String,
                    ORIGIN_COUNTRY_NAME: String,
                    count: BigInt)
  val flightsDF = spark.read
    .parquet("./src/resources/flight-data/parquet/2010-summary.parquet/")
  flightsDF.show(10)
  //we do need to provide an implicit encoder (explicit is another option)
  //https://stackoverflow.com/questions/38664972/why-is-unable-to-find-encoder-for-type-stored-in-a-dataset-when-creating-a-dat
  implicit val enc: Encoder[Flight] = Encoders.product[Flight]
  val flights = flightsDF.as[Flight]
  flights.show(5)

  //we get type safety with using DataSet of our chosen Case classes
  //so we filter first 77 nonCanadians and add 500 to their flight count
  val nonCanadians = flights
    .take(77)
    .filter(flight_row => flight_row.ORIGIN_COUNTRY_NAME != "Canada")
    .map(fr => Flight(fr.DEST_COUNTRY_NAME, fr.ORIGIN_COUNTRY_NAME, fr.count + 500))
  nonCanadians.slice(0,10).foreach(println)
  //at some point we will also want to convert back to dataframe from our Array of Case Classes

  //streaming simulation
  // in Scala
  val retailDayPath = "./src/resources/retail-data/by-day/*.csv"
  val staticDataFrame = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(retailDayPath) //so we are reading ALl by-day csv
  staticDataFrame.createOrReplaceTempView("retail_data") //for our SQL queries
  val staticSchema = staticDataFrame.schema
  staticDataFrame.show(10)
//  staticDataFrame.describe().show(20) //some statistics about our data

  // in Scala
  import org.apache.spark.sql.functions.{window, column, desc, col}
  staticDataFrame
    .selectExpr(
      "CustomerId",
      "(UnitPrice * Quantity) as total_cost",
      "InvoiceDate")
    .groupBy(
      col("CustomerId"), window(col("InvoiceDate"), "1 day"))
    .sum("total_cost")
    .show(5)

  //alternative to the above, would have been to just write SQL to Selection, Group By and use te sliding window
  //just like reqular SQL

  val streamingDataFrame = spark.readStream
    .schema(staticSchema)
    .option("maxFilesPerTrigger", 1) //for demo purposes, in production this would be taken off
    .format("csv")
    .option("header", "true")
    .load(retailDayPath)

  println(s"Our retail data is streaming: ${streamingDataFrame.isStreaming}" )// returns true})

  // in Scala
  val purchaseByCustomerPerHour = streamingDataFrame
    .selectExpr(
      "CustomerId",
      "(UnitPrice * Quantity) as total_cost",
      "InvoiceDate")
    .groupBy(
      col("CustomerId"), window(col("InvoiceDate"), "1 day"))
    .sum("total_cost")

    //This is still a lazy operation, so we will need to call a streaming action to start the execution of
  //this data flow

  // in Scala
  purchaseByCustomerPerHour.writeStream
    .format("memory") // memory = store in-memory table
    .queryName("customer_purchases") // the name of the in-memory table
    .outputMode("complete") // complete = all the counts should be in the table
    .start()

  // in Scala
  spark.sql("""
SELECT *
FROM customer_purchases
ORDER BY `sum(total_cost)` DESC
""")
    .show(5)


  //  //let's delay ending our Spark session
  var rawInput = StdIn.readLine("enter q(uit) to quit otherwise anything else continues")
  while (!rawInput.startsWith("q")) {
      spark.sql("""
          SELECT *
          FROM customer_purchases
          ORDER BY `sum(total_cost)` DESC
          """)
        .show(5)
    rawInput = StdIn.readLine("enter q(uit) to quit otherwise anything else continues")
  }
  println("All done streaming")
}
