package com.github.valrcs.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.ml.feature.OneHotEncoder

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
  //we would want to gracefully shut down our stream as well
  println("All done streaming")

  //Machine Learning Demo
  staticDataFrame.printSchema()

  // in Scala
  import org.apache.spark.sql.functions.date_format
  val preppedDataFrame = staticDataFrame
    .na.fill(0) //fill the empty areas with 0 // in ML 0 might not always be the best answer
    .withColumn("day_of_week", date_format(col("InvoiceDate"), "EEEE"))
    .coalesce(5)

  // in Scala
  //in Real Life we would probably want to do stratified random split across all dates
  val trainDataFrame = preppedDataFrame
    .where("InvoiceDate < '2011-07-01'")
  val testDataFrame = preppedDataFrame
    .where("InvoiceDate >= '2011-07-01'")

  println(trainDataFrame.count())
  println(testDataFrame.count())

  // in Scala
  import org.apache.spark.ml.feature.StringIndexer
  val indexer = new StringIndexer()
    .setInputCol("day_of_week")
    .setOutputCol("day_of_week_index")

  //This will turn our days of weeks into corresponding numerical values. For example, Spark might
  //represent Saturday as 6, and Monday as 1. However, with this numbering scheme, we are
  //implicitly stating that Saturday is greater than Monday (by pure numerical values). This is
  //obviously incorrect. To fix this, we therefore need to use a OneHotEncoder to encode each of
  //these values as their own column. These Boolean flags state whether that day of week is the
  //relevant day of the week:

  // in Scala
  //https://en.wikipedia.org/wiki/One-hot

  val encoder = new OneHotEncoder()
    .setInputCol("day_of_week_index")
    .setOutputCol("day_of_week_encoded")

  // in Scala
  import org.apache.spark.ml.feature.VectorAssembler
  val vectorAssembler = new VectorAssembler()
    .setInputCols(Array("UnitPrice", "Quantity", "day_of_week_encoded"))
    .setOutputCol("features")

  //Here, we have three key features: the price, the quantity, and the day of week. Next, we’ll set this
  //up into a pipeline so that any future data we need to transform can go through the exact same
  //process:

  // in Scala
  import org.apache.spark.ml.Pipeline
  val transformationPipeline = new Pipeline()
    .setStages(Array(indexer, encoder, vectorAssembler))

  // in Scala
  val fittedPipeline = transformationPipeline.fit(trainDataFrame)

  // in Scala
  val transformedTraining = fittedPipeline.transform(trainDataFrame)

  // in Scala
  import org.apache.spark.ml.clustering.KMeans
  val kmeans = new KMeans()
    .setK(20) //this is where you would make a guess on how many different groupings you want
    //this is Kmeans specific, Kmeans NEEDS to know how many groups to split your data
    .setSeed(1L)

  // in Scala
  val kmModel = kmeans.fit(transformedTraining)

  // in Scala
  val transformedTest = fittedPipeline.transform(testDataFrame)

  println("Our Kmeans fit results")
  transformedTest.show()

  transformedTest.coalesce(1)
    .select("Quantity","day_of_week", "day_of_week_index", "InvoiceNo")
    //first we need to fall back to single partition
    .write
    .option("header","true")
    .option("sep",",")
    .mode("overwrite") //meaning we will overwrite old data with same file name
    .csv("./src/resources/csv/kmeans-results.csv")


}
