package com.github.valrcs.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Encoder, Encoders}

object TourDeSpark extends App {
  println("Overview of Spark capabilities")

  println(s"Reading CSVs with Scala version: ${util.Properties.versionNumberString}")

  val spark = SparkSession.builder().appName("tourDeSpark").master("local").getOrCreate()
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


}
