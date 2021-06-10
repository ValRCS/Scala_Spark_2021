package com.github.valrcs.spark

import org.apache.spark.sql.SparkSession

object ReadingCSV extends App {
  println(s"Reading CSVs with Scala version: ${util.Properties.versionNumberString}")

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  println(s"Session started on Spark version ${spark.version}")

  // in Scala
  val flightData2015 = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("./src/resources/flight-data/csv/2015-summary.csv")

  flightData2015.take(3).foreach(println) //take actually puts data into our local memory so careful with large takes
  flightData2015.show(5)
  //we can see how Spark will execute a certain operation/transformation
  flightData2015.sort("count").explain()

  //if we had 5 or more machines /clusters then this would be useful
  //as we only have 1 local machine this wouldnt really do anything
  spark.conf.set("spark.sql.shuffle.partitions", "5")

//  flightData2015.sort("count").show(10)
  flightData2015.sort("count").show(10)

  //before making SQL type of queries we need a view
  flightData2015.createOrReplaceTempView("flight_data_2015") //view name is up to you

  // in Scala
  val sqlWay = spark.sql("""
SELECT DEST_COUNTRY_NAME, count(1)
FROM flight_data_2015
GROUP BY DEST_COUNTRY_NAME
""")
  sqlWay.show()

//  val dataFrameWay = flightData2015
//    .groupBy("DEST_COUNTRY_NAME") //looks like column number is wrong maybe col0 ?
//    .count()
//
//  dataFrameWay.show()
  sqlWay.explain
//  dataFrameWay.explain

  val sqlDesc = spark.sql(
    """
      |SELECT *
      |FROM flight_data_2015
      |ORDER BY count DESC
      |""".stripMargin)
  //so we can use regular SQL to make our queries
  sqlDesc.show() //the most frequent flight should be at the top

  //of course Spark is a bit of overkill for working just with single small CSVs



}
