package com.github.valrcs.spark

import com.github.valrcs.spark.ReadingCSV.spark

object ExerciseJ15 extends App {
  val spark = SparkUtil.createSpark("ExerciseJ15")

  val flightData2010 = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("./src/resources/flight-data/csv/2010-summary.csv")
  println(flightData2010.schema)
  flightData2010.show(5)

  //TODO create new DataFrame with flight count over 1000
  //TODO Also FILTER off Canada and Ireland
  //TODO bonus figure out how to order by top results and show top 10

}
