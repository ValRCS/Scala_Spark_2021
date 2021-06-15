package com.github.valrcs.spark

import org.apache.spark.sql.functions.{col,sum}

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
  //TODO bonus figure out how to order by top results and show top 10 - we did not quite cover this yet
  flightData2010.where(col("count") > 1000)
    .where(col("DEST_COUNTRY_NAME") =!= "Canada")
    .where(col("DEST_COUNTRY_NAME") =!= "Ireland")
    .where(col("ORIGIN_COUNTRY_NAME") =!= "Canada")
    .where(col("ORIGIN_COUNTRY_NAME") =!= "Ireland")
    .sort(col("count").desc)
    .show(10)

  //same as above but with sql syntax
  flightData2010.createOrReplaceTempView(("flights2010"))
  spark.sql("SELECT * FROM flights2010 " +
    "WHERE count > 1000 " +
    "AND dest_country_name NOT IN ('Canada', 'Ireland') " +
    "AND origin_country_name NOT IN ('Canada', 'Ireland') " +
    "ORDER BY count DESC " +
    "LIMIT 10")
    .show(10) //10
  // is redundant here since we already have LIMIT 10

  //for longer list of countries we would want to check with some sequence
  val includeNames = Seq("Canada","Ireland","Germany")
  flightData2010
//    .where(col("count") > 1000)
    .where(col("dest_country_name").isInCollection(includeNames)) //this will give us only the 3 countries
    .sort(col("count").desc)
    .show(10)
  //TODO how to mass exclude many names, as in give a list /sequence of strings to exclude



  flightData2010
    .where(col("DEST_COUNTRY_NAME") =!= col("ORIGIN_COUNTRY_NAME")) //so no domestic flights
    .sort(col("count").desc)
    .show(5)

  flightData2010
    .select(col("count")).agg(sum("count")) //so all flights but domestic
    .show()

  flightData2010
    .where(col("DEST_COUNTRY_NAME") =!= col("ORIGIN_COUNTRY_NAME")) //so no domestic flights
    .select(col("count")).agg(sum("count")) //so all flights but domestic
    .show()

}
