package com.github.valrcs.spark

import org.apache.spark.sql.functions.{avg, col, desc, expr, lit, round}

object ExerciseJul19 extends App {
  val spark = SparkUtil.createSpark("exerciseJul19")
  val filePath = "./src/resources/csv/stocks_2013_2018.csv"

  //TODO read the file, infer schema

  //TODO calculate the daily change in percentages for all rows
  //TODO group the results by day and calculate the average change for all stocks combined by day

  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(filePath)
    .coalesce(5) //?

  df.printSchema()
  df.show(5, false)

  //TODO calculate the daily change in percentages for all rows

//  val change = df.withColumn("change", round(lit(100) * (col("close") - col("open"))/col("open"), 2))
//  change.show()
//  //change in %, % sign not shown
//
//  //TODO group the results by day and calculate the average change for all stocks combined by day
//  change.groupBy("date").avg("change").show(false) //we should probably do round at the end
  //if you do too much rounding in between you may actually start losing meaningful information

  val dfWithReturn = df
    .withColumn("daily_return", (col("close") - col("open"))/col("open")*100)

  dfWithReturn
    .groupBy("date")
//    .pivot("Name")
    .agg(avg("daily_return"))
//    .orderBy(desc("date"))
    .orderBy(desc("avg(daily_return)"))
    .show(10, false)

  dfWithReturn.orderBy(desc("daily_return"))
    .show(10, false) //days could be matching

  dfWithReturn.orderBy(desc("daily_return"))
    .where("Name != 'NFLX'")
    .show(10, false) //days could be matching

  val dfWithPreviousDay = df.withColumn("prevOpen", expr("" +
    "LAG (open,1,0) " +
    "OVER (PARTITION BY Name " +
    "ORDER BY date )"))
    .withColumn("prevClose", expr("" +
      "LAG (close,1,0) " +
      "OVER (PARTITION BY Name " +
      "ORDER BY date )"))
  dfWithPreviousDay.show(10, false)
}
