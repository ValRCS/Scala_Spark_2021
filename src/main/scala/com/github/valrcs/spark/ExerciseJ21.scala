package com.github.valrcs.spark

import org.apache.spark.sql.functions.{col, expr, monotonically_increasing_id, rand, round}

object ExerciseJ21 extends App {
  println("Let's analyze some data for June 21st - 2011")
  val filePath = "./src/resources/retail-data/by-day/2011-06-21.csv"

  val spark = SparkUtil.createSpark("exj21")

  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true") //so Spark tries to figure out the schema if column has 1 2 and 3C the type will be StringType
    .load(filePath)

  //lets get some stats on this DataFrame
  df.describe().show()

  df.select(monotonically_increasing_id(),expr("*")).show(5)
  //alternative solution, except the column is added at the end
  df.withColumn("IncreasingIds", monotonically_increasing_id()).show(5)

  val threshold = 0.5
  //TODO create lucky column where the above random column > 0.5
  //TODO create a new column of unit price + random column call this column RandomPriceIncrease
  //TODO show the first 10 rows  including original data
  df.withColumn("IncreasingIds", monotonically_increasing_id())
    .withColumn("RandomWithSeed42", rand(42))
    .withColumn("lucky", col("RandomWithSeed42") > threshold)
    .withColumn("RandomPriceIncrease", col("RandomWithSeed42") + col("UnitPrice"))
    //if we wanted different order or not show all columns we could have used select (or selectExpr) here before show
    .show(10)


  //TODO filter by that lucky column (so true will be shown)
  //TODO show that including rest of the columns in original data
  df.withColumn("IncreasingIds", monotonically_increasing_id())
    .withColumn("RandomWithSeed42", rand(42))
    .withColumn("lucky", col("RandomWithSeed42") > threshold)
    .withColumn("RandomPriceIncrease", round(col("RandomWithSeed42") + col("UnitPrice"),2))
    .filter("lucky") //also filter(col("lucky")) would work
    .show(10, false)

}
