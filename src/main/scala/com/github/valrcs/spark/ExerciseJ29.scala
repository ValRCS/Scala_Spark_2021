package com.github.valrcs.spark

import org.apache.spark.sql.functions.{col, desc, expr, round, udf}

object ExerciseJ29 extends App {
  //TODO read June 29th sales data for 2011
  //TODO create your own UDF - which calculates total from Quantity and UnitPrice
  //TODO add new total column to this DataFrame using your custom UDF function
  //TODO sort by total descending
  //TODO show top 10 sales

  //TODO bonus, create total2 column which you can use regular col math
  //because for calculating two column multiplication you can live without UDF :)
  val spark = SparkUtil.createSpark("ch6")

  // in Scala
  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true") //so Spark tries to figure out the schema if column has 1 2 and 3C the type will be StringType
    .load("./src/resources/retail-data/by-day/2011-06-29.csv")

  df.printSchema()
  df.describe().show(false)
  def total(quantity:Int, unitPrice:Double):Double = quantity * unitPrice
  spark.udf.register("total", total(_:Integer, _:Double):Double)
  val totalUdf = udf(total(_:Integer, _:Double):Double)

  df.withColumn("Total", round(totalUdf(col("Quantity"), col("UnitPrice")), 2))
    .withColumn("Total2", round(col("Quantity")* col("UnitPrice"), 2))
    .withColumn("Total3", expr("total(Quantity, unitPrice)"))
    .orderBy(desc("Total"))
    .show(10, false)
}
