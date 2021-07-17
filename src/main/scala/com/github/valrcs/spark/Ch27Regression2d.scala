package com.github.valrcs.spark

object Ch27Regression2d extends App {
  val spark = SparkUtil.createSpark("regressions")
  val filePath = "./src/resources/csv/range2d.csv"

  val df = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", filePath)
    .load

  df.printSchema()
  df.show(5)
}
