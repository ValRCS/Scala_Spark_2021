package com.github.valrcs.spark

object Ch7Grouping extends App {
  val spark = SparkUtil.createSpark("ch7")
  // in Scala
  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("./src/resources/retail-data/all/*.csv")
    .coalesce(5)
  df.cache() //caching frequent accesses for performance at a cost of using more memory
  df.createOrReplaceTempView("dfTable")

  df.printSchema()
  df.show(5, false)

  //

}
