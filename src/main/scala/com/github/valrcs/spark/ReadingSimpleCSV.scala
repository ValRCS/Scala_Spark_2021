package com.github.valrcs.spark

object ReadingSimpleCSV extends App {
  println("Reading a simple CSV with some missing values")

  val filePath = "./src/resources/csv/simple-ints.csv"

  val spark = SparkUtil.createSpark("fixingCSV")

  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true") //so Spark tries to figure out the schema if column has 1 2 and 3C the type will be StringType
    .load(filePath)

  df.describe().show(false)
  df.show()


  df.na.drop(how="all", Seq("desc")).show()


}
