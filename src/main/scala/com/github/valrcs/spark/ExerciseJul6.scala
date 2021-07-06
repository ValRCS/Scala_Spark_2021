package com.github.valrcs.spark

object ExerciseJul6 extends App {
  //TODO read parquet files folder from src/resources/regression
  //TODO print schema
  //TODO show a sample of rows
  //TODO show some statistics

  //possibly something did not commit to git
  val spark = SparkUtil.createSpark("exJul06")

//  val filePath = "./src/resources/regression/*.parquet" //using wildcard is certainly fine
  val filePath = "./src/resources/regression" //we could also just supply the folder with the parquet files

  val parquetDF = spark
    .read
    .format("parquet")
    .load(filePath)

  parquetDF.printSchema()
  parquetDF.describe().show()
  parquetDF.show(10,false)
  //turns out we only have 5 rows out of 7 parque files (in gzip format)
  //it could happen but usually you would expect more rows
}
