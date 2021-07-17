package com.github.valrcs.spark

import org.apache.spark.sql.functions.expr

object GenerateRandomData extends App {
  val spark = SparkUtil.createSpark("generateRandomness")
  val filePath = "./src/resources/csv/range100"

  val df = spark
    .range(100)
    .toDF
    .withColumnRenamed("id", "x")
    .withColumn("y", expr("round(x*3 + 2 + rand()-0.5, 3)")) //so rand() gives us 0...1 so we want an offset to fuzz in both directions

  df.show(5)

  df.write
    .format("csv")
    .option("path", filePath)
    .option("header", true)
    .mode("overwrite")
    .save

}
