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

//  df.write
//    .format("csv")
//    .option("path", filePath)
//    .option("header", true)
//    .mode("overwrite")
//    .save

  val df2d = spark.range(100)
    .toDF
    .withColumnRenamed("id", "x1")
    .withColumn("x2", expr("x1+1000")) //x2 will dominate x1 at least at the beginnning
    .withColumn("y", expr("round(500 + x1*2.5 + x2*4 + rand()*20-10, 3)")) //random +- 10 noise

  df2d.show(5, false)

    df2d.write
      .format("csv")
      .option("path", "./src/resources/csv/range2d")
      .option("header", true)
      .mode("overwrite")
      .save

}
