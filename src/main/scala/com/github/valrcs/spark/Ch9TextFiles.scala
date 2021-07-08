package com.github.valrcs.spark

import org.apache.spark.sql.functions.{col, expr, regexp_extract}

object Ch9TextFiles extends App {
  val spark = SparkUtil.createSpark("ch9DataSources")
  val filePath = "./src/resources/csv/dirty-data.csv"

  val df = spark.read
    .textFile(filePath)

  df.printSchema()

  df.show(false)

  val regexString = ".*([1-2]\\d(\\d\\d)).*"

  df.withColumn("full match", regexp_extract(col("value"), regexString, 0)) //full match or empty string
    .withColumn("year", regexp_extract(col("value"), regexString, 1)) //so we want what whatever is inside parenthesis in our regex
    .withColumn("yy", regexp_extract(col("value"), regexString, 2)) //so we want what whatever is inside 2nd parenthesis in our regex
  .show(false)

  //TODO create column called name, which extracts name, - name being defined as something after first comma which starts with a capital Letter


}
