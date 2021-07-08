package com.github.valrcs.spark

import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, expr, lit, regexp_extract}

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

  val nameRegex = "\\d,\\s*([A-Z]{1}[a-z]*)"
  val carRegex = ",.*,\\s*([A-Z].*)," //so , anything , then possible whitespace and then Car with anything until 3rd comma

  df
    .withColumn("nameAndAll", regexp_extract(col("value"), nameRegex, 0))
    .withColumn("name", regexp_extract(col("value"), nameRegex, 1))
    .withColumn("car", regexp_extract(col("value"), carRegex, 1))

    .show(false)

  val ndf = df
    .withColumn("name", regexp_extract(col("value"), nameRegex, 1))
    .withColumn("car", regexp_extract(col("value"), carRegex, 1))
    .withColumn("year", regexp_extract(col("value"), regexString, 1))

  ndf.show(false)

  //now of course it would be beneficial to store the ndf as parquet or csv or some other stronger structure format

  ndf
    .select(col("car"))
    .na
    .drop //so we drop only nulls from car column
    .filter("car != ''") //only non empty car strings
    .write
    .mode("overwrite")
    .text("./src/resources/cars.txt")

  //so how about saving multiple columns in a text file
  //well we can create a new text column with all the other columns
  //again saving it as better structured date would be better

  ndf
    .filter("car != '' AND name != ''")
    .select(functions.concat(col("name"), lit(" "), col("year"), lit(":::"), col("car")))
    .write
    .mode("overwrite")
    .text("./src/resources/dirty-data.txt")
}
