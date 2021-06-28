package com.github.valrcs.spark

import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{array_contains, col, desc, explode, split}

object ExerciseJ28 extends App {
  //TODO read 28th of Jun 2011 sales data
  //TODO add new column descWords with description words split into Array
  //TODO add another column hasRed which indicates whether there is RED among the words
  //TODO add countWords column
  //TODO show first 10 rows ordered by countWords

  val spark = SparkUtil.createSpark("ch6")

  // in Scala
  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true") //so Spark tries to figure out the schema if column has 1 2 and 3C the type will be StringType
    .load("./src/resources/retail-data/by-day/2011-06-28.csv")

  df.printSchema()
  df.describe().show(false)

    df
    .withColumn("descWords",split(col("Description"), " "))
    .withColumn("hasRed", array_contains(col("descWords"), "RED"))
    .withColumn("countWords", functions.size(col("descWords")))
    .orderBy(desc("countWords"))
    .show(10,false)

  //how about getting top 10 of those rows(records) which have RED in them?
  df
    .withColumn("descWords",split(col("Description"), " "))
    .withColumn("hasRed", array_contains(col("descWords"), "RED"))
    .withColumn("countWords", functions.size(col("descWords")))
    .filter(col("hasRed"))
    .orderBy(desc("countWords"))
    .show(10,false)

  //Bonus
  //TODO explode the descWords column
  df
    .withColumn("descWords",split(col("Description"), " "))
    .withColumn("hasRed", array_contains(col("descWords"), "RED"))
    .withColumn("countWords", functions.size(col("descWords")))
    .orderBy(desc("countWords"))
    .withColumn("exploded", explode(col("descWords")))
    .show(10,false)
}
