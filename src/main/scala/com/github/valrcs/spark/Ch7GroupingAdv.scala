package com.github.valrcs.spark

import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, desc, expr, sum, to_date}

object Ch7GroupingAdv extends App {
  val spark = SparkUtil.createSpark("ch7")
  // in Scala
    val filePath = "./src/resources/retail-data/all/*.csv"
//  val filePath = "./src/resources/retail-data/by-day/2011-07-03.csv"

  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(filePath)
    .coalesce(5)
  df.cache() //caching frequent accesses for performance at a cost of using more memory
  df.createOrReplaceTempView("dfTable")

  df.printSchema()
  df.show(5, false)

  val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"),
    "M/d/yyyy H:mm")) //without frm to_date would not know how to parse InvoiceDate //book had wrong fmt!!
  dfWithDate.createOrReplaceTempView("dfWithDate")

  val dfNoNull = dfWithDate.na.drop("any") //sometimes you might want to replace the null values with something
  dfNoNull.createOrReplaceTempView("dfNoNull")

  //Rollup
  //When we set our grouping keys of multiple
  //columns, Spark looks at those as well as the actual combinations that are visible in the dataset. A
  //rollup is a multidimensional aggregation that performs a variety of group-by style calculations
  //for us.
  //Let’s create a rollup that looks across time (with our new Date column) and space (with the
  //Country column) and creates a new DataFrame that includes the grand total over all dates, the
  //grand total for each date in the DataFrame, and the subtotal for each country on each date in the
  //DataFrame:

  val rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum("Quantity"))
    .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")
    .orderBy("Date")

  rolledUpDF.show(10, false)

  dfNoNull.groupBy("Date", "Country").agg(sum("Quantity"))
    .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")
    .orderBy("Date").show(10, false)

  //so by using rollup our aggregation can include aggregations for everything (all entries ),and also groupings by single column
  //so in this example we get 4 aggregations (("Data", "Country"), just "Date", just "Country", and eerythingg together
  rolledUpDF.where(col("Country") === "Australia")
    .orderBy(desc("total_quantity"))
    .show(10, false)


}
