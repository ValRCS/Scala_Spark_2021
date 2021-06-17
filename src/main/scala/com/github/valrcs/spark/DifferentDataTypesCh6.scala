package com.github.valrcs.spark

import org.apache.spark.sql.functions.{col, lit}

object DifferentDataTypesCh6 extends App {
  val spark = SparkUtil.createSpark("ch6")

  // in Scala
  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true") //so Spark tries to figure out the schema if column has 1 2 and 3C the type will be StringType
    .load("./src/resources/retail-data/by-day/2010-12-01.csv")

  df.printSchema()
  df.createOrReplaceTempView("dfTable")

  //so we make 3 columns without using any of the existing columns
  df.select(lit(5), lit("five"), lit(5.0)).show(4) //so no data from our csv is shown here
  spark.sql("SELECT 5, 'five' as fiverr, 5.0 from dfTable").show(4) //same as above except middle column renamed
  //so by default column names are the values of the lit

  df.where(col("InvoiceNo").equalTo(536365)) //equalTo lets us do more chaining compare to === , otherwise same
    .select("InvoiceNo", "Description", "Quantity", "UnitPrice")
    .show(5, truncate = false)

  //same as above
  df.where(col("InvoiceNo") === 536365)
    .select("InvoiceNo", "Description", "Quantity", "UnitPrice")
    .show(5, truncate = false)

  //same with SQL
  spark.sql("SELECT InvoiceNo, Description, Quantity, UnitPrice from dfTable WHERE InvoiceNo = 536365")
    .show(5,truncate = false)

  //hybrid of the above using SQL expression inside
  df.where("InvoiceNo = 536365") //so single = as in SQL
    .select("InvoiceNo", "Description", "Quantity", "UnitPrice") //i need to select AFTER filter else I would not have filter column
    .show(5, truncate = false)

  //We mentioned that you can specify Boolean expressions with multiple parts when you use and
  //or or.
  //
  // In Spark, you should always chain together AND filters as a sequential filter.
  //The reason for this is that even if Boolean statements are expressed serially (one after the other),
  //Spark will flatten all of these filters into one statement and perform the filter at the same time,
  //creating the and statement for us. Although you can specify your statements explicitly by using
  //and if you like, they’re often easier to understand and to read if you specify them serially.
  //
  // OR
  //statements need to be specified in the same statement:

  // in Scala
  val priceFilter = col("UnitPrice") > 600
  val descripFilter = col("Description").contains("POSTAGE")
  df.where(col("StockCode").isin("DOT"))
    .where(priceFilter.or(descripFilter)) //so you can keep adding AND filters while OR filters are inside each where
    .show(false)

  //so SQL of the above would be
  spark.sql("SELECT * FROM dfTable " +
    "WHERE StockCode in ('DOT') " +
    "AND (UnitPrice > 600 OR Description LIKE '%POSTAGE%')"
  ).show(false)

  //another SQL version from the book
  spark.sql("SELECT * FROM dfTable " +
    "WHERE StockCode in ('DOT') " +
    "AND(UnitPrice > 600 OR instr(Description, 'POSTAGE') >= 1)"
  ).show(false)

  //TODO more Boolean filtering
}
