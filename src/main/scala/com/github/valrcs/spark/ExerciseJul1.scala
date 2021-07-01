package com.github.valrcs.spark

import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, corr, desc, expr}

object ExerciseJul1 extends App {
  //TODO read data from July 1st of 2011 CSV
  //TODO find what is the correlation coefficient for UnitPrice and Quantity (should be close to 0)

  //TODO find top 10 countries that purchased items that day
  //TODO find count of items purchased in each invoice
  //TODO find average quantity of items purchased per invoice
  //TODO find sum of all items purchases per invoice



  val spark = SparkUtil.createSpark("exjul1")

  // in Scala
  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true") //so Spark tries to figure out the schema if column has 1 2 and 3C the type will be StringType
    .load("./src/resources/retail-data/by-day/2011-07-01.csv")

  df.printSchema()
  df.describe().show(false)

  df.select(corr("UnitPrice", "Quantity")).show()

  df.groupBy("Country")
    .count()
    .orderBy(desc("count"))
    .show(10)

  df.
    groupBy("InvoiceNo")
    .agg("Quantity"->"count",
      "Quantity"->"avg",
      "Quantity"->"sum"
    ).show()

  //Bonus TODO Find Top 5 biggest purchases per invoice that day - you will need a total column first before grouping
  df.
    withColumn("Total", col("Quantity")*col("UnitPrice")). //we could have used our own UDF as well
    groupBy("InvoiceNo")
    .agg("Quantity"->"count",
      "Quantity"->"avg",
      "Quantity"->"sum",
      "Total"->"sum",
      "Total"->"avg" //TODO how to round with Map
    )
    .orderBy(desc("sum(Total)"))
    .withColumn("sum(Total)", functions.round(col("sum(Total)"),2)) //so we are overwriting the old column
    .show(10, false)

  df.withColumn("Total", col("Quantity")*col("UnitPrice"))
    .groupBy("InvoiceNo")
    .agg(
    functions.count("Quantity").alias("Count"),
    expr("sum(Quantity) sum"),
    expr("stddev_pop(Quantity) std"),
    expr("sum(UnitPrice) pricesSum"),
    expr("avg(UnitPrice) avgUPrice"),
      expr("round(sum(Total),2) sumTotal"), //so with expr we can round immediately
      expr("avg(Total)")
    )
    .orderBy(desc("sumTotal"))
    .show(10, false)
}
