package com.github.valrcs.spark

import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{desc, expr, first, round, sum}

object ExerciseJul5 extends App {
  //TODO load 2011-07-05.csv
  //TODO load customers.csv - notice it is in retail-data folder
  //TODO Join both DataFrames on CustomerId and ID - INNER JOIN - you will have quite a bit of rows - probably around 30 ?
  //TODO Show in sorted order by purchase totals(Quantity * UnitPrice)

  //BONUS aggregate the results by CustomerId and show total sales for each customer for that day (so only 3 rows)

  val spark = SparkUtil.createSpark("jul5exercise")

  val purchasesFilePath = "./src/resources/retail-data/by-day/2011-07-05.csv"
  val customersFilePath = "./src/resources/retail-data/customers.csv"

  val purchases = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true") //so Spark tries to figure out the schema if column has 1 2 and 3C the type will be StringType
    .load(purchasesFilePath)

  val customers = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true") //so Spark tries to figure out the schema if column has 1 2 and 3C the type will be StringType
    .load(customersFilePath)

  purchases.printSchema()
  purchases.show(5,false)

  customers.printSchema()
  customers.show(false)

  val joinExpression = purchases.col("CustomerID") === customers.col("Id")

  val df = purchases.join(customers, joinExpression) //inner join by default without 3rd parameter

  df.show(25, false)

  df.withColumn("Total", expr("round(Quantity * UnitPrice, 2)"))
//    .withColumn("Total", round(col("Quantity")* col("UnitPrice"), 2)) //what I usually use but expr above is fine too
    .orderBy(desc("Total"))
    .show(10, false)

  //we could have saved this dataframe
  df
    .withColumn("Total", expr("round(Quantity * UnitPrice, 2)"))
    //    .withColumn("Total", round(col("Quantity")* col("UnitPrice"), 2)) //what I usually use but expr above is fine too
//    .orderBy(desc("Total")) //not needed here
    .groupBy("Id")
    .agg(
//      round(sum("Total"),2),
      expr("round(sum(Total)) as TotalSum"),
      expr("first(InvoiceDate) as PurchaseDate"),
      functions.count(" FirstName"),
      first(" LastName"),
      expr("count(Quantity) as distinctSales")
    )
    .orderBy(desc("TotalSum"))
    .show(false) //we should only have the 3 customers
}
