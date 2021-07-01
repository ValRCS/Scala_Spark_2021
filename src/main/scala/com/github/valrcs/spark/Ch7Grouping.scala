package com.github.valrcs.spark

import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{desc, expr}

object Ch7Grouping extends App {
  val spark = SparkUtil.createSpark("ch7")
  // in Scala
  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("./src/resources/retail-data/all/*.csv")
    .coalesce(5)
  df.cache() //caching frequent accesses for performance at a cost of using more memory
  df.createOrReplaceTempView("dfTable")

  df.printSchema()
  df.show(5, false)

  //A more common task is to
  //perform calculations based on groups in the data. This is typically done on categorical data for
  //which we group our data on one column and perform some calculations on the other columns
  //that end up in that group.

  df.groupBy("Country").count().show(10, false)
  //ordered Top 10 purchasers
  df.groupBy("Country")
    .count()
    .orderBy(desc("count"))
    .show(10, false)


  //We do this grouping in two phases. First we specify the column(s) on which we would like to
  //group, and then we specify the aggregation(s). The first step returns a
  //RelationalGroupedDataset, and the second step returns a DataFrame.
  df.groupBy("InvoiceNo", "CustomerId").count().show()
  spark.sql("SELECT InvoiceNo, CustomerId, count(*) FROM dfTable GROUP BY InvoiceNo, CustomerId").show()

  df.groupBy("InvoiceNo", "CustomerId")
    .count()
    .orderBy(desc("count"))
    .show(10)

  //so if we group just by CustomerId (ignoring specific purchases) we should get null customer as the biggest
  df.groupBy("CustomerId")
    .count()
    .orderBy(desc("count"))
    .show(10)

  //Grouping with Expressions
  //As we saw earlier, counting is a bit of a special case because it exists as a method. For this,
  //usually we prefer to use the count function. Rather than passing that function as an expression
  //into a select statement, we specify it as within agg. This makes it possible for you to pass-in
  //arbitrary expressions that just need to have some aggregation specified. You can even do things
  //like alias a column after transforming it for later use in your data flow:

  // in Scala
  df.groupBy("InvoiceNo").agg(
    functions.count("Quantity").alias("quan"),
    expr("count(Quantity)"),
    expr("count(Quantity) Quan3"))
    .show()

//  Grouping with Maps
//  Sometimes, it can be easier to specify your transformations as a series of Maps for which the key
//  is the column, and the value is the aggregation function (as a string) that you would like to
//    perform. You can reuse multiple column names if you specify them inline, as well:
  // in Scala
  df.groupBy("InvoiceNo")
    .agg("Quantity"->"avg",
      "Quantity"->"count",
      "Quantity"->"sum",
      "Quantity"->"stddev_pop" //so just add , and another Map entry
    ).show()
  //TODO unclear in the above example on how to renamed column names immediately (we could do it after aggregation of course
  //that is why expr based aggregations might be preferable

  df.groupBy("InvoiceNo")
    .agg("Quantity"->"avg",
      "Quantity"->"count",
      "Quantity"->"sum",
      "Quantity"->"stddev_pop",
      "UnitPrice"->"sum",
      "UnitPrice"->"avg"
      //so just add , and another Map entry
    ).show()

  //same as above
  df.groupBy("InvoiceNo").agg(
    functions.count("Quantity").alias("Count"),
    expr("sum(Quantity) sum"),
    expr("stddev_pop(Quantity) std"),
    expr("sum(UnitPrice) pricesSum"),
    expr("avg(UnitPrice) avgUPrice"))
    .show()

}
