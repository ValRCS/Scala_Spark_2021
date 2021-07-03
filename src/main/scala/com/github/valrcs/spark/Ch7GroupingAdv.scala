package com.github.valrcs.spark

import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col,count, desc, expr, grouping_id, sum, to_date}

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

  val dfWithDate = df
    .withColumn("Total", col("UnitPrice") * col("Quantity"))
    .withColumn("date", to_date(col("InvoiceDate"),
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

  //Now where you see the null values is where you’ll find the grand totals. A null in both rollup
  //columns specifies the grand total across both of those columns
  rolledUpDF.show(10, false)

  dfNoNull.groupBy("Date", "Country").agg(sum("Quantity"))
    .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")
    .orderBy("Date").show(10, false)

  //so by using rollup our aggregation can include aggregations for everything (all entries ),and also groupings by single column
  //so in this example we get 3 aggregations (("Data", "Country"), just "Date",  and grand total
  rolledUpDF.where(col("Country") === "Australia")
    .orderBy(desc("total_quantity"))
    .show(10, false)

  //Cube
  //A cube takes the rollup to a level deeper. Rather than treating elements hierarchically, a cube
  //does the same thing across all dimensions. This means that it won’t just go by date over the
  //entire time period, but also the country. To pose this as a question again, can you make a table
  //that includes the following?
  //The total across all dates and countries
  //The total for each date across all countries
  //The total for each country on each date
  //The total for each country across all dates
  //The method call is quite similar, but instead of calling rollup, we call cube

  val cubedDF = dfNoNull
    .cube("Date", "Country")
    .agg(sum(col("Quantity")))
//    .select("Date", "Country", "sum(Quantity)")
    .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity") //renaming columns on the fly by
    .orderBy("Date")

  cubedDF
    .orderBy(desc("total_quantity"))
    .show(10, false)

  cubedDF.where(col("Country") === "Australia")
    .orderBy(desc("total_quantity"))
    .show(10, false)

  //Grouping Metadata
  //Sometimes when using cubes and rollups, you want to be able to query the aggregation levels so
  //that you can easily filter them down accordingly. We can do this by using the grouping_id,
  //which gives us a column specifying the level of aggregation that we have in our result set. The
  //query in the example that follows returns four distinct grouping IDs

  dfNoNull.cube("customerId", "stockCode")
    .agg(grouping_id(), sum("Quantity"))
    .orderBy(expr("grouping_id()").desc, desc("sum(Quantity)"))
    .show(10, false)

  //lets add the 3rd column for our aggregations
  dfNoNull.cube("customerId", "stockCode", "Date")
    .agg(grouping_id(), sum("Quantity"))
    .orderBy(expr("grouping_id()").desc, desc("sum(Quantity)"))
    .show(10, false)


  dfNoNull.cube("customerId", "stockCode", "Date")
    .agg(grouping_id(), sum("Quantity"))
    .where(col("grouping_id()") === 0) //so this would be same as groupby all 3 columns ONLY
    .orderBy(desc("sum(Quantity)"))
    .show(10, false)

  val df4agg = dfNoNull.cube("customerId", "stockCode", "Date", "Country")
    .agg(grouping_id(),
      sum("Quantity"),
      sum("Total"),
      count("Total")
    )
    .withColumn("sum(Total)", functions.round(col("sum(Total)"), 2))
    .orderBy(expr("grouping_id()").desc, desc("sum(Quantity)"))

  df4agg
//    withColumn("sum(Total)", functions.round(col("sum(Total)"), 2)) //so we only round up when we show something
      .show(10, false)

  //let's see all the different grouping ids that we do have
  //and get top 5 sales for all different aggregations
  (0 to 15).foreach(id => df4agg
    .where(col("grouping_id()") === id)
    .orderBy(desc("sum(Total)"))
    .show(5, false))

  //TODO exercise
  //get top 5 sales(which is sum(Total)) for each single grouping of the following columns
  //"customerId", "stockCode", "Date", "Country"
  //use dfNoNull for your aggregations
}
