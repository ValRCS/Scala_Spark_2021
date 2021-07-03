package com.github.valrcs.spark

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, cume_dist, dense_rank, desc, expr, lag, lead, ntile, percent_rank, to_date}

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

  //Window Functions
  //You can also use window functions to carry out some unique aggregations by either computing
  //some aggregation on a specific “window” of data, which you define by using a reference to the
  //current data. This window specification determines which rows will be passed in to this function


  //To demonstrate, we will add a date column that will convert our invoice date into a column that
  //contains only date information (not time information, too):

  val dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"),
    "M/d/yyyy H:mm")) //without frm to_date would not know how to parse InvoiceDate //book had wrong fmt!!
  dfWithDate.createOrReplaceTempView("dfWithDate")

  dfWithDate.printSchema()
  dfWithDate.show(3, false)

  //The first step to a window function is to create a window specification. Note that the partition
  //by is unrelated to the partitioning scheme concept that we have covered thus far. It’s just a
  //similar concept that describes how we will be breaking up our group. The ordering determines
  //the ordering within a given partition, and, finally, the frame specification (the rowsBetween
  //statement) states which rows will be included in the frame based on its reference to the current
  //input row.
  //
  // In the following example, we look at all previous rows up to the current row:
  val windowSpec = Window
    .partitionBy("CustomerId", "date")
    .orderBy(col("Quantity").desc)
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  //Now we want to use an aggregation function to learn more about each specific customer. An
  //example might be establishing the maximum purchase quantity over all time. To answer this, we
  //use the same aggregation functions that we saw earlier by passing a column name or expression.
  //In addition, we indicate the window specification that defines to which frames of data this
  //function will apply:

  val maxPurchaseQuantity = functions.max(col("Quantity")).over(windowSpec)


  //You will notice that this returns a column (or expressions). We can now use this in a DataFrame
  //select statement. Before doing so, though, we will create the purchase quantity rank. To do that
  //we use the dense_rank function to determine which date had the maximum purchase quantity
  //for every customer. We use dense_rank as opposed to rank to avoid gaps in the ranking
  //sequence when there are tied values (or in our case, duplicate rows):

  val purchaseDenseRank = dense_rank().over(windowSpec)
  val purchaseRank = functions.rank().over(windowSpec)

  dfWithDate
    .where("CustomerId IS NOT NULL") //Null customers would wreck havoc on ranking functions
    .orderBy("CustomerId")
    .select(
      col("CustomerId"),
      col("InvoiceNo"),
      col("date"),
      col("Quantity"),
      purchaseRank.alias("quantityRank"),
      purchaseDenseRank.alias("quantityDenseRank"),
      maxPurchaseQuantity.alias("maxPurchaseQuantity"))
    .show(35, false)

  //sql "one liner"
    spark.sql("""SELECT CustomerId, date, Quantity,
                |rank(Quantity) OVER (PARTITION BY CustomerId, date
                |ORDER BY Quantity DESC NULLS LAST
                |ROWS BETWEEN
                |UNBOUNDED PRECEDING AND
                |CURRENT ROW) as rank,
                |dense_rank(Quantity) OVER (PARTITION BY CustomerId, date
                |ORDER BY Quantity DESC NULLS LAST
                |ROWS BETWEEN
                |UNBOUNDED PRECEDING AND
                |CURRENT ROW) as dRank,
                |max(Quantity) OVER (PARTITION BY CustomerId, date
                |ORDER BY Quantity DESC NULLS LAST
                |ROWS BETWEEN
                |UNBOUNDED PRECEDING AND
                |CURRENT ROW) as maxPurchase
                |FROM dfWithDate WHERE CustomerId IS NOT NULL ORDER BY CustomerId""".stripMargin)
      .show(35,false)

  //Top 10 sales overall
  df
    .withColumn("Total", functions.round(col("Quantity")*col("UnitPrice"),2))
    .orderBy(desc("Total"))
    .show(10,false)

  //now we should create a ranking and specify a window
  val countrySpec = Window
    .partitionBy("Country")
    .orderBy(col("Total").desc)
    .rowsBetween(Window.unboundedPreceding, Window.currentRow) //we will see how Spark optimizes because ranking 500k items could take a while

  val reverseCountrySpec = Window
    .partitionBy("Country")
    .orderBy(col("Total").asc) //asc is the default
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

  val simpleCountrySpec = Window
    .partitionBy("Country")
    .orderBy(desc("Total"))

  val countryPurchaseDenseRank = dense_rank().over(countrySpec) //so we start with Austria
  val countryPurchaseRank = functions.rank().over(countrySpec) //so we start with Austria
  val countryPercentageRank = percent_rank().over(countrySpec) //percentage_rank is actually in range 0 to 1 so *100 for actual percentage
  val countryReversePercentageRank = percent_rank().over(reverseCountrySpec)
  val countryNtile = ntile(40).over(countrySpec) //maybe better to use ntile as needed because here we have fixed the number of buckets to 40
  val countryCumDist = cume_dist().over(simpleCountrySpec) //so needed simple spec for cumulative distribution
  val leadCountry = lead("Total", 3).over(simpleCountrySpec) //so we are looking at the value of total 2 rows ahead
  //we will get null here when we reach the last 3 rows of current grouping/partition in this case country
  val lagCountry = lag("Total", 4).over(simpleCountrySpec)

  df
    .withColumn("Total", functions.round(col("Quantity")*col("UnitPrice"),2))
    .orderBy(desc("Total"))
    .withColumn("CountryDenseRank", countryPurchaseDenseRank)
    .withColumn("CountryRank", countryPurchaseRank)
    .withColumn("revPercRank", countryReversePercentageRank)
    .withColumn("percRank", countryPercentageRank) //last spec wins the ordering
    .withColumn("ntile40", countryNtile)
    .withColumn("cumDistribution", countryCumDist)
    .withColumn("lead3", leadCountry)
    .withColumn("lag4", lagCountry)
//    .withColumn("CountryCumulativeDistribution", countryCumDist)
    .show(25,false)
}
