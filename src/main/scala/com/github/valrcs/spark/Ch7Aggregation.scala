package com.github.valrcs.spark

import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{approx_count_distinct, avg, collect_list, collect_set, corr, countDistinct, covar_pop, covar_samp, expr, first, kurtosis, last, max, min, skewness, stddev_pop, stddev_samp, sum, sumDistinct, var_pop, var_samp}

object Ch7Aggregation extends App {
  println("Aggregating is the act of collecting something together and is a cornerstone of big data analytics")

  val spark = SparkUtil.createSpark("ch7")

//  In addition to working with any type of values, Spark also allows us to create the following
//    groupings types:
//    The simplest grouping is to just summarize a complete DataFrame by performing an
//  aggregation in a select statement.
//    A “group by” allows you to specify one or more keys as well as one or more
//    aggregation functions to transform the value columns.
//    A “window” gives you the ability to specify one or more keys as well as one or more
//    aggregation functions to transform the value columns. However, the rows input to the
//  function are somehow related to the current row.
//  A “grouping set,” which you can use to aggregate at multiple different levels. Grouping
//  sets are available as a primitive in SQL and via rollups and cubes in DataFrames.
//    A “rollup” makes it possible for you to specify one or more keys as well as one or more
//  aggregation functions to transform the value columns, which will be summarized
//    hierarchically.
//      A “cube” allows you to specify one or more keys as well as one or more aggregation
//  functions to transform the value columns, which will be summarized across all
//    combinations of columns.

  // in Scala
  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("./src/resources/retail-data/all/*.csv")
    .coalesce(5)
  df.cache() //caching frequent accesses for performance at a cost of using more memory
  df.createOrReplaceTempView("dfTable")

  df.printSchema()
  df.sample(0.1).show(5, false)
  df.describe().show(false)

  println(df.count()) //count is an eager function meaning it returns immediately
  //count is actually an action as
  //opposed to a transformation, and so it returns immediately. You can use count to get an idea of
  //the total size of your dataset but another common pattern is to use it to cache an entire
  //DataFrame in memory, just like we did in this example.

  //there is also count used as a transformation (meaning it will not run immediately)
  df.select(functions.count("StockCode")).show()
  df.select(functions.count("CustomerID")).show()
  spark.sql("SELECT COUNT(CustomerID) FROM dfTable").show()
  spark.sql("SELECT COUNT(*) FROM dfTable").show()

  //WARNING
  //There are a number of gotchas when it comes to null values and counting. For instance, when
  //performing a count(*), Spark will count null values (including rows containing all nulls). However,
  //when counting an individual column, Spark will not count the null values.

  df.select(countDistinct("StockCode")).show() // 4070
  spark.sql("SELECT COUNT(DISTINCT StockCode) FROM DFTABLE").show()

  //Often, we find ourselves working with large datasets and the exact distinct count is irrelevant.
  //There are times when an approximation to a certain degree of accuracy will work just fine, and
  //for that, you can use the approx_count_distinct function
  df.select(approx_count_distinct("StockCode", 0.1)).show()
  df.select(approx_count_distinct("StockCode")).show() //rsd 0.05 is default
  df.select(approx_count_distinct("StockCode", rsd=0.025)).show()

  //You can get the first and last values from a DataFrame by using these two obviously named
  //functions. This will be based on the rows in the DataFrame, not on the values in the DataFrame
  df.select(first("StockCode"), last("StockCode")).show()

  df.select(min("Quantity"), max("Quantity")).show()
  df.selectExpr("min(Quantity)", "max(Quantity)").show()
  spark.sql("SELECT min(Quantity), max(Quantity) FROM dfTable").show()

  //SUM
  df.select(sum("Quantity")).show()

  //sumDistinct - sum only distinct values - so each distinct entry is summed only once
  //sumDistinct(1,1,2,2,3) would be 6
  df.select(sumDistinct("Quantity")).show()

  df.select(
    functions.count("Quantity").alias("total_transactions"),
    sum("Quantity").alias("total_purchases"),
    avg("Quantity").alias("avg_purchases"), //same as mean
    expr("mean(Quantity)").alias("mean_purchases"))
    .selectExpr(
      "total_purchases/total_transactions",
      "avg_purchases",
      "mean_purchases").show()

  //NOTE
  //You can also average all the distinct values by specifying distinct. In fact, most aggregate functions
  //support doing so only on distinct values
  spark.sql("SELECT AVG(DISTINCT StockCode), COUNT(DISTINCT StockCode) FROM dfTable").show()

//  Calculating the mean naturally brings up questions about the variance and standard deviation.
//    These are both measures of the spread of the data around the mean. The variance is the average
//  of the squared differences from the mean, and the standard deviation is the square root of the
//    variance. You can calculate these in Spark by using their respective functions. However,
//  something to note is that Spark has both the formula for the sample standard deviation as well as
//  the formula for the population standard deviation. These are fundamentally different statistical
//  formulae, and we need to differentiate between them. By default, Spark performs the formula for
//    the sample standard deviation or variance if you use the variance or stddev functions.
//    You can also specify these explicitly or refer to the population standard deviation or variance:

  //for those who have not done statistics in a while:
  //https://www.khanacademy.org/math/statistics-probability/summarizing-quantitative-data/variance-standard-deviation-sample/a/population-and-sample-standard-deviation-review
  df.select(var_pop("Quantity"), var_samp("Quantity"),
    stddev_pop("Quantity"), stddev_samp("Quantity")).show()

  //skewness and kurtosis
  //Skewness and kurtosis are both measurements of extreme points in your data. Skewness
  //measures the asymmetry of the values in your data around the mean, whereas kurtosis is a
  //measure of the tail of data. These are both relevant specifically when modeling your data as a
  //probability distribution of a random variable.
  //https://en.wikipedia.org/wiki/Skewness
  //https://en.wikipedia.org/wiki/Kurtosis

  df.select(skewness("Quantity"), kurtosis("Quantity")).show()
  df.selectExpr("skewness(Quantity)","kurtosis(Quantity)").show()
  spark.sql("SELECT skewness(Quantity), kurtosis(Quantity) FROM dfTable").show()

  //Covariance and Correlation
  //We discussed single column aggregations, but some functions compare the interactions of the
  //values in two difference columns together. Two of these functions are cov and corr, for
  //covariance and correlation, respectively. Correlation measures the Pearson correlation
  //coefficient, which is scaled between –1 and +1. The covariance is scaled according to the inputs
  //in the data.
  //Like the var function, covariance can be calculated either as the sample covariance or the
  //population covariance. Therefore it can be important to specify which formula you want to use.
  //Correlation has no notion of this and therefore does not have calculations for population or
  //sample.

  //https://en.wikipedia.org/wiki/Correlation_and_dependence

  df.select(corr("InvoiceNo", "Quantity"), covar_samp("InvoiceNo", "Quantity"),
    covar_pop("InvoiceNo", "Quantity")).show()
  spark.sql("SELECT corr(InvoiceNo, Quantity)," +
    " covar_samp(InvoiceNo, Quantity)," +
    " covar_pop(InvoiceNo, Quantity)" +
    " FROM dfTable")
    .show()

  //so we want to find out whether people buy more or less when the unitprice raises
  df.select(corr("UnitPrice", "Quantity")).show()
  //so looks like there is no statistically significant correlation between UnitPrice and Quantity


  //Aggregating to Complex Types
  //In Spark, you can perform aggregations not just of numerical values using formulas, you can also
  //perform them on complex types. For example, we can collect a list of values present in a given
  //column or only the unique values by collecting to a set.
  //You can use this to carry out some more programmatic access later on in the pipeline or pass the
  //entire collection in a user-defined function (UDF):

  df.agg(collect_set("Country"), collect_list("Country")).show()
  spark.sql("SELECT collect_set(Country), collect_list(Country) FROM dfTable").show()

}
