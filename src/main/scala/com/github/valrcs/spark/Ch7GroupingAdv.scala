package com.github.valrcs.spark

import org.apache.spark.sql
import org.apache.spark.sql.{Dataset, Row, functions}
import org.apache.spark.sql.functions.{asc, col, count, desc, expr, grouping_id, sum, to_date}

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

  dfNoNull
    .groupBy("customerID")
    .agg(
      sum("Quantity"),
      sum("Total"),
      count("Total")
    )
    .orderBy(desc("sum(Total)"))
    .show(5, false)

  //since we are lazy programmers we want to avoid copy and pasting multiple single group queries
  //so we will make a function to help us along we will give a dataframe and grouping column
  //and we will receive a aggregated dataframe
  //requirement being that Datafmae has Quantity and Total column names and obviously the groupColName as well
  def topSalesBy(df: sql.DataFrame, groupColName: String): Dataset[Row] = {
    df
      .groupBy(groupColName)
      .agg(
        sum("Quantity"),
        sum("Total"),
        count("Total")
      )
      .orderBy(desc("sum(Total)"))
  }

  val colGroups = Seq("customerId", "stockCode", "Date", "Country")

  //if I wanted to save them I could do map, but then of course memory could become an issue for really large data sets.
  //instead i will use foreach to show the results
  colGroups.foreach(colName => topSalesBy(dfNoNull, colName).show(5, false))

//  Pivot
//  Pivots make it possible for you to convert a row into a column. For example, in our current data
//    we have a Country column. With a pivot, we can aggregate according to some function for each
//  of those given countries and display them in an easy-to-query way

  // in Scala
  //without Pivot
  dfWithDate
    .groupBy("date", "Country")
    .agg(
      sum("Quantity"),
      sum("UnitPrice"),
      sum("CustomerID"), //of course CustomerIDs do not make sense to add usually :)
      sum("Total")
    ) //summing countries does not really work, we get null
    .orderBy(asc("date"))
    .show(5, false)

  //with Pivot so making lots of new columns for each country and sum combination
  val pivoted = dfWithDate
    .groupBy("date")
    .pivot("Country") //so we moved a row of country values into columns, useful when you do not have too many distinct row
    .sum()
    .orderBy(asc("date")) //order AFTER aggregation

  pivoted.show(5,false)

  //so again Pivot is quite nie for exploring data when you do not have too many distinct values in pivot column
  //if you have 1000 different values you will get 1000 * number of aggregation columns


  //User-Defined Aggregation Functions
  //User-defined aggregation functions (UDAFs) are a way for users to define their own aggregation
  //functions based on custom formulae or business rules. You can use UDAFs to compute custom
  //calculations over groups of input data (as opposed to single rows). Spark maintains a single
  //AggregationBuffer to store intermediate results for every group of input data.
  //To create a UDAF, you must inherit from the UserDefinedAggregateFunction base class and
  //implement the following methods:
  //inputSchema represents input arguments as a StructType
  //bufferSchema represents intermediate UDAF results as a StructType
  //dataType represents the return DataType
  //deterministic is a Boolean value that specifies whether this UDAF will return the
  //same result for a given input
  //initialize allows you to initialize values of an aggregation buffer
  //update describes how you should update the internal buffer based on a given row
  //merge describes how two aggregation buffers should be merged
  //evaluate will generate the final result of the aggregation
  //The following example implements a BoolAnd, which will inform us whether all the rows (for a
  //given column) are true; if they’re not, it will return false:

  import org.apache.spark.sql.expressions.MutableAggregationBuffer
  import org.apache.spark.sql.expressions.UserDefinedAggregateFunction //depreceated so will need to read on new method eventually
  import org.apache.spark.sql.types._
  class BoolAnd extends UserDefinedAggregateFunction {
    def inputSchema: org.apache.spark.sql.types.StructType =
      StructType(StructField("value", BooleanType) :: Nil)
    def bufferSchema: StructType = StructType(
      StructField("result", BooleanType) :: Nil
    )
    def dataType: DataType = BooleanType
    def deterministic: Boolean = true
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = true
    }
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getAs[Boolean](0) && input.getAs[Boolean](0)
    }
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getAs[Boolean](0) && buffer2.getAs[Boolean](0)
    }
    def evaluate(buffer: Row): Any = {
      buffer(0)
    }
  }

  //Now, we simply instantiate our class and/or register it as a function:
  val ba = new BoolAnd
  spark.udf.register("booland", ba) //again depreceated eventally will need updating

  spark.range(1)
    .selectExpr("explode(array(TRUE, TRUE, TRUE)) as tcol") //so 3 here
    .selectExpr("explode(array(TRUE, FALSE, TRUE)) as fcol", "tcol") //so 3 x 3 here since we took tcol with 3 and then exploded
    .show(10, false) //so 9 rows actually


  spark.range(1)
    .selectExpr("explode(array(TRUE, TRUE, TRUE)) as t")
    .selectExpr("explode(array(TRUE, FALSE, TRUE)) as f", "t")
    .select(ba(col("t")), expr("booland(f)"))
    .show()
}
