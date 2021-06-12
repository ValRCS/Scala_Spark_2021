package com.github.valrcs.spark

import org.apache.spark.sql.SparkSession

object StructuredAPIOverview extends App {
  println("CH4: Structured API Overview - look into DataFrames and Datasets")

  println(s"Reading CSVs with Scala version: ${util.Properties.versionNumberString}")

  val spark = SparkSession.builder().appName("tourDeSpark").master("local").getOrCreate()
  spark.conf.set("spark.sql.shuffle.partitions", "5") //recommended for local, default is 200?
  println(s"Session started on Spark version ${spark.version}")

  // in Scala
  val df = spark.range(30).toDF("number")
  val newDf = df.select(df.col("number") + 100) //so this adds 100 to all cells in a particular column
  newDf.show(30) // default is 20

  // To Spark (in Scala), DataFrames are
  //simply Datasets of Type Row. The “Row” type is Spark’s internal representation of its optimized
  //in-memory format for computation. This format makes for highly specialized and efficient
  //computation because rather than using JVM types, which can cause high garbage-collection and
  //object instantiation costs, Spark can operate on its own internal format without incurring any of
  //those costs.

  //Columns
  //Columns represent a simple type like an integer or string, a complex type like an array or map, or
  //a null value. Spark tracks all of this type information for you and offers a variety of ways, with
  //which you can transform columns.

  //Rows
  //A row is nothing more than a record of data. Each record in a DataFrame must be of type Row, as
  //we can see when we collect the following DataFrames. We can create these rows manually from
  //SQL, from Resilient Distributed Datasets (RDDs), from data sources, or manually from scratch.

  // in Scala
  val arrRow = spark.range(10).toDF(colNames = "myNumber").collect() //we created a dataframe with single column
  //collect was required to move the data into our local memory
  arrRow.slice(2,6).foreach(println) //our range started with 0, so we took a slice from 3rd to 6th
  arrRow.slice(0,4).foreach(r => println(r.get(0))) //so this gets us first field of the row
  arrRow.slice(3,9).foreach(r => println(r.getAs("myNumber"))) //so this gets field by name

  import org.apache.spark.sql.types._ //so this gets us access to Spark data types
  val b = ByteType //so b is just a shortcute to ByteType




}
