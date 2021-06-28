package com.github.valrcs.spark

import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{array_contains, col, explode, split, struct}

object Ch6ComplexTypes extends App {
  val spark = SparkUtil.createSpark("ch6")

  // in Scala
  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true") //so Spark tries to figure out the schema if column has 1 2 and 3C the type will be StringType
    .load("./src/resources/retail-data/by-day/2010-12-01.csv")

  df.printSchema()
  df.createOrReplaceTempView("dfTable")

  //Structs
  //You can think of structs as DataFrames within DataFrames. A worked example will illustrate
  //this more clearly. We can create a struct by wrapping a set of columns in parenthesis in a query:
  df.selectExpr("(Description, InvoiceNo) as complex", "*").show(5, false)
  df.selectExpr("struct(Description, InvoiceNo) as complex", "*").show(5, false)

//  We now have a DataFrame with a column complex. We can query it just as we might another
//  DataFrame, the only difference is that we use a dot syntax to do so, or the column method
//    getField:

  val complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
  complexDF.createOrReplaceTempView("complexDF")
  complexDF.select("complex.Description").show(3)
  complexDF.select(col("complex").getField("Description")).show(3)

  //We can also query all values in the struct by using *. This brings up all the columns to the top-
  //level DataFrame:
  complexDF.select("complex.*").show(3,false)
  spark.sql("SELECT complex.* FROM complexDF").show(3,false)

//  Arrays
//  To define arrays, let’s work through a use case. With our current data, our objective is to take
//  every single word in our Description column and convert that into a row in our DataFrame.
//  The first task is to turn our Description column into a complex type, an array.
  //so we are splitting our description by whitespace
  df.select(split(col("Description"), " ")).show(3, false)

  df.withColumn("splitDesc",split(col("Description"), " "))
    .show(5,false)

  //This is quite powerful because Spark allows us to manipulate this complex type as another
  //column. We can also query the values of the array using Python-like syntax:
  // in Scala
  df.select(split(col("Description"), " ").alias("array_col"))
    .selectExpr("array_col[1]").show(5) //so 2nd word of every split

  df
    .withColumn("splitDesc",split(col("Description"), " "))
    .withColumn("wordCount", functions.size(col("splitDesc"))) //size is also used by Scala so we have to add a functions prefix
    .show(5,false)

  df
    .withColumn("splitDesc",split(col("Description"), " "))
    .withColumn("wordCount", functions.size(col("splitDesc"))) //size is also used by Scala so we have to add a functions prefix
    .withColumn("has White", array_contains(col("splitDesc"), "WHITE"))
    .show(5,false)


  //explode
  //The explode function takes a column that consists of arrays and creates one row (with the rest of
  //the values duplicated) per value in the array.

  df.withColumn("splitted", split(col("Description"), " "))
    .withColumn("exploded", explode(col("splitted")))
//    .select("Description", "InvoiceNo", "exploded") //lets show all
    .show(25, false)

  spark.sql("-- in SQL\nSELECT Description, InvoiceNo, exploded\n" +
    "FROM (SELECT *, split(Description, \" \") as splitted " +
    "FROM dfTable)\n" +
    "LATERAL VIEW explode(splitted) as exploded")
    .show(5, false)

  //Maps
  //Maps are created by using the map function and key-value pairs of columns. You then can select
  //them just like you might select from an array:

  df.select(functions.map(col("Description"), col("InvoiceNo"))
    .alias("complex_map"))
    .show(5, false) //again map needs prefix to avoid name collission

  //You can query them by using the proper key. A missing key returns null

  // in Scala
  df.select(functions.map(col("Description"), col("InvoiceNo")).alias("complex_map"))
    .selectExpr("complex_map['WHITE METAL LANTERN']").show(5)
}
