package com.github.valrcs.spark

import org.apache.spark.sql.functions.{bround, col, corr, expr, initcap, lit, lower, monotonically_increasing_id, not, pow, rand, regexp_replace, round, upper}

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
  val descriptionFilter = col("Description").contains("POSTAGE")
  df.where(col("StockCode").isin("DOT"))
    .where(priceFilter.or(descriptionFilter)) //so you can keep adding AND filters while OR filters are inside each where
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
//the isExpensive column is a Boolean column with results of particular comparison
  df.withColumn("isExpensive", not(col("UnitPrice").leq(3.39))).show(30, truncate = false)

  df.withColumn("isExpensive", not(col("UnitPrice").leq(250)))
    .filter("isExpensive")
    .select("Description", "UnitPrice").show(5)

  //same as previous using the expression
  df.withColumn("isExpensive", expr("NOT UnitPrice <= 250"))
    .filter("isExpensive")
    .select("Description", "UnitPrice").show(5)

  //WARNING
  //One “gotcha” that can come up is if you’re working with null data when creating Boolean expressions.
  //If there is a null in your data, you’ll need to treat things a bit differently. Here’s how you can ensure
  //that you perform a null-safe equivalence test:
  //df.where(col("Description").eqNullSafe("hello")).show()

  //should be same as previous two
  df.where(col("UnitPrice") > 250).show(5)

  //NUMBERS

  val fabricatedQuantity = pow(col("Quantity") * col("UnitPrice"), 2) + 5 //no calculation will take place here we just define the formula
  df.select(col("CustomerId"),
    col("Quantity"),
    col("UnitPrice"),
    fabricatedQuantity.alias("realQuantity"))
    .show(5)

  // in Scala using SQL expressions no view needed
  df.selectExpr(
    "CustomerId",
    "Quantity",
    "UnitPrice",
    "UnitPrice * 10",
    "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity",
    "ROUND((POWER((Quantity * UnitPrice), 2.0) + 5), 2) as roundQuantity")
    .show(2)

  //You lear of power from this reference of SQL functions available: https://spark.apache.org/docs/latest/api/sql/index.html#power

  //same idea as above two but using full SQL so we need the temporary view dfTable which we created earlier
  spark.sql("SELECT customerId, UnitPrice, Quantity, (POWER((Quantity * UnitPrice), 2.0) + 5) " +
    "as realQuantity FROM dfTable")
    .show(2)

  df.select(round(col("UnitPrice"), 1).alias("rounded"), col("UnitPrice")).show(5)

  //we are using our dataframe to create a new column of values not related in any way to original data
  df.select(round(lit("2.49")),round(lit("2.5")), bround(lit("2.5"))).show(2)

  //Another numerical task is to compute the correlation of two columns. For example, we can see
  //the Pearson correlation coefficient for two columns to see if cheaper things are typically bought
  //in greater quantities. We can do this through a function as well as through the DataFrame
  //statistic methods:

  println("Pearson correlation coefficient ")
  println(df.stat.corr("Quantity", "UnitPrice"))
  df.select(corr("Quantity", "UnitPrice")).show()

  //well looks like a tiny negative correlation but not significant by itself
  spark.sql("SELECT corr(Quantity, UnitPrice) FROM dfTable").show() //same as above two calculations

  //Another common task is to compute summary statistics for a column or set of columns. We can
  //use the describe method to achieve exactly this. This will take all numeric columns and
  //calculate the count, mean, standard deviation, min, and max. You should use this primarily for
  //viewing in the console because the schema might change in the future

  df.describe().show()

  //If you need these exact numbers, you can also perform this as an aggregation yourself by
  //importing the functions and applying them to the columns that you need:
  //// in Scala
  //import org.apache.spark.sql.functions.{count, mean, stddev_pop, min, max}

  // in Scala
  val colName = "UnitPrice"
  val quantileProbabilities = Array(0.25,0.5,0.75, 0.99) //so 0.5 will be median
  val relError = 0.05
  df.stat.approxQuantile("UnitPrice", quantileProbabilities, relError).foreach(println) // 2.51 //with BigData you might not want to do full calculation of quantiles

  //we can add some numbers starting from 0
  // in Scala
  df.select(monotonically_increasing_id(),expr("*")).show(5) //expr("*") is like SELECT * FROM mytable

  //lets add some random numbers as new column
  df.select(monotonically_increasing_id(), rand(), rand(42),expr("*")).show(5)
  //so rand(42) sequence should repeat on 2nd run, while rand() should not
  df.select(monotonically_increasing_id(), rand(), rand(42),expr("*")).show(5)


  //Strings
  //capitalize each word in the column
  df.select(initcap(col("Description"))).show(5, false)
  //same as above with spark sql
  spark.sql("SELECT initcap(Description) FROM dfTable").show(5, false)

  df.select(col("Description"), //original
    initcap(col("Description")), //capitalize first words
    lower(col("Description")), //lowercase
    upper(col("Description"))) //uppercase
    .show(5)

  //same as above skipping initcap
  spark.sql("SELECT Description, initcap(Description), lower(Description), upper(Description) FROM dfTable")
    .show(5, false)

  import org.apache.spark.sql.functions.{lit, ltrim, rtrim, rpad, lpad, trim}
  df.select(
    ltrim(lit(" HELLO ")).as("ltrim"),
    rtrim(lit(" HELLO ")).as("rtrim"),
    trim(lit(" HELLO ")).as("trim"),
    lpad(lit("HELLO"), 3, " ").as("lp3"),
    lpad(lit("HELLO"), 9, "*").as("lp9"), //so we will be adding 4 starts to the left of HELLO
    rpad(lit("HELLO"), 10, " ").as("rp")).show(3)

  //Regular expressions give
  //the user an ability to specify a set of rules to use to either extract values from a string or replace
  //them with some other values.

  val simpleColors = Seq("black", "white", "red", "green", "blue")
  val regexString = simpleColors.map(_.toUpperCase).mkString("|") // the | signifies `OR` in regular expression syntax
  //with the above regex building method we can have a sequence of colors and our regex will be made for us automatically

  //so now we select original column and the one with particular colors replaced by generic COLOR
  df.select(
    regexp_replace(col("Description"), regexString, "COLOR").alias("color_clean"),
    col("Description"))
    .show(10, false)

  val regexMetal = "METAL"
  df.select(
    regexp_replace(col("Description"), regexMetal, "Metallica").alias("MetalColumn"),
    col("Description")
  ).show(15, false)

  val regexDigit = "\\d" //we need to double escape our backspace once for Scala string and once more for regex
  df.select(
    regexp_replace(col("Description"), regexDigit, "Number").alias("MetalColumn"),
    col("Description")
  ).show(15, false)

  val regexP = "P" //so each single letter P to be replaced with something else
  val regexPPlus = "P+" //the idea is to replace one or more capital PPPP with single lowercase p

  df.select(
    regexp_replace(col("Description"), regexP, "p").alias("PColumn"),
    regexp_replace(col("Description"), regexPPlus, "p").alias("PlusColumn"),
    col("Description")
  ).show(15, false)

}
