package com.github.valrcs.spark

//import org.apache.spark.sql.functions._ //this would include all functions from functions but there are too many so not recommended
import org.apache.spark.sql.functions.{bround, coalesce, col, corr, current_date, current_timestamp, date_add, date_sub, datediff, expr, initcap, lit, lower, monotonically_increasing_id, months_between, not, pow, rand, regexp_extract, regexp_replace, round, to_date, to_timestamp, translate, upper}

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

  //Another task might be to replace given characters with other characters. Building this as a
  //regular expression could be tedious, so Spark also provides the translate function to replace these
  //values. This is done at the character level and will replace all instances of a character with the
  //indexed character in the replacement string

  //so simpler and possibly quicker alternative to full regex
  //so each character from matching string is replaced by corresponding string in replaceString
  df.select(translate(col("Description"), "LEET", "1337"), col("Description"))
    .show(5, false)

  //there should be no need for two EEs and two 33s
  spark.sql("SELECT translate(Description, 'LET', '137'), Description FROM dfTable")
    .show(5, false)


  //extracting matches out of strings

  println(regexString) //so simple multiple match regex | is OR
  df.select(
    regexp_extract(col("Description"), regexString, 0).alias("color_clean"),
    col("Description"))
    .show(10, false)

  spark.sql("SELECT regexp_extract(Description, '(BLACK|WHITE|RED|GREEN|BLUE)', 0), Description FROM dfTable")
    .show(10, false)

  //Sometimes, rather than extracting values, we simply want to check for their existence. We can do
  //this with the contains method on each column. This will return a Boolean declaring whether the
  //value you specify is in the column’s string

  // in Scala
  val containsBlack = col("Description").contains("BLACK")
  val containsWhite = col("DESCRIPTION").contains("WHITE")
  df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
    .where("hasSimpleColor") //so only Rows where the hasSimpleColor evaluates to True
    .select("Description").show(10, false)

  //same as above
  spark.sql("SELECT Description FROM dfTable WHERE instr(Description, 'BLACK') >= 1 OR instr(Description, 'WHITE') >= 1")
    .show(10,false)

  //Let’s work through this in a more rigorous way and take advantage of Spark’s ability to accept a
  //dynamic number of arguments. When we convert a list of values into a set of arguments and pass
  //them into a function, we use a language feature called varargs. Using this feature, we can
  //effectively unravel an array of arbitrary length and pass it as arguments to a function. This,
  //coupled with select makes it possible for us to create arbitrary numbers of columns
  //dynamically:

  println(simpleColors.mkString("(", "|", ")"))
  val selectedColumns = simpleColors.map(color => {
    col("Description").contains(color.toUpperCase).alias(s"is_$color")
  }):+expr("*") // could also append this value
  df.select(selectedColumns:_*).where(col("is_white").or(col("is_red")))
    .select("Description").show(5, false)

  //https://stackoverflow.com/questions/32551919/how-to-use-column-isin-with-list
  val exactMatches = Seq("WHITE METAL LANTERN", "HAND WARMER RED POLKA DOT" )
  df.filter(col("Description").isin(exactMatches:_*)) //exactMatches:_* converst Seq to variable number of arguments
    .show(10, false)

  //lets extract rows which contain a digit somewhere in description

  println(regexDigit)
  df.filter(col("Description").rlike(regexDigit))
    .show(10, false)

  //how about extracting rows which have 2 or more digit numbers in the description
  df.filter(col("Description").rlike("\\d{2,}"))
    .show(10, false)

  //this gives us empty rows
  spark.sql("SELECT * FROM dfTable WHERE Description RLIKE '\\d+'")
    .show(10, false)

  //this works
  spark.sql("SELECT * FROM dfTable WHERE Description RLIKE 1")
    .show(10, false)

//  spark.sql("SELECT * FROM dfTable WHERE Description RLIKE \\d+")
//    .show(10, false)
  //TODO find in documentation SQL RLIKE syntax, again we can use rlike with no problems

//As we hinted earlier, working with dates and timestamps closely relates to working with strings
  //because we often store our timestamps or dates as strings and convert them into date types at
  //runtime. This is less common when working with databases and structured data but much more
  //common when we are working with text and CSV files

  //Although Spark will do read dates or times on a best-effort basis. However, sometimes there will
  //be no getting around working with strangely formatted dates and times. The key to
  //understanding the transformations that you are going to need to apply is to ensure that you know
  //exactly what type and format you have at each given step of the way. Another common “gotcha”
  //is that Spark’s TimestampType class supports only second-level precision, which means that if
  //you’re going to be working with milliseconds or microseconds, you’ll need to work around this
  //problem by potentially operating on them as longs. Any more precision when coercing to a
  //TimestampType will be removed

  //Spark can be a bit particular about what format you have at any given point in time. It’s
  //important to be explicit when parsing or converting to ensure that there are no issues in doing so.
  //At the end of the day, Spark is working with Java dates and timestamps and therefore conforms
  //to those standards.

  val dateDF = spark.range(10)
    .withColumn("today", current_date())
    .withColumn("now", current_timestamp())
  dateDF.createOrReplaceTempView("dateTable")

  dateDF.printSchema()
  dateDF.show()

//  Now that we have a simple DataFrame to work with, let’s add and subtract five days from today.
//  These functions take a column and then the number of days to either add or subtract as the
//    arguments:

  dateDF.select(col("today"),
    date_sub(col("today"), 5),
    date_add(col("today"), 5))
    .show(2, false)

  dateDF.select(col("today"),
    date_sub(col("today"), 45),
    date_add(col("today"), 15))
    .show(2, false)

  //Another common task is to take a look at the difference between two dates. We can do this with
  //the datediff function that will return the number of days in between two dates. Most often we
  //just care about the days, and because the number of days varies from month to month, there also
  //exists a function, months_between, that gives you the number of months between two dates:

  dateDF.withColumn("week_ago", date_sub(col("today"), 7))
    .select(datediff(col("week_ago"), col("today")),
      col("week_ago"),
      col("today"))
    .show(1, false)


  dateDF.select(
    to_date(lit("2022-01-01")).alias("start"),
    to_date(lit("2021-06-22")).alias("end"))
    .select(months_between(col("start"), col("end")),
      col("start"),
      col("end"),
      datediff(col("start"), col("end")))
        .show(1, false)

  //Spark will not throw an error if it cannot parse the date; rather, it will just return null. This can
  //be a bit tricky in larger pipelines because you might be expecting your data in one format and
  //getting it in another.

  spark.range(5).withColumn("date", lit("2021-06-22"))
    .select(to_date(col("date"))).show(1)

  //so what happens when we have yyyy-DD-MM format
  //by default we will either get null, or get wrong date, unless of course date and month matches :)
  dateDF.select(to_date(lit("2016-20-12")),to_date(lit("2017-12-11"))).show(1)

  val dateFormat = "yyyy-dd-MM"
  val cleanDateDF = spark.range(1).select(
    to_date(lit("2017-12-11"), dateFormat).alias("date"),
    to_date(lit("2021-22-06"), dateFormat).alias("date2"))
  cleanDateDF.createOrReplaceTempView("dateTable2")
  cleanDateDF.show(false)

  spark.sql("SELECT to_date(date, 'yyyy-dd-MM'), to_date(date2, 'yyyy-dd-MM'), to_date(date) FROM dateTable2")
    .show(truncate=false)

  //an example of to_timestamp, which always requires a format to be specified:

  cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()

  val sillyDateTime = "01 12 2038 20:37:19"
  val sillyFormat = "dd MM yyyy HH:mm:ss"
  val sillyDateDF = spark
    .range(2)
    .withColumn("today", current_date())
    .withColumn("now", current_timestamp())
    .withColumn("future", to_timestamp(lit(sillyDateTime), sillyFormat))

  sillyDateDF.printSchema()
  sillyDateDF.show(2, false)

  //in Spark Working with Nulls in Data
  //As a best practice, you should always use nulls to represent missing or empty data in your
  //DataFrames.

  //WARNING
  //Nulls are a challenging part of all programming, and Spark is no exception. In our opinion, being
  //explicit is always better than being implicit when handling null values. For instance, in this part of the
  //book, we saw how we can define columns as having null types. However, this comes with a catch.
  //When we declare a column as not having a null time, that is not actually enforced. To reiterate, when
  //you define a schema in which all columns are declared to not have null values, Spark will not enforce
  //that and will happily let null values into that column. The nullable signal is simply to help Spark SQL
  //optimize for handling that column. If you have null values in columns that should not have null values,
  //you can get an incorrect result or see strange exceptions that can be difficult to debug

//  Spark includes a function to allow you to select the first non-null value from a set of columns by
//    using the coalesce function.

  df.select(coalesce(col("Description"), col("CustomerId"))).show(false)

  //ifnull, nullIf, nvl, and nvl2
  //There are several other SQL functions that you can use to achieve similar things. ifnull allows
  //you to select the second value if the first is null, and defaults to the first. Alternatively, you could
  //use nullif, which returns null if the two values are equal or else returns the second if they are
  //not. nvl returns the second value if the first is null, but defaults to the first. Finally, nvl2 returns
  //the second value if the first is not null; otherwise, it will return the last specified value
  //(else_value in the following example)

  spark.sql("SELECT ifnull(null, 'return_value'), " +
    "nullif('value', 'value'), nvl(null, 'return_value'), " +
    "nvl2('not_null', 'return_value', 'else_value') " +
    "FROM dfTable LIMIT 1")
    .show(false)

//  The simplest function is drop, which removes rows that contain nulls. The default is to drop any
//    row in which any value is null:
  df.na.drop().show(25, false)

//  We can also apply this to certain sets of columns by passing in an array of columns:
    // in Scala
    df.na.drop("all", Seq("StockCode", "InvoiceNo"))

  //TODO test our drop and fill
}
