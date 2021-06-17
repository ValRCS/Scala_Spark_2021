package com.github.valrcs.spark

// in Scala
import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
import org.apache.spark.sql.types.Metadata

object BasicStructuredOperations extends App {
  println("Chapter 5. Basic Structured Operations")
  val spark = SparkUtil.createSpark("basicOps")
  //our json files should consist of multiple rows of JSON objects {}
  val flightPath = "./src/resources/flight-data/json/2015-summary.json"
  val foodPath = "./src/resources/json/foods.json"
  val properFoodPath = "./src/resources/json/properFoods.json"


  // in Scala
  val df = spark.read.format("json") //we let spark figure out the schema automatically usually it is okay
    .load(foodPath) //by default it reads each entry from single row of text in json

  df.show()

  //we can get the schema out of any dataframe
  println(df.schema)

  //WARNING
  //Deciding whether you need to define a schema prior to reading in your data depends on your use case.
  //For ad hoc analysis, schema-on-read usually works just fine (although at times it can be a bit slow with
  //plain-text file formats like CSV or JSON). However, this can also lead to precision issues like a long
  //type incorrectly set as an integer when reading in a file. When using Spark for production Extract,
  //Transform, and Load (ETL), it is often a good idea to define your schemas manually, especially when
  //working with untyped data sources like CSV and JSON because schema inference can vary depending
  //on the type of data that you read in.

  val myManualSchema = StructType(Array(
    StructField("DEST_COUNTRY_NAME", StringType, nullable = true),
    StructField("ORIGIN_COUNTRY_NAME", StringType, nullable = true),
    StructField("count", LongType, nullable = false,
      Metadata.fromJson("{\"hello\":\"world\"}"))
  ))

  val fdf = spark
    .read
    .format("json")
    .schema(myManualSchema) //so specify how the data should be formatted the data types
    .load(flightPath)

  fdf.show()
  println(fdf.schema)

  //if you need normal JSON say an array of objects then you want multiline permissive mode
  val properDf = spark
    .read
    .format(source = "json")
    .option("multiLine", value = true)
    .option("mode", "PERMISSIVE")
    .json(properFoodPath)
  
  properDf.show()

  println(properDf.schema)

  df
//    .coalesce(2) //if you want a guaranteed single file you want coalesce(1)
    //first we need to fall back to single partition
    .write
    .option("header","true")
    .option("sep",",")
    .mode("overwrite") //meaning we will overwrite old data with same file name
    .csv("./src/resources/csv/foods.csv")

  //Columns
  //To Spark, columns are logical constructions that simply represent a value computed on a per-
  //record basis by means of an expression. This means that to have a real value for a column, we
  //need to have a row; and to have a row, we need to have a DataFrame. You cannot manipulate an
  //individual column outside the context of a DataFrame; you must use Spark transformations
  //within a DataFrame to modify the contents of a column.

  import org.apache.spark.sql.functions.{col, column}
  import spark.implicits._
  df.select($"price",$"name").show()
  df.select("price", "name","model").show()
  df.select($"price"+50,$"name").show() //we can do math using implicit $ for columns
  val bigPrice = (df.col("price")+50)*100
  val ndf = df.withColumn("bigPrice",bigPrice) //added new column bigPrice
  //we compare two columns and result is asigned to new column
  val priceVsQuantity = (df.col("price") > df.col("quantityKg"))
  //we add this new column to ndf (but do not save it yet)
  ndf.withColumn("priceVsQuantity", priceVsQuantity).show()

  //lets do the above with SQL
  //first we need a view
  df.createOrReplaceTempView("foodies")
  spark.sql("SELECT * FROM foodies").show()

  //to create new columns with SQL we simply select the new column
  val sdf = spark.sql("SELECT *, (price+50)*100 as bigPrice FROM foodies")
  sdf.show()

  sdf.createOrReplaceTempView("sdfView")
  val cdf = spark.sql("SELECT *, price > quantityKg as priceVsQnty FROM sdfView")
  cdf.show()
  //in fact for creating multiple columns at once it is recommended to use SQL select syntax
  //https://sparkbyexamples.com/spark/spark-dataframe-withcolumn#add-replace-update-multiple-columns

  //use mydf.show() to show end results
  //remember you need to create a temporary view for properDF first!!

  properDf.createOrReplaceTempView("pdfView")
  val myDf = spark.sql("SELECT *, ROUND((price * 1.1),2) as bigPrice FROM pdfView")
  myDf.show()

  myDf.createOrReplaceTempView("newPdfView")
  val myNewDf = spark.sql("SELECT * FROM newPdfView WHERE price < 1.00")
  myNewDf.show()
  //if I just want to see results I do not need to save into Dataframe
  spark.sql("SELECT * FROM newPdfView WHERE bigPrice < 1.00").show()

  //we can also get our column names as Array of Strings
  println(myNewDf.columns.mkString("Array(", ", ", ")")) //could use foreach as well

  //first row
  val firstRow = myNewDf.first() //head also should work
  println(firstRow)
  println(firstRow.mkString("Row(", ", ", ")"))
  println(firstRow.get(2)) //should give us potato
  println(myNewDf.head.get(2)) //should also be a potato

  //Creating Rows
  //You can create rows by manually instantiating a Row object with the values that belong in each
  //column. It’s important to note that only DataFrames have schemas. Rows themselves do not have
  //schemas. This means that if you create a Row manually, you must specify the values in the same
  //order as the schema of the DataFrame to which they might be appended

  // in Scala
  import org.apache.spark.sql.Row
  //we are creating a standalone Row with no relation to anything just yet
  val myRow = Row(300000, "XC90", "Volvo",
    10000, 2000, Seq("Centrs","Jurmala"),
    true, 12500)
  // in Scala
  println(myRow(0))// type Any
  println(myRow(0).toString) //this casts Any to Scala String type, will work with pretty much anything
//  println(myRow(0).asInstanceOf[String]) // String coerciion out of Int will throw an int
//  println(myRow.getString(0) )// String also does not work on non STrings
  println(myRow.getInt(3)) // Int

  //TODO println(myRow.json) //myRow does not know about the column names //we need schema for this


  // in Scala
  //it is possible to create a DataFrame out of sequence of Rows and manual Schema
  import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
  val myManualSchema2 = new StructType(Array(
    StructField("some", StringType, true),
    StructField("col", StringType, true),
    StructField("names", LongType, false)))
  val myRows = Seq(Row("Hello", null, 1L), Row("There", "something", 20L))
  val myRDD = spark.sparkContext.parallelize(myRows)
  val myDfFromRows = spark.createDataFrame(myRDD, myManualSchema2) //funnily enough it works also on myManualSchema which is similar
  myDfFromRows.show()

//  select and selectExpr
//  select and selectExpr allow you to do the DataFrame equivalent of SQL queries on a table of
//    data:

  //in SQL SELECT DEST_COUNTRY_NAME FROM dfTable LIMIT 2
  fdf.select("DEST_COUNTRY_NAME").show(2)

  // in Scala //show is just for printing (returns nothing Unit)
  //-- in SQL
  //SELECT DEST_COUNTRY_NAME, ORIGIN_COUNTRY_NAME FROM dfTable LIMIT 2
  fdf.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)

  // you can refer to columns in a number of different
  //ways; all you need to keep in mind is that you can use them interchangeably
  // in Scala NOT good in practice to use different methods
  //stick with one + sql one
  import org.apache.spark.sql.functions.{expr, col, column}
  fdf.select(
    fdf.col("DEST_COUNTRY_NAME"),
    col("DEST_COUNTRY_NAME"),
    column("DEST_COUNTRY_NAME"),
    'DEST_COUNTRY_NAME, //shortest one, but speed wise they are all the same
    $"DEST_COUNTRY_NAME",
    expr("DEST_COUNTRY_NAME")) //we have a new Dataframe of 6 columns
    .show(2)

  //As we’ve seen thus far, expr is the most flexible reference that we can use. It can refer to a plain
  //column or a string manipulation of a column. To illustrate, let’s change the column name, and
  //then change it back by using the AS keyword and then the alias method on the column:
  //// in Scala
  fdf.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)

//  This changes the column name to “destination.” You can further manipulate the result of your
//    expression as another expression:
    // in Scala
  //here destination is overwritten immediately with a new alias
    fdf.select(expr("DEST_COUNTRY_NAME as destination").alias("AGAIN_DEST_COUNTRY_NAME"))
    .show(2)
  fdf.select(expr("DEST_COUNTRY_NAME").alias("NEW_DEST_COUNTRY_NAME"))
    .show(2)

//  Because select followed by a series of expr is such a common pattern, Spark has a shorthand
//  for doing this efficiently: selectExpr. This is probably the most convenient interface for
//    everyday use:
  // in Scala
  fdf.selectExpr("DEST_COUNTRY_NAME as newColumnName", "DEST_COUNTRY_NAME").show(2)

  //We can treat selectExpr as a simple way to build up
  //complex expressions that create new DataFrames. In fact, we can add any valid NON-AGGREGATING
  //SQL statement, and as long as the columns resolve, it will be valid! Here’s a simple example that
  //adds a new column withinCountry to our DataFrame that specifies whether the destination and
  //origin are the same:

  // in Scala
  fdf.selectExpr(
    "*", // include all original columns
    "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry") //this will be a boolean column
    .show(10)

  // in Scala
  //With select expression, we can also specify aggregations over the entire DataFrame
  fdf.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)
  //-- in SQL
  //SELECT avg(count), count(distinct(DEST_COUNTRY_NAME)) FROM dfTable LIMIT 2
  //TODO produce purse SQL version of the above line (we already have most of the syntax place just need a view
  fdf.createOrReplaceTempView("flightView")
  spark.sql("SELECT avg(count), count(distinct(DEST_COUNTRY_NAME)) FROM flightView LIMIT 2").show() //2 is not needed because of LIMIT 2

  // in Scala
  import org.apache.spark.sql.functions.lit
  fdf.select(expr("*"), lit(1).as("One")).show(3)

  spark.sql("SELECT *, 1 as One, current_timestamp() as TimeStamp FROM flightView LIMIT 4").show(15, false)
  //truncate = false will show all column contents for the rows we are working on

  // in Scala
  fdf.withColumn("numberFortyTwo", lit(42)).show(6)

  // in Scala
  fdf.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))
    .show(12)

  //creating new column based on existing column
  fdf.withColumn("Destination", expr("DEST_COUNTRY_NAME")).show(3)

  //a bit more correct would be to use the premade method for renaming columns
  //we can chain them like most things in Spark
  // in Scala
  fdf
    .withColumnRenamed("DEST_COUNTRY_NAME", "dest")
    .withColumnRenamed("count", "NumFlights")
    .show(3)

  //same as above but with sql
  spark.sql("SELECT DEST_COUNTRY_NAME as dest, ORIGIN_COUNTRY_NAME, count as NumFlights FROM flightView").show(3)

  // in Scala
  import org.apache.spark.sql.functions.expr
  val dfWithLongColName = fdf.withColumn(
    "This Long Column-Name",
    expr("ORIGIN_COUNTRY_NAME")) //here our column name is with underscores so no need for backticks

  //We don’t need escape characters here because the first argument to withColumn is just a string
  //for the new column name. In NEXT example, however, we need to use backticks because we’re
  //referencing a column in an expression

  // in Scala
  dfWithLongColName.selectExpr(
    "`This Long Column-Name`",
    "`This Long Column-Name` as `new country origin`") //we need the backticks because we are referencing columns in an expression
    .show(2)

  spark.sql("SELECT dest_COUNTRY_NAME from flightView").show(2)
  //if we want to set sql to be case sensitive we can do so
  spark.conf.set("spark.sql.caseSensitive", value = true)
  //turns even this call fails when caseSensitive is true
  spark.conf.set("spark.sql.caseSensitive", value = false) //so I will turn case sensitive off again
  spark.sql("SELECT DEST_COUNTRY_NAME from flightView").show(2)
  //next line should not work because of the setting change
  spark.sql("SELECT dest_COUNTRY_NAME from flightView").show(2)

  //Removing Columns
  //Now that we’ve created this column, let’s take a look at how we can remove columns from
  //DataFrames. You likely already noticed that we can do this by using select. However, there is
  //also a dedicated method called drop:

  dfWithLongColName.drop("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").show(3)

  println(fdf.schema)

  val dfInt = fdf.withColumn("count2", col("count").cast("long"))
  println(dfInt.schema)
  dfInt.show(2)
  //same with sql
  spark.sql("SELECT *, cast(count as long) AS count2 FROM flightView").show(2)

  fdf.filter(col("count") > 1000).show(5) //first approach will catch more errors earlier
  fdf.where("count > 1000").show(5)
  spark.sql("SELECT * FROM flightView WHERE count > 1000").show(5)

  //Instinctually, you might want to put multiple filters into the same expression. Although this is
  //possible, it is not always useful, because Spark automatically performs all filtering operations at
  //the same time regardless of the filter ordering. This means that if you want to specify multiple
  //AND filters, just chain them sequentially and let Spark handle the rest:

  // in Scala we chain
  fdf
    .where(col("count") < 2)
    .where(col("ORIGIN_COUNTRY_NAME") =!= "Croatia")
    .show(2)

  //in sql we just write our  multiple conditionals
  spark.sql("SELECT * FROM flightView WHERE count < 2 AND ORIGIN_COUNTRY_NAME != 'Croatia' LIMIT 2").show()

  // in Scala
  println(fdf.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count())
  //SQL
  spark.sql("SELECT COUNT(DISTINCT(ORIGIN_COUNTRY_NAME, DEST_COUNTRY_NAME)) FROM flightView").show() //we can show a single scalar as well

  //// in Scala
  println(fdf.select("ORIGIN_COUNTRY_NAME").distinct().count())
  spark.sql("SELECT COUNT(DISTINCT ORIGIN_COUNTRY_NAME) FROM flightView").show(1)

  //Random Samples
  //Sometimes, you might just want to sample some random records from your DataFrame. You can
  //do this by using the sample method on a DataFrame, which makes it possible for you to specify
  //a fraction of rows to extract from a DataFrame and whether you’d like to sample with or without
  //replacement:

  val seed = 5 //specific seed value guarantees specific pseudo-random values /without seed you will have different values each time
  val withReplacement = false
  val fraction = 0.2 //20%
  fdf.sample(withReplacement, fraction, seed).show()
  println(fdf.sample(withReplacement, fraction, seed).count()) //TODO why 34 not 25 or 26?

  //Random Splits
  //Random splits can be helpful when you need to break up your DataFrame into a random “splits”
  //of the original DataFrame. This is often used with machine learning algorithms to create training,
  //validation, and test sets. In this next example, we’ll split our DataFrame into two different
  //DataFrames by setting the weights by which we will split the DataFrame (these are the
  //arguments to the function). Because this method is designed to be randomized, we will also
  //specify a seed (just replace seed with a number of your choosing in the code block). It’s
  //important to note that if you don’t specify a proportion for each DataFrame that adds up to one,
  //they will be normalized so that they do:

  // in Scala
  val dataFrames = fdf.randomSplit(Array(0.25, 0.75), seed)
  //weights – weights for splits, will be normalized if they don't sum to 1.
  //seed – Seed for sampling.
  dataFrames.foreach(df => println(df.count()))

  val dataFrames2 = fdf.randomSplit(Array(3, 7), seed) //so one side will have 3/10 of rows another 7/10 of rows randomly selected with seed
  dataFrames2.foreach(df => println(df.count()))

  //Concatenating and Appending Rows (Union)
  //As you learned in the previous section, DataFrames are immutable. This means users cannot
  //append to DataFrames because that would be changing it. To append to a DataFrame, you must
  //union the original DataFrame along with the new DataFrame. This just concatenates the two
  //DataFramess. To union two DataFrames, you must be sure that they have the same schema and
  //number of columns; otherwise, the union will fail.

  val schema = fdf.schema //this is crucial that we have the right schema for our union
  val newRows = Seq(
    Row("New Country", "Other Country", 5L),
    Row("New Country 2", "Other Country 3", 1L)
  )
  val parallelizedRows = spark.sparkContext.parallelize(newRows) //we create a lower level RDD of our Rows
  val newDF = spark.createDataFrame(parallelizedRows, schema)
  fdf.union(newDF)
    .where("count = 1")
    .where($"ORIGIN_COUNTRY_NAME" =!= "United States")
    .show() // get all of them and we'll see our new rows at the end

  //it does matter whether we do union before or after filters
  fdf
    .where(col("ORIGIN_COUNTRY_NAME") === "Estonia")
    .union(newDF) //here we should only have 3 rows the Estonia one and the 2 new ones
    .show()

  //filtering with OR
  //https://sparkbyexamples.com/spark/spark-dataframe-where-filter/
  //multiple condition
  fdf.filter(fdf("ORIGIN_COUNTRY_NAME") === "Estonia"
    || fdf("DEST_COUNTRY_NAME") === "Estonia"
    || col("ORIGIN_COUNTRY_NAME") === "Lithuania" //so we can use col and also the actual dataframe
  ) // || indicates OR
    .show(false)

  //
  // in Scala
  fdf.sort("count").show(5) //by default Ascending
  fdf.sort("count", "DEST_COUNTRY_NAME").show(5) //we have a 2nd tiebreak column here, both ascending
  fdf.orderBy("count", "DEST_COUNTRY_NAME").show(5) //we have a 2nd tiebreak column here, both ascending
  fdf.orderBy(col("count"), col("DEST_COUNTRY_NAME")).show(5) //again two columns with 2nd being the tiebreak

  //To more explicitly specify sort direction, you need to use the asc and desc functions if operating
  //on a column. These allow you to specify the order in which a given column should be sorted:
  //// in Scala
  import org.apache.spark.sql.functions.{desc, asc}
  fdf.orderBy(expr("count desc")).show(5)
  fdf.orderBy(desc("count"), asc("DEST_COUNTRY_NAME")).show(5) //there will not be many tiebreaks
  //until we get to the end
  fdf.orderBy(asc("count"), desc("DEST_COUNTRY_NAME")).show(5) //so 1 count flights but starting from Z country


  //so how about doing the same with Spark SQL?
  //-- in SQL
  //SELECT * FROM dfTable ORDER BY count DESC, DEST_COUNTRY_NAME ASC LIMIT 2
  spark.sql("SELECT * FROM flightView" +
    " ORDER BY count DESC" +
    ", DEST_COUNTRY_NAME ASC " +
    "LIMIT 5")
    .show() //limit is already 5 so it will not show 20

  fdf.orderBy(asc("count") //so 1 count
    , desc("DEST_COUNTRY_NAME")  //2nd tiebreak from Z down country
    , asc("ORIGIN_COUNTRY_NAME") //3rd tiebreak starting from A and up (of course if country started with 0 it would be first)
  ).show(5)
  //same as above just in Spark SQL
  spark.sql("SELECT * FROM flightView" + //we already created flightView much earlier
    " ORDER BY count ASC" +
    ", DEST_COUNTRY_NAME DESC " +
    ", ORIGIN_COUNTRY_NAME ASC " +
    "LIMIT 5")
    .show() //limit is already 5 so it will not show 20

  //An advanced tip is to use asc_nulls_first, desc_nulls_first, asc_nulls_last, or
  //desc_nulls_last to specify where you would like your null values to appear in an ordered
  //DataFrame.
  //you'd have to import them just like we imported asc and desc

  //For optimization purposes, it’s sometimes advisable to sort within each partition before another
  //set of transformations. You can use the sortWithinPartitions method to do this:
  // in Scala
  spark.read.format("json").load("./src/resources/flight-data/json/*-summary.json") //will load all summary files
    .sortWithinPartitions("count")
    .show(10)

  spark.read.format("json").load("./src/resources/flight-data/json/*-summary.json") //will load all summary files
    .sortWithinPartitions(desc("count"))
    .show(10)
  //TODO how to extract the year from the file name when reading multiple files

  //all 3 are the same below
  fdf.limit(10).show() //limit should be more efficient when working with larger datasets
  fdf.show(10) //this might be slower if we are just actually asking for a lot of data firsthand
  spark.sql("SELECT * FROM flightView LIMIT 10").show()

  //TESTING whether offset exists in Spark SQL //so there is no OFFSET as of 2021
  //spark.sql("SELECT * FROM flightView OFFSET 5 LIMIT 10 ").show()

  //what do we do when we want to offest we could take more than we need and then drop
//  fdf.limit(10+5).take(15).slice(6,15)
fdf.head(15).slice(5,15).foreach(println) //this is not very good if I have to skip say 1000 or 100k rows or more, because array[T] will be read into memory
  //TODO how to skip more rows without going into local memory


  println(fdf.rdd.getNumPartitions) // should be 5 because we set it by default in our Utilities object
  //this not going to do anything on a single machine
  // in Scala
  fdf.repartition(15)
  println(fdf.rdd.getNumPartitions) //so nothing happens on a single machine still 1

  //If you know that you’re going to be filtering by a certain column often, it can be worth
  //repartitioning based on that column:
  //// in Scala
  //df.repartition(col("DEST_COUNTRY_NAME"))

  //You can optionally specify the number of partitions you would like, too:
  //// in Scala
  //df.repartition(5, col("DEST_COUNTRY_NAME"))

  //Coalesce, on the other hand, will not incur a full shuffle and will try to combine partitions. This
  //operation will shuffle your data into five partitions based on the destination country name, and
  //then coalesce them (without a full shuffle):
  //// in Scala
  //df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)

  //Collecting Rows to the Driver
  //we used several different methods
  //for doing so that are effectively all the same. collect gets all data from the entire DataFrame,
  //take selects the first N rows, and show prints out a number of rows nicely.

  // in Scala
  val collectDF = fdf.limit(10)
  val flightArray = collectDF.take(5) // take works with an Integer count, gives us Array of Rows
  collectDF.show() // this prints it out nicely
  collectDF.show(5, truncate = false) //prints full columns in cases they are very wide
  val collectedData = collectDF.collect() //Returns an array that contains all rows in this Dataset.
  // Running collect requires moving all the data into the application's driver process,
  // and doing so on a very large dataset can crash the driver process with OutOfMemoryError.

  //so avoid going into local memory as much as possible when working with larger datasets
  //of course if it is couple of million rows no problem

//  There’s an additional way of collecting rows to the driver in order to iterate over the entire
//  dataset. The method toLocalIterator collects partitions to the driver as an iterator. This
//  method allows you to iterate over the entire dataset partition-by-partition in a serial manner:
//    collectDF.toLocalIterator()

}
