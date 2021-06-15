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
    StructField("DEST_COUNTRY_NAME", StringType, true),
    StructField("ORIGIN_COUNTRY_NAME", StringType, true),
    StructField("count", LongType, false,
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
    .option("multiLine", true)
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
  val bigPrice = ((df.col("price")+50)*100)
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
    new StructField("some", StringType, true),
    new StructField("col", StringType, true),
    new StructField("names", LongType, false)))
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
}
