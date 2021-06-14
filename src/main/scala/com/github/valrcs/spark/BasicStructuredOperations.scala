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

  //TODO exercise
  //TODO create a new Dataframe which has ALL columns from properDf  and also newPrice which is 10% higher than existing price
  //TODO create a Dataframe of all columns above Dataframe with only rows where price is less than 1.00
  //use mydf.show() to show end results
  //remember you need to create a temporary view for properDF first!!
}
