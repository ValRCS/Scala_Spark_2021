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

  df.coalesce(1)
    //first we need to fall back to single partition
    .write
    .option("header","true")
    .option("sep",",")
    .mode("overwrite") //meaning we will overwrite old data with same file name
    .csv("./src/resources/csv/foods.csv")
}
