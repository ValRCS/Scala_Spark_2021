package com.github.valrcs.spark

import org.apache.spark.sql.types.{IntegerType, LongType, Metadata, StringType, StructField, StructType}

object Ch9DataSources extends App {
  val spark = SparkUtil.createSpark("ch9DataSources")

  //Basics of Reading Data
  //The foundation for reading data in Spark is the DataFrameReader. We access this through the
  //SparkSession via the read attribute:
  //spark.read
  //After we have a DataFrame reader, we specify several values:
  //The format
  //The schema
  //The read mode
  //A series of options
  //The format, options, and schema each return a DataFrameReader that can undergo further
  //transformations and are all optional, except for one option. Each data source has a specific set of
  //options that determine how the data is read into Spark (we cover these options shortly). At a
  //minimum, you must supply the DataFrameReader a path to from which to read.
  //Here’s an example of the overall layout:
  //spark.read.format("csv")
  //.option("mode", "FAILFAST")
  //.option("inferSchema", "true")
  //.option("path", "path/to/file(s)")
  //.schema(someSchema)
  //.load()
  val filePath = "./src/resources/csv/dirty-data.csv"


  //Spark’s read modes
  //Read mode
  //Description
  //permissive
  //Sets all fields to null when it encounters a corrupted record and places all corrupted records
  //in a string column called _corrupt_record
  //dropMalformed Drops the row that contains malformed records
  //failFast
  //Fails immediately upon encountering malformed records

  val df = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("path", filePath)
//    .option("mode", "FAILFAST") //default is PERMISSIVE
    //if we wanted id to be aninteger we would have had to specify a schema
    .load

  df.printSchema()
  df.show(false)

  //writing

  df.write
    .format("json")
//    .option("mode", "OVERWRITE")
    .mode("overwrite")
    .option("path", "./src/resources/json/jul5") //it will create a folder
    .save

  //Spark’s save modes
  //Save mode
  //Description
  //append
  //Appends the output files to the list of files that already exist at that location
  //overwrite
  //Will completely overwrite any data that already exists there
  //errorIfExists Throws an error and fails the write if data or files already exist at the specified location
  //ignore
  //If data or files exist at the location, do nothing with the current DataFrame

  //cleanup bad rows with null data if we do not show dataframe this should be quite efficient actually
  df.na
    .drop
    .write
    .format("csv")
    .mode("overwrite") //The default is errorIfExists.
    .option("header", "true")
    .option("path", "./src/resources/csv/jul5") //it will create a folder
    .option("sep", "***") //usual separators are /t space , ;
    .save

  //TODO specify Schema if needed
  // in Scala
  //Id,Name,Car,Year
  val myManualSchema = new StructType(Array(
    StructField("Id", IntegerType, true),
    StructField("Name", StringType, true),
    StructField("Car", StringType, true),
    StructField("Year", IntegerType, true),
    ))

  val manualCSV = spark
    .read
    .format("csv")
    .option("header", "true")
//    .option("mode", "FAILFAST")
    .schema(myManualSchema)
    .option("path", filePath)
    .load

  manualCSV.show(false)
  manualCSV.printSchema()
  //so one reason for forcing your own schema on Spark would be so your numeric columns do not turn into strings

  //JSON in SPARK can be loaded from two types of files
  //DEFAULT line-delimited JSON meaning each line is valid JSON (usually an JSON object (a collection key:value pairs
  //alternative is multiline (more common in web development, web data)

  val foodPath = "./src/resources/json/foods.json"
  val properFoodPath = "./src/resources/json/properFoods.json"

  val fromLineDelimited = spark
    .read
    .format("json") //we let spark figure out the schema automatically usually it is okay
    .load(foodPath) //by default it reads each entry from single row of text in json

  fromLineDelimited.show(false)

  val fromMultiLineJSON = spark
    .read
//    .format(source = "json")
    .option("multiLine", value = true) // so we are reading regular JSON here meaning it is spread over multiple lines
    .json(properFoodPath) //so json method simply combines format and load into one specific call
  fromMultiLineJSON.show(false)

  val parquetFilePath = "./src/resources/flight-data/parquet/2010-summary.parquet"

  val fromParquetDF = spark
    .read
    .format("parquet")
    .load(parquetFilePath)

  fromParquetDF.printSchema()
  fromParquetDF.show(5,false)

  manualCSV.write
    .format("parquet")
    .mode("overwrite")
    .save("./src/resources/parquet/jul7")

  val fromManualCSV = spark.read
    .format("parquet")
    .load("./src/resources/parquet/jul7")

  fromManualCSV.printSchema()
  fromManualCSV.show(false)

  //only two options for parquet read and write
//https://spark.apache.org/docs/latest/sql-data-sources-parquet.html
  //can compress even more when writing
  //can add columns when reading

  //ORC files
  //What is the difference between ORC and Parquet?
  //For the most part, they’re quite similar; the fundamental difference is that Parquet is further
  //optimized for use with Spark, whereas ORC is further optimized for Hive.
  //generally Parquet will be a bit faster on Spark specific workloads

  val orcFilepath = "./src/resources/flight-data/orc/2010-summary.orc"

//not needed but we could pass a manual schema when reading
  val myFlightSchema = StructType(Array(
    StructField("DEST_COUNTRY_NAME", StringType, nullable = true),
    StructField("ORIGIN_COUNTRY_NAME", StringType, nullable = true),
    StructField("count", LongType, nullable = false,
      Metadata.fromJson("{\"hello\":\"world\"}"))
  ))

  spark.read
    .format("orc")
    .load(orcFilepath)
    .show(5, false)

  fromManualCSV.write
    .format("orc")
    .mode("overwrite") //since we are going to run this example over and over
    .save("./src/resources/orc/jul7")

  spark.read
    .format("orc")
    .load("./src/resources/orc/jul7")
    .show(false)
}
