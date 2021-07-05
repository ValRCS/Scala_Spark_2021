package com.github.valrcs.spark

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

  //TODO load SQL database
}
