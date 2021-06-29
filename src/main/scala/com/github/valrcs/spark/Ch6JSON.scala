package com.github.valrcs.spark

import org.apache.spark.sql.functions.{col, desc, expr, from_json, get_json_object, json_tuple, split, to_json}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object Ch6JSON extends App {
  println("Reading some JSON data inside our dataframes")

  val spark = SparkUtil.createSpark("ch6")

  // in Scala
  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true") //so Spark tries to figure out the schema if column has 1 2 and 3C the type will be StringType
    .load("./src/resources/retail-data/by-day/2010-12-01.csv")

  df.printSchema()
  df.createOrReplaceTempView("dfTable")

//  Spark has some unique support for working with JSON data. You can operate directly on strings
//    of JSON in Spark and parse from JSON or extract JSON objects. Let’s begin by creating a JSON
//    column:

  // in Scala
  val jsonDF = spark.range(1).selectExpr("""
'{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString""")
    jsonDF.printSchema()
    jsonDF.show(false)

  //You can use the get_json_object to inline query a JSON object, be it a dictionary or array.
  //You can use json_tuple if this object has only one level of nesting

  // in Scala
  jsonDF.select(
    get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]") as "column",
    json_tuple(col("jsonString"), "myJSONKey"))
    .show(2, false)

  jsonDF.select(
    get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]") as "column",
    json_tuple(col("jsonString"), "myJSONKey"))
    .printSchema()

  //Here’s the equivalent in SQL: //FIXME does not work!
  jsonDF.selectExpr(
  "json_tuple(jsonString, '$.myJSONKey.myJSONValue[1]') as column")
    .show(false)

  df.selectExpr("(InvoiceNo, Description) as myStruct")
    .select(to_json(col("myStruct")), col("myStruct"))
    .show(5, false)

  df
    .withColumn("myStruct", expr("(InvoiceNo, Description)"))
    .withColumn("convertedToJSON", to_json(col("myStruct")))
    .show(5,false)

  df
    .withColumn("myStruct", expr("(InvoiceNo, Description)"))
    .withColumn("convertedToJSON", to_json(col("myStruct"))) //this is JSON objet(Map type in scala)
    .withColumn("descWords",split(col("Description"), " "))
    .withColumn("splitJSON", to_json(col("descWords"))) //so this should be a JSON array
    .show(5,false)

  //You can use the from_json function to parse this (or other JSON data) back in. This
  //naturally requires you to specify a schema, and optionally you can specify a map of options, as
  //well:
  val parseSchema = new StructType(Array(
    new StructField("InvoiceNo",StringType,true),
    new StructField("Description",StringType,true)))

  df.selectExpr("(InvoiceNo, Description) as myStruct")
    .select(to_json(col("myStruct")).alias("newJSON"), col("myStruct"))
    .select(from_json(col("newJSON"), parseSchema), col("myStruct"), col("newJSON"))
    .show(2, false)

  df.selectExpr("(InvoiceNo, Description) as myStruct")
    .select(to_json(col("myStruct")).alias("newJSON"), col("myStruct"))
    .select(from_json(col("newJSON"), parseSchema), col("myStruct"), col("newJSON"))
    .printSchema()

  df.withColumn("combinedCols", expr("(Quantity, UnitPrice, InvoiceNo, Description)"))
    .show(5, false)

  df.withColumn("combinedCols", expr("(Quantity, UnitPrice, InvoiceNo, Description)"))
    .printSchema()

  df.withColumn("combinedCols", expr("(Quantity, UnitPrice, InvoiceNo, Description)"))
    .withColumn("combJSON", to_json(col("combinedCols")))
    .show(5, false)

  val parseSchema4 = new StructType(Array(
    new StructField("Quantity", IntegerType, true),
    new StructField("UnitPrice", DoubleType, true),
    new StructField("InvoiceNo",StringType,true),
    new StructField("Description",StringType,true)))

  df.withColumn("combinedCols", expr("(Quantity, UnitPrice, InvoiceNo, Description)"))
    .withColumn("combJSON", to_json(col("combinedCols")))
    .withColumn("combinedColsFromJSON", from_json(col("combJSON"), parseSchema4))
    .show(5, false)

  df.select(expr("(InvoiceDate, Quantity, UnitPrice, InvoiceNo, Description) as combinedCols"))
    .withColumn("combJSON", to_json(col("combinedCols")))
    .show(5,false)

  //lets filter only expensive purchases and save them in multiline JSON
  df.filter(col("UnitPrice") > 20)
    .sort(desc("UnitPrice"))
    .show(10, false)

  df.filter(col("UnitPrice") > 20)
    .sort(desc("UnitPrice"))
    .select(expr("(InvoiceDate, Quantity, UnitPrice, InvoiceNo, Description) as combinedCols"))
    .select(to_json(col("combinedCols"))) //we will only have a single column our json data
    .show(10, false)

  //TODO save in CSV or JSON

  val combinedDF =   df.filter(col("UnitPrice") > 20)
    .sort(desc("UnitPrice"))
    .select(expr("(InvoiceDate, Quantity, UnitPrice, InvoiceNo, Description) as combinedCols"))
    .select(to_json(col("combinedCols")))

  combinedDF.show(10, false)

  combinedDF.write
    .format("csv")
    .option("path","./src/resources/j29")
    .save()

  combinedDF.coalesce(1) //for larger data sets without coalesce Spark will divide your data in partitions
    .write
    .format("csv")
    .option("path","./src/resources/csv/j29")
    .save()

  combinedDF.coalesce(1)
    .write
    .format("json")
    .option("path","./src/resources/json/j29")
    .save()

}
