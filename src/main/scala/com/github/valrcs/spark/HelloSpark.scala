package com.github.valrcs.spark

import org.apache.spark.sql.SparkSession

object HelloSpark extends App {
  println(s"Testing Scala version: ${util.Properties.versionNumberString}")

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  //session is also commonly used instead of spark as a value name
  println(s"Session started on Spark version ${spark.version}")
  val myRange = spark.range(1000).toDF("number")
  // in Scala
  val divisibleBy2 = myRange.where("number % 2 = 0") //similar to plain Scala filter
  val divisibleBy50 = myRange.where("number % 50 = 0")
  println(divisibleBy2.count()) //should be 500
  println(divisibleBy50.count()) //should be 20
  divisibleBy50.show() //should print DataFrame to console //first 20 rows actually in this case we only have 20 !

  val simpleInts = spark
    .read
    .option("inferSchema", "true") //spark will try to figure out what type of data you have
    .option("header", "true") //first row will be considered a header
    .csv("./src/resources/csv/simple-ints.csv")

  simpleInts.show()
}
