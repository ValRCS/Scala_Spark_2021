package com.github.valrcs.spark

import org.apache.spark.sql.SparkSession

object HelloSpark extends App {
  println(s"Testing Scala version: ${util.Properties.versionNumberString}")

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  //session is also commonly used instead of spark as a value name
  println(s"Session started on Spark version ${spark.version}")


}
