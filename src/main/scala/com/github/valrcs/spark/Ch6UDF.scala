package com.github.valrcs.spark

import org.apache.spark.sql.functions.{col, udf}

object Ch6UDF extends App {
  println("Creating our own UDFs - user defined functions")

  val spark = SparkUtil.createSpark("ch6")

  // in Scala
  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true") //so Spark tries to figure out the schema if column has 1 2 and 3C the type will be StringType
    .load("./src/resources/retail-data/by-day/2010-12-01.csv")

  df.printSchema()
  df.createOrReplaceTempView("dfTable")

  // in Scala
  val udfExampleDF = spark.range(5).toDF("num")
  udfExampleDF.createOrReplaceTempView("numTable")
  udfExampleDF.show()

  //regular function so far
  def power3(number:Double):Double = number * number * number //remember to keep your UDF functions pure (meaning no side effects)
  println(power3(2.0))

  //Now that we’ve created these functions and tested them, we need to register them with Spark so
  //that we can use them on all of our worker machines. Spark will serialize the function on the
  //driver and transfer it over the network to all executor processes. This happens regardless of
  //language.

  val power3udf = udf(power3(_:Double):Double)

  val cubeDF = udfExampleDF.withColumn("cubed", power3udf(col("num")))



  //However, we can also register this UDF as a
  //Spark SQL function. This is valuable because it makes it simple to use this function within SQL
  //as well as across languages.
  spark.udf.register("power3", power3(_:Double):Double)

  udfExampleDF.selectExpr("power3(num)").show(3)

  spark.sql("SELECT num, power3(num) FROM numTable").show()

  //of course add can already done with col("c1") + col("c2")
  def add(n1:Double, n2:Double):Double = n1+n2
  println(add(3.14, 2.7))

  //so we add our custom function to work with 2 columns
  val addUdf = udf(add(_:Double, _:Double):Double)

  cubeDF.show()
  cubeDF.printSchema()

  val sumDF = cubeDF
    .withColumn("num+cube", addUdf(col("num"), col("cubed")))
    .show()


  spark.udf.register("myAdd", add(_:Double, _:Double):Double)

  cubeDF.selectExpr("myAdd(num, cubed)").show(3)

  cubeDF.createOrReplaceTempView("cubeTable")

  spark.sql("SELECT *, myAdd(num,cubed) FROM cubeTable").show()

  df.show(5, false)

  def multiplyFirstWord(sentence:String, times:Int):String = {
    val first = sentence.split(" ")(0)
    first*times //will return first word copied multiple times
  }

  println(multiplyFirstWord("Quick brown fox jumped over a sleeping dog", 4))

  val stringUdf = udf(multiplyFirstWord(_:String, _:Int):String)
  df.select(stringUdf(col("Description"), col("Quantity")))
    .show(5, false)

  //if we want to use sql syntax then we do need to register it
  spark.udf.register("sillyString", multiplyFirstWord(_:String, _:Int):String)
  df.selectExpr("sillyString(Description, Quantity)")
    .show(5, false)
}
