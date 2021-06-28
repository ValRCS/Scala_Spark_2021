package com.github.valrcs.spark

object ReadingSimpleCSV extends App {
  println("Reading a simple CSV with some missing values")

  val filePath = "./src/resources/csv/simple-ints.csv"

  val spark = SparkUtil.createSpark("fixingCSV")

  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true") //so Spark tries to figure out the schema if column has 1 2 and 3C the type will be StringType
    .load(filePath)

  df.printSchema()
  df.describe().show(false)
  df.show()


  df.na.drop(how="all", Seq("desc")).show()

  //fill
  //Using the fill function, you can fill one or more columns with a set of values. This can be done
  //by specifying a map—that is a particular value and a set of columns.
  //For example, to fill all null values in columns of type String, you might specify the following:
  df.na.fill("All Null values become this string").show(false)
  //We could do the same for columns of type Integer by using df.na.fill(5:Integer), or for
  //Doubles df.na.fill(5:Double). To specify columns, we just pass in an array of column names
  //like we did in the previous example:

  df.na.fill(77, Seq("int4")).show()
  df.na.fill(77).show() //so without specifying column name we still fill out int4 because it contains intType

  df.na.fill("All Null values become this string")
    .na.fill(88, Seq("int4"))
    .show(false)

  //REPLACE
//  replace
//  In addition to replacing null values like we did with drop and fill, there are more flexible
//    options that you can use with more than just null values. Probably the most common use case is
//  to replace all values in a certain column according to their current value. The only requirement is
//    that this value be the same type as the original value:

  // in Scala
  df.na.replace("int3", Map(600 -> 60000, 900 -> 9000)).show(false)


}
