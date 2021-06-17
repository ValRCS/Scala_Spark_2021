package com.github.valrcs.spark

import org.apache.spark.sql.functions.{col, desc, round}

object ExerciseJ17 extends App {
  val spark = SparkUtil.createSpark("ExerciseJ17")

  //TODO Read all retail CSV src/resources/retail-data/all/online-retail-dataset.csv

  //TODO show top 10 biggest quantities bought (price can be used as 2nd tiebreak also desc)
  val retailData = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("./src/resources/retail-data/all/online-retail-dataset.csv")

  println(retailData.schema) //our schema / data types for columns
  retailData.describe().show() //some stats about our data in a format of another dataframe

  retailData.orderBy(desc("Quantity"),desc("UnitPrice")).show(10)

  //BONUS challenge
  //TODO create total column which is price * quantity (can use SPARK SQL for this)
  //TODO show top 10 purchases

  retailData.createOrReplaceTempView("retailView")
  spark.sql("SELECT *, ROUND((Quantity * UnitPrice),2) as Total FROM retailView ORDER BY Total DESC").show(10)

  //how about making the above sql with Spark methods /functions
  retailData
    .withColumn("Total", round(col("UnitPrice") * col("Quantity")*100)/100)
    //we want 2 decimal places in the above so we rounded the 100x value and after rounding we got 2 decimal places by dividing
    .orderBy(desc("Total"))
    .show(10)

  //no need to for 100/100 trick anymore
  //https://stackoverflow.com/questions/53927574/how-to-round-decimal-in-scala-spark
  retailData
    .withColumn("Total", round(col("UnitPrice") * col("Quantity"), 2)) //so there is a simple solution now
    //we want 2 decimal places in the above so we rounded the 100x value and after rounding we got 2 decimal places by dividing
    .orderBy(desc("Total"))
    .show(10)

}
