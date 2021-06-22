package com.github.valrcs.spark

import org.apache.spark.sql.functions.{col, desc, round}

object ExerciseJ22 extends App {
  println("Let's analyze some data for June 22st - 2011")
  val filePath = "./src/resources/retail-data/by-day/2011-06-22.csv"

  val spark = SparkUtil.createSpark("exj22")

  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true") //so Spark tries to figure out the schema if column has 1 2 and 3C the type will be StringType
    .load(filePath)

  df.describe().show() //some basic stats

  df.show(20, false)

  //TODO extract Rows which contain CAT or DOG
//TODO add column with total purchase price (Quantity * UnitPrice) call the new column TotalPurchase
  //TODO order by this TotalPurchase column
  //TODO show top 20

  val containsCat = col("Description").contains("CAT")
  val containsDog= col("DESCRIPTION").contains("DOG")
  df.withColumn("hasAnimal", containsCat.or(containsDog))
    .where("hasAnimal")
    .withColumn("TotalPurchase", round(col("Quantity") * col("UnitPrice"), 2))
    .select("Description", "Quantity", "UnitPrice", "TotalPurchase")
    .where(col("Quantity") > 0) //exclude neg quantity
    .orderBy(col("TotalPurchase").desc)
    .show(20, false)


  df.withColumn("CatOrDog", containsCat.or(containsDog))
    .withColumn("TotalPurchase", round(col("Quantity") * col("UnitPrice")*100/100))
    .where("CatOrDog")
    .orderBy(desc("TotalPurchase"))
    .show(20, false)
}
