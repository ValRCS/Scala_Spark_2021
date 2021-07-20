package com.github.valrcs.spark

import org.apache.spark.sql.functions.expr

object ExerciseJul20 extends App {
  val spark = SparkUtil.createSpark("exerciseJul20")

  //TODO read part-01.json
  //TODO get schema
  //TODO count how many walking events there were for user i
  //TODO find out how far user i has walked :)
  //TODO super simple would be find starting and ending location

  //For those who are comfortable
  //TODO even better would be if you can calculate distance between each walking event
  //above would probably involve Window functios
  // TODO this would involve sorting by Creation Time

  //lets use Euclidian disantance in 3 dimension so moveement would be Square root of xdelta squared + ydelta squared + zdelta squared

  //TODO read part-01.json
  val filePath = "src/resources/activity-data/part-01.json"
  val df = spark.read
    .format("json")
    .load(filePath)

  //TODO get schema
  df.printSchema()
  df.show(10, false)

  //TODO count how many walking events there were for user i
  val walkingCount = df.where("user == 'i'")
    .where("gt == 'walk'")
    .count()
  println(s"The user i has $walkingCount walking events")

  val walkDf = df.where("user == 'i'")
    .where("gt == 'walk'")
    .withColumn("distance", expr("ROUND(SQRT(POW(x,2)+POW(y,2)+POW(z,2)),4)")) //x,y,z are deltas already
//perhaps z should not be included when walking, it seems to measure up and down movement in your pocket
  walkDf.show(5, false)

  walkDf
    .select(expr("ROUND(SUM(distance),4)"))
    .show()

  //how to get specific values out of dataFrame in specific type
  //https://stackoverflow.com/questions/58602034/in-scala-how-to-convert-the-result-in-sql-query-row-to-double
  import spark.implicits._
  val distanceWalked =   walkDf
    .select(expr("ROUND(SUM(distance),4)"))
    .as[Double].first() //we would have multiple values possibly we want the first (and only here)


  println(s"Walked $distanceWalked units(meters?)")


}
