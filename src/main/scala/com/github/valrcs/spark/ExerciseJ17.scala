package com.github.valrcs.spark

object ExerciseJ17 extends App {
  val spark = SparkUtil.createSpark("ExerciseJ17")

  //TODO Read all retail CSV src/resources/retail-data/all/online-retail-dataset.csv

  //TODO show top 10 biggest quantities bought (price can be used as 2nd tiebreak also desc)

  //BONUS challenge
  //TODO create total column which is price * quantity (can use SPARK SQL for this)
  //TODO show top 10 purchases


}
