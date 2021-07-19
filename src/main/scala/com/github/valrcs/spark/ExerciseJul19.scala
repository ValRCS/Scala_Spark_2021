package com.github.valrcs.spark

object ExerciseJul19 extends App {
  val spark = SparkUtil.createSpark("exerciseJul19")
  val filePath = "./src/resource/csv/stocks_2013_2018.csv"

  //TODO read the file, infer schema

  //TODO calculate the daily change in percentages for all rows
  //TODO group the results by day and calculate the average change for all stocks combined by day
}
