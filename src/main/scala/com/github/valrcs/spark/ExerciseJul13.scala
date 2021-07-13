package com.github.valrcs.spark

import org.apache.spark.ml.tuning.TrainValidationSplitModel

object ExerciseJul13 extends App {

  //TODO run and save model using code from Ch24Pipeline

  //TODO load the model here
  //TODO load into dataframe ml-data.csv

  //TODO make a prediction(transform) on all values of dataframe (no need to split)

  //TODO Show the results

  val spark = SparkUtil.createSpark("loadingModel")
  val model = TrainValidationSplitModel.load("./src/resources/models")

  val filePath = "./src/resources/csv/ml-data.csv"

  val df = spark
    .read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(filePath)

  df.show(5)

  val fittedDF = model.transform(df)

  fittedDF.show(false)


}
