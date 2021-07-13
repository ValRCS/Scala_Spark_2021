package com.github.valrcs.spark

import org.apache.spark.ml.tuning.TrainValidationSplitModel

object Ch24LoadingModel extends App {

  val spark = SparkUtil.createSpark("loadingModel")
  //After writing out the model, we can load it into another Spark program to make predictions. To
  //do this, we need to use a “model” version of our particular algorithm to load our persisted model
  //from disk. If we were to use CrossValidator, we’d have to read in the persisted version as the
  //CrossValidatorModel, and if we were to use LogisticRegression manually we would have
  //to use LogisticRegressionModel. In this case, we use TrainValidationSplit, which outputs
  //TrainValidationSplitModel:

  val model = TrainValidationSplitModel.load("./src/resources/models")

  val filePath = "./src/resources/simple-ml/*.json"

  val df = spark
    .read
    .format("json")
    .load(filePath)

  df.show(5)

  val Array(train, test) = df.randomSplit(Array(0.7, 0.3))

  val fittedTest = model.transform(test)
  fittedTest.show(10, false)

}
