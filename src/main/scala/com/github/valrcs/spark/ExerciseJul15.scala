package com.github.valrcs.spark

import org.apache.spark.ml.classification.{GBTClassifier, LinearSVC, NaiveBayes, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.DataFrame

object ExerciseJul15 extends App {
  //TODO load iris.data
  //TODO pick one classifier not yet used see above or can use a few more from documentation below
  //also https://spark.apache.org/docs/latest/ml-classification-regression.html#classification

  //prepare features and label columns (can use RFormula or Vectorizer and StringIndexer)
  //split irises in 75% - 25% split of train, test

  //train on the train set
  //evaluate accuracy on the test set

//  val spark = SparkUtil.createSpark("irisesClassification")
//  val filePath = "./src/resources/irises/iris.data"
//  val df = spark.read
//    .format("csv")
//    .option("inferSchema", "true")
//    .load(filePath)
//
//  df.printSchema()
//  df.describe().show(false)
//  df.show(5)
//
//  val supervised = new RFormula()
//    .setFormula("flower ~ . ") //so formulae is defined but not called here
//
//  val ndf = df.withColumnRenamed("_c4", "flower")
//  ndf.show(5, false)
//
//  val fittedRF = supervised.fit(ndf)
//  val preparedDF = fittedRF.transform(ndf)
//  preparedDF.show(false)
//  preparedDF.sample(0.1).show(false)
//
//  val Array(train, test) = preparedDF.randomSplit(Array(0.75, 0.25))
//
//  val randForest = new RandomForestRegressor()
//    .setLabelCol("label")
//    .setFeaturesCol("features")
//
//  val fittedModel = randForest.fit(train)
//
//  val testDF = fittedModel.transform(test)
//
//  testDF.show(30, false)
//
  def showAccuracy(df: DataFrame): Unit = {
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(df)
    println(s"DF size: ${df.count()} Accuracy $accuracy - Test Error = ${1.0 - accuracy}")
  }
//
//  showAccuracy(testDF)

  val spark = SparkUtil.createSpark("irisesClassification")
  val filePath = "./src/resources/irises/iris.data"
  val df = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .load(filePath)

  df.printSchema()
  df.describe().show(false)
  df.show(5, false)

  val supervised = new RFormula() //RFormula is a quicker way of creating needed column
    .setFormula("flower ~ . ")

  val ndf = df.withColumnRenamed("_c4", "flower")
  ndf.show(5, false)

  val fittedRF = supervised.fit(ndf)
  val preparedDF = fittedRF.transform(ndf)
  preparedDF.show(false)
  preparedDF.sample(0.1).show(false)

  val Array(train, test) = preparedDF.randomSplit(Array(0.75, 0.25))
  val randomForest = new RandomForestClassifier()
    .setLabelCol("label")
    .setFeaturesCol("features")
    .setNumTrees(10) //this is so called hyperparameter for this particular algorithm

  val fittedModel = randomForest.fit(train) //create a model
  val testDF = fittedModel.transform(test)  //use this model to make predictions and save them in a DataFrame
  testDF.show(30,false)

  showAccuracy((testDF))
}
