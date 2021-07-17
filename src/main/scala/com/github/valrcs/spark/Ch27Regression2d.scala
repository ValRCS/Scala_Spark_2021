package com.github.valrcs.spark

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.Row

object Ch27Regression2d extends App {
  val spark = SparkUtil.createSpark("regressions")
  val filePath = "./src/resources/csv/range2d.csv"

  val df = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", filePath)
    .load

  df.printSchema()
  df.show(5)

  import org.apache.spark.ml.feature.RFormula
  val supervised = new RFormula()
    .setFormula("y ~ x1 + x2 ") //so here y is label and x1 and x2 will be used as features
//    .setFormula("y ~ . ") //so y is the answer and rest will be used as features

  //if you do not need some column you could do it in RFormula as above line 19 or alternative
  //would be to select the columns you want in DataFrame  before the below fit -> transform

  val ndf = supervised
    .fit(df) //prepares the formula
    .transform(df) //generally transform will create the new data

  ndf.show(5)

  val linReg = new LinearRegression()

  val lrModel = linReg.fit(ndf)

  val intercept = lrModel.intercept
  val coefficients = lrModel.coefficients
  val x1 = coefficients(0)
  val x2 = coefficients(1) //of course we would have to know how many x columns we have as features

  println(s"Intercept: $intercept and coefficient for x1 is $x1 and for x2 is $x2")

//  val testValues = List(Row(200.0,300.0), Row(100.0,200.0), Row(500.0,100.0)) //this is just some random data
  import org.apache.spark.ml.linalg.Vectors
val arrayDF = spark.createDataFrame(Seq(
  (Vectors.dense(1.0, 2),1),
  (Vectors.dense(200.0, 500),2),
  (Vectors.dense(100.0, 800),3)
)).toDF("features", "label")


//    .withColumnRenamed("value", "features")

  arrayDF.printSchema()
  arrayDF.show()

//  val featureFormula= new RFormula()
//    .setFormula("~ . ") //so we want to only create features out of ALL (here 1) columns no labels
//
//  val arrayFeaturesDF = featureFormula.fit(arrayDF).transform(arrayDF)
//  arrayFeaturesDF.show(5, false)

  val arrayPredicted = lrModel.transform(arrayDF)

  arrayPredicted.show(false)

  val summary = lrModel.summary
  summary.residuals.show()
  println(summary.rootMeanSquaredError)
  println(summary.r2) //these stats show how well our model fitted to the given training data
}
