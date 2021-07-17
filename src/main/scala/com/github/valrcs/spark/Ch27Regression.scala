package com.github.valrcs.spark

import org.apache.spark.ml.regression.LinearRegression

object Ch27Regression extends App {
  val spark = SparkUtil.createSpark("regressions")
  val filePath = "./src/resources/csv/range100.csv"

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
    .setFormula("y ~ . ")

  val ndf = supervised
    .fit(df) //prepares the formula
    .transform(df) //generally transform will create the new data

  ndf.show(5)


  val linReg = new LinearRegression()
//    .setLabelCol("y") //in ML answers/labels are often called with small y
//    .setFeaturesCol("x") //in ML multiple features are often marked with big X
  //we could set some hyper-parameters here as well
  println(linReg.explainParams())

  val lrModel = linReg.fit(ndf) //this actually does the work of creating the model

  val summary = lrModel.summary
  summary.residuals.show()
  val intercept = lrModel.intercept
  val coefficient = lrModel.coefficients(0) //we only have one cofficient since we only have one column of x
  println(s"Intercept: $intercept and coefficient is $coefficient")
  //if y = ax+b then coefficient is a and intercept is b
}
