package com.github.valrcs.spark

import org.apache.spark.ml.regression.{LinearRegression, IsotonicRegression}

object ExerciseJul17 extends App {
  val spark = SparkUtil.createSpark("regressions")
  //TODO load range3d.csv

  //TODO Find Interecept and Coefficients (there are 3!) for simple Linear Regression
  val filePath = "./src/resources/csv/range3d.csv"

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
    .setFormula("y ~ x1 + x2 + x3")

  val ndf = supervised
    .fit(df) //prepares the formula
    .transform(df) //generally transform will create the new data

  ndf.show(5)

  val Array(train,test) = ndf.randomSplit(Array(0.75, 0.25))

  val linReg = new LinearRegression()

//  val lrModel = linReg.fit(ndf)
  val lrModel = linReg.fit(train)

  val intercept = lrModel.intercept
  val coefficients = lrModel.coefficients
  val x1 = coefficients(0) // we have 3 columns of x
  val x2 = coefficients(1)
  val x3 = coefficients(2)

  println(s"Intercept: $intercept and coefficient for x1 is $x1, for x2 is $x2 and for x3 is $x3")
  val predictedDF = lrModel.transform(test)
  predictedDF.show(false)

  // Trains an isotonic regression model.
  val ir = new IsotonicRegression() //Isotonic has to be a growing function
  val model = ir.fit(ndf)

  println(s"Boundaries in increasing order: ${model.boundaries}\n")
  println(s"Predictions associated with the boundaries: ${model.predictions}\n")

  //you can predict
//  model.transform(ndf) //instead of ndf you would use the data that you want predict
//
}
