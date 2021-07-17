package com.github.valrcs.spark

import org.apache.spark.ml.regression.LinearRegression

object RegressionPipeline extends App {
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
    .setFormula("y ~ . ")

  val ndf = supervised
    .fit(df) //prepares the formula
    .transform(df) //generally transform will create the new data

  ndf.show(5)


  import org.apache.spark.ml.evaluation.RegressionEvaluator
  import org.apache.spark.ml.regression.GeneralizedLinearRegression
  import org.apache.spark.ml.Pipeline
  import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
//  val glr = new GeneralizedLinearRegression()
//    .setFamily("gaussian")
//    .setLink("identity")
  val glr = new LinearRegression()
  val pipeline = new Pipeline().setStages(Array(glr))
  val params = new ParamGridBuilder().addGrid(glr.regParam, Array(0, 0.5, 1))
    .build()
  val evaluator = new RegressionEvaluator()
    .setMetricName("rmse") //root mean square error
    .setPredictionCol("prediction")
    .setLabelCol("label")
  val cv = new CrossValidator()
    .setEstimator(pipeline)
    .setEvaluator(evaluator)
    .setEstimatorParamMaps(params)
    .setNumFolds(5) // should always be 3 or more but this dataset is small //meaning we will test our data in folds of 20

  val model = cv.fit(ndf)

  println(model.bestModel.toString)
  println(model.bestModel.params.mkString("Parameters(", " ||| ", ")")) //TODO this is empty
  println("Model 1 was fit using parameters: " + model.bestModel.extractParamMap) //also nothing
  println(model.bestModel.explainParams())
//  println(model.bestModel.getParam("regularization"))

  import org.apache.spark.mllib.evaluation.RegressionMetrics
  val out = model.transform(ndf)
    .select("prediction", "label")
    .rdd.map(x => (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double]))
  val metrics = new RegressionMetrics(out)
  println(s"MSE = ${metrics.meanSquaredError}")
  println(s"RMSE = ${metrics.rootMeanSquaredError}")
  println(s"R-squared = ${metrics.r2}")
  println(s"MAE = ${metrics.meanAbsoluteError}")
  println(s"Explained variance = ${metrics.explainedVariance}")

}
