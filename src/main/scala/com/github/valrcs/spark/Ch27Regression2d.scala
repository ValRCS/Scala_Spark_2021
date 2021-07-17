package com.github.valrcs.spark

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
}
