package com.github.valrcs.spark

import org.apache.spark.ml.feature.RFormula

object IrisesClassification extends App {
  val spark = SparkUtil.createSpark("irisesClassification")
  val filePath = "./src/resources/irises/iris.data"
  val df = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .load(filePath)

  df.printSchema()
  df.describe().show(false)
  df.show(5, false)

  //now that we have data loaded with default column names _c0 ... _c4
  //we need to create two new columns one would be features (which combines all 4 measurements into a Vector of Doubles)
  //and we need to convert the string labels into numeric labels(most likely 0,1,2) again doubles

  val supervised = new RFormula() //RFormula is a quicker way of creating needed column
    .setFormula("flower ~ . ")
//    .setFormula("_c4 ~ . + _c0 + _c1 + _c2 + _c3")

  val ndf = df.withColumnRenamed("_c4", "flower")
  ndf.show(5, false)

  val fittedRF = supervised.fit(ndf)
  val preparedDF = fittedRF.transform(ndf)
  preparedDF.show(false)
  preparedDF.sample(0.1).show(false)

  val Array(train, test) = preparedDF.randomSplit(Array(0.8, 0.2)) //so 80 percent for training and 20 percent for testing

  import org.apache.spark.ml.classification.DecisionTreeClassifier //this Algorithm is like a game of yes/no questions
  //like the party game 21 questions
  //we could creat more models out of different classifiers
  val decTree = new DecisionTreeClassifier() //there are hyperparameters we could adjust but not for now
    .setLabelCol("label")
    .setFeaturesCol("features")

  val fittedModel = decTree.fit(train) //this is the hard work here of creating the model


  val testDF = fittedModel.transform(test) //here we get some results

  testDF.show(30,false) //we should have roughly 30 (since 20% of 150 is 30)

  //TODO compare Decision Tree with Logistic Regression and some other Classifier algorithms

  //TODO move onto Regression (quantitative predictions)



}
