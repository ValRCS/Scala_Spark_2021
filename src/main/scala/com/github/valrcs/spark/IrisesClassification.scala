package com.github.valrcs.spark

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{Bucketizer, RFormula, StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.DataFrame

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


  //let's how to make a features column using  VectorAssembler
  val va = new VectorAssembler()
    .setInputCols(Array("_c0","_c1","_c2","_c3"))
    .setOutputCol("features") //default name is kind of ugly vecAssembler
  val tdf = va.transform(df)
  tdf.show(5, false)

  //let's convert our string label _c4 into a numerical value

  val labelIndexer = new StringIndexer().setInputCol("_c4").setOutputCol("label")
  val labelDF = labelIndexer.fit(tdf).transform(tdf)
  labelDF.show(5,false)

  val fittedModel2 = decTree.fit(labelDF) //create a new model but I used ALL of the data!!!
  //so using test dataframe is sort of useless because we alreday learned from the whole dataset, so chance of overfit is extremely

  val fittedDF = fittedModel2.transform(test)
  fittedDF.show(5, false)

  //bare minimum to make a prediction with some classifier model is to have a Vector[Double] column default name being features
  fittedModel2.transform(test.select("features")).show(5, false)

  def showAccuracy(df: DataFrame): Unit = {
    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(df) //in order for this to work we need label and prediction columns
    println(s"DF size: ${df.count()} Accuracy $accuracy - Test Error = ${(1.0 - accuracy)}")
  }
  showAccuracy(fittedDF)
  showAccuracy(testDF)


  //so why is this a bit misleading this 100% accuracy ?

  //TODO move onto Regression (quantitative predictions)

  //lets try a bad Decision Tree

//  val badDecTreeModel = new DecisionTreeClassifier()

  //creating a label with a bucketer out of some numeric column
  val bucketBorders = Array(0.0, 3, 4.7, 5, 6, 10) //Array head should be less or equal to min value and tail should be more or equal to max value of our column
  val bucketer = new Bucketizer()
    .setSplits(bucketBorders)
    .setInputCol("_c0")
    .setOutputCol("label")
  val bucketedDF = bucketer.transform(df)
  bucketedDF.show(5, false)

  val badDecisionTreeClassifier = new DecisionTreeClassifier()
    .setLabelCol("label")
    .setFeaturesCol("features")
    //so lets try changing some hyper parameters
    .setMaxDepth(1) //so only one question, what do you thing it will do to the accuracy ? ;)

  val badDecModel = badDecisionTreeClassifier.fit(train) //create a new model but I used ALL of the data!!!
  //so using test dataframe is sort of useless because we alreday learned from the whole dataset, so chance of overfit is extremely

  val badTestDF = badDecModel.transform(test)

  showAccuracy(badTestDF)

  def decisionTreeTester(classifier:DecisionTreeClassifier, train: DataFrame, test: DataFrame):Unit = {
    val model = classifier.fit(train)
    val df = model.transform(test)
    showAccuracy(df)
  }

  badDecisionTreeClassifier.setMaxDepth(2) //so now it should be a bit better
  decisionTreeTester(badDecisionTreeClassifier, train, test)

  println("this is our own pipeline tester")
  (1 to 5).foreach(n => {
    badDecisionTreeClassifier.setMaxDepth(n)
    decisionTreeTester(badDecisionTreeClassifier, train,test )})

  //lets do the official pipeline tester

  val stages = Array(decTree) //we are only going to test hyperparameters for our decision tree
  val pipeline = new Pipeline().setStages(stages)

  val params = new ParamGridBuilder()
    .addGrid(decTree.maxDepth, Array(1,2,3,4,5)) //so doing the same as the foreach loop
    .build()

  val evaluator = new MulticlassClassificationEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("accuracy")

  val tvs = new TrainValidationSplit()
    .setTrainRatio(0.75) // also the default. 75% for training
    .setEstimatorParamMaps(params)
    .setEstimator(pipeline)
    .setEvaluator(evaluator)

  val tvsFitted = tvs.fit(train) //this is where all the work is done (and takes the longest)

  val testResults = evaluator.evaluate(tvsFitted.transform(test)) //this will tell us how the best model works
  println(s"Our model is $testResults accurate on test data set")

  val trainedPipeline = tvsFitted.bestModel.asInstanceOf[PipelineModel] //so we extract the best model out of our pipeline
  //then we want to know what the best parameters are
  import org.apache.spark.ml.classification.DecisionTreeClassificationModel
  val trainedDecTree = trainedPipeline.stages(0).asInstanceOf[DecisionTreeClassificationModel]
  println(trainedDecTree.toDebugString)
}
