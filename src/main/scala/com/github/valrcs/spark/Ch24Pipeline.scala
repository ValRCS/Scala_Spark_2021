package com.github.valrcs.spark

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.RFormula
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}

object Ch24Pipeline extends App {
  val spark = SparkUtil.createSpark("simpleMLpipeline")
  val filePath = "./src/resources/simple-ml/*.json"

  val df = spark
    .read
    .format("json")
    .load(filePath)

  df.show(5)

  //In order to make sure we don’t overfit, we are going to create a holdout test set and tune our
  //hyperparameters based on a validation set (note that we create this validation set based on the
  //original dataset, not the preparedDF used in the previous pages)

  val Array(train, test) = df.randomSplit(Array(0.7, 0.3))

  val lr = new LogisticRegression().setLabelCol("label").setFeaturesCol("features")
  val rForm = new RFormula() //We will set the potential values for the RFormula in the next section. Now instead of manually
//  using our transformations and then tuning our model we just make them stages in the overall
//    pipeline, as in the following code snippet:

  val stages = Array(rForm, lr)
  val pipeline = new Pipeline().setStages(stages)

  //Training and Evaluation
  //Now that you arranged the logical pipeline, the next step is training. In our case, we won’t train
  //just one model (like we did previously); we will train several variations of the model by
  //specifying different combinations of hyperparameters that we would like Spark to test. We will
  //then select the best model using an Evaluator that compares their predictions on our validation
  //data. We can test different hyperparameters in the entire pipeline, even in the RFormula that we
  //use to manipulate the raw data. This code shows how we go about doing that:

  val params = new ParamGridBuilder()
    .addGrid(rForm.formula, Array(
      "lab ~ . + color:value1", //we are going to test building a model with only color and value 1 as features
      "lab ~ . + color:value1 + color:value2")) //here we want to test both values and color
    .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0)) //so elastinetParam will apply to certain models including logistic regression
    .addGrid(lr.regParam, Array(0.1, 2.0))
    //we could add more parameters here
    .build()

  //so how many combinations of parameters will we be testing
  //in other words how many different ways of building model are up on this params Grid


  //next step is to specify how are we going to evaluate the results

  //Now that the grid is built, it’s time to specify our evaluation process. The evaluator allows us to
  //automatically and objectively compare multiple models to the same evaluation metric. There are
  //evaluators for classification and regression, covered in later chapters, but in this case we will use
  //the BinaryClassificationEvaluator, which has a number of potential evaluation metrics, as
  //we’ll discuss in Chapter 26. In this case we will use areaUnderROC, which is the total area under
  //the receiver operating characteristic, a common measure of classification performance:

  val evaluator = new BinaryClassificationEvaluator()
    .setMetricName("areaUnderROC")
    .setRawPredictionCol("prediction")
    .setLabelCol("label")

  //As we discussed, it is a best practice in machine learning to fit hyperparameters on a validation
  //set (instead of your test set) to prevent overfitting. For this reason, we cannot use our holdout test
  //set (that we created before) to tune these parameters. Luckily, Spark provides two options for
  //performing hyperparameter tuning automatically. We can use TrainValidationSplit, which
  //will simply perform an arbitrary random split of our data into two different groups, or
  //CrossValidator, which performs K-fold cross-validation by splitting the dataset into k non-
  //overlapping, randomly partitioned folds

  val tvs = new TrainValidationSplit()
    .setTrainRatio(0.75) // also the default. 75% for training
    .setEstimatorParamMaps(params)
    .setEstimator(pipeline)
    .setEvaluator(evaluator)

  //finally we are ready to test our 12 combinations
  //Let’s run the entire pipeline we constructed. To review, running this pipeline will test out every
  //version of the model against the validation set. Note the type of tvsFitted is
  //TrainValidationSplitModel. Any time we fit a given model, it outputs a “model” type

  val tvsFitted = tvs.fit(train) //this is where all the work is done (and takes the longest)

  val testResults = evaluator.evaluate(tvsFitted.transform(test)) //this will tell us how the best model works
  println(s"Our model is $testResults accurate on test data set")

  val trainedPipeline = tvsFitted.bestModel.asInstanceOf[PipelineModel]
  import org.apache.spark.ml.classification.LogisticRegressionModel
  val TrainedLR = trainedPipeline.stages(1).asInstanceOf[LogisticRegressionModel]
  val summaryLR = TrainedLR.summary
  println("Our progress has been")
  println(summaryLR.objectiveHistory.mkString("Array(", ", ", ")"))

//  The objective history shown here provides details related to how our algorithm performed over
//    each training iteration. This can be helpful because we can note the progress our algorithm is
//  making toward the best model. Large jumps are typically expected at the beginning, but over
//    time the values should become smaller and smaller, with only small amounts of variation
//  between the values

  //Now that we trained this model, we can persist it to disk to use it for prediction purposes later on
  tvsFitted.write.overwrite.save("./src/resources/models")

}
