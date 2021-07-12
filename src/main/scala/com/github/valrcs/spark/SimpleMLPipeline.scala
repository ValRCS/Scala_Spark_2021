package com.github.valrcs.spark

import org.apache.spark.sql.functions.{count, expr, sum}

object SimpleMLPipeline extends App {
  //TODO read json file from simple-ml folder
  //TODO print schema and print 10 values
  val spark = SparkUtil.createSpark("simpleML")
  val filePath = "./src/resources/simple-ml/*.json"

  val jsonDF = spark
    .read
    .format("json")
    .load(filePath)

  jsonDF.printSchema()
  jsonDF.show(10,false)
  jsonDF.describe().show(false)

  //Feature Engineering with Transformers
  //As already mentioned, transformers help us manipulate our current columns in one way or
  //another. Manipulating these columns is often in pursuit of building features (that we will input
  //into our model). Transformers exist to either cut down the number of features, add more features,
  //manipulate current ones, or simply to help us format our data correctly. Transformers add new
  //columns to DataFrames.
  //When we use MLlib, all inputs to machine learning algorithms (with several exceptions
  //discussed in later chapters) in Spark must consist of type Double (for labels) and
  //Vector[Double] (for features).


  //To achieve this in our example, we are going to specify an RFormula. This is a declarative
  //language for specifying machine learning transformations and is simple to use once you
  //understand the syntax. RFormula supports a limited subset of the R operators that in practice
  //work quite well for simple models and manipulations (we demonstrate the manual approach to
  //this problem in Chapter 25). The basic RFormula operators are:
  //~
  //Separate target and terms
  //+
  //Concat terms; “+ 0” means removing the intercept (this means that the y-intercept of the line
  //that we will fit will be 0)
  //-
  //Remove a term; “- 1” means removing the intercept (this means that the y-intercept of the
  //line that we will fit will be 0—yes, this does the same thing as “+ 0”
  //:
  //Interaction (multiplication for numeric values, or binarized categorical values)
  //.
  //All columns except the target/dependent variable
  //In order to specify transformations with this syntax, we need to import the relevant class. Then
  //we go through the process of defining our formula. In this case we want to use all available
  //variables (the .) and also add in the interactions between value1 and color and value2 and
  //color, treating those as new features:

  import org.apache.spark.ml.feature.RFormula
  val supervised = new RFormula()
    .setFormula("lab ~ . + color:value1 + color:value2") //this specifies which is our label column
  //also specifies what rest of columns will combine to create features super column(of Vector[Double] type)

  // in Scala
  val fittedRF = supervised.fit(jsonDF)
  val preparedDF = fittedRF.transform(jsonDF)
  preparedDF.show(false)

  //In the output we can see the result of our transformation—a column called features that has our
  //previously raw data. What’s happening behind the scenes is actually pretty simple. RFormula
  //inspects our data during the fit call and outputs an object that will transform our data according
  //to the specified formula, which is called an RFormulaModel. This “trained” transformer always
  //has the word Model in the type signature. When we use this transformer, Spark automatically
  //converts our categorical variable to Doubles so that we can input it into a (yet to be specified)
  //machine learning model. In particular, it assigns a numerical value to each possible color
  //category, creates additional features for the interaction variables between colors and
  //value1/value2, and puts them all into a single vector. We then call transform on that object in
  //order to transform our input data into the expected output data.

  //Thus far you (pre)processed the data and added some features along the way. Now it is time to
  //actually train a model (or a set of models) on this dataset. In order to do this, you first need to
  //prepare a test set for evaluation.
  //TIP
  //Having a good test set is probably the most important thing you can do to ensure you train a model you
  //can actually use in the real world (in a dependable way). Not creating a representative test set or using
  //your test set for hyperparameter tuning are surefire ways to create a model that does not perform well
  //in real-world scenarios. Don’t skip creating a test set—it’s a requirement to know how well your
  //model actually does!

  val Array(train, test) = preparedDF.randomSplit(Array(0.7, 0.3))
  train.describe().show(false) //70 for training our model
  test.describe().show(false) //30 percent for testing , important that you do not mix with above train

//  Estimators
//  Now that we have transformed our data into the correct format and created some valuable
//  features, it’s time to actually fit our model. In this case we will use a classification algorithm
//    called logistic regression. To create our classifier we instantiate an instance of
//  LogisticRegression, using the default configuration or hyperparameters. We then set the label
//  columns and the feature columns; the column names we are setting—label and features—are
//  actually the default labels for all estimators in Spark MLlib, and in later chapters we omit them:

  // in Scala
  import org.apache.spark.ml.classification.LogisticRegression
  val lr = new LogisticRegression()
    .setLabelCol("label")
    .setFeaturesCol("features")

//  Before we actually go about training this model, let’s inspect the parameters. This is also a great
//  way to remind yourself of the options available for each particular model:
    // in Scala
    println(lr.explainParams())

  //finally we can create our predictive model
  //slightly anti-climatic

  //Upon instantiating an untrained algorithm, it becomes time to fit it to data. In this case, this
  //returns a LogisticRegressionModel
  // in Scala
  val fittedLR = lr.fit(train) // so we used these 78 lines/rows of data (features column) to have the machine
  //learn which labels(in this case 0 or 1)  fit which features (our 10 value vectors)

  //eager evaluation of the above - meaning not lazy, meaning performed immediately

  //Once complete, you can use the model to make predictions. Logically this means tranforming
  //features into labels. We make predictions with the transform method. For example, we can
  //transform our training dataset to see what labels our model assigned to the training data and how
  //those compare to the true outputs. This, again, is just another DataFrame we can manipulate.
  //Let’s perform that prediction with the following code snippet:

  val fittedTrain = fittedLR.transform(train)
  fittedTrain.show(5,false)

  //well on the training set we expect near 100 % accuracy of labels matching predictions

  //however things might be different when we transform our test set (which model does not know about)

  val fittedTest = fittedLR.transform(test)

  //lets evaluate accuracy by hand (there are ML lib methods do this as well)

  fittedTrain
    .select("label", "prediction")
    .withColumn("match", expr("label = prediction")) //so creating a Boolean of whether prediction matches label
    .agg(expr("SUM(INT(match))"), count("match"))
    .show(false)
  //so 77 out of 77 we have 100% of 1.00 accuracy BUT that is on training set

  fittedTest
    .select("label", "prediction")
    .withColumn("match", expr("label = prediction"))
    .agg(expr("SUM(INT(match))"), count("match"))
    .show(false)

  //so looks our model works great on these binary (good or bad) classification tasks
  //TODO evaluate our accuracy using ML lib since accuracy measurement is basically required to check if our model works!

  //TODO build a full pipeline of commands (meaning combine the above steps into one whole)
}
