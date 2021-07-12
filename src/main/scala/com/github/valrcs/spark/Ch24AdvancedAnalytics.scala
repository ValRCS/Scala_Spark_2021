package com.github.valrcs.spark

object Ch24AdvancedAnalytics extends App {
//  The overall process involves, the following steps (with some variation):
//  1. Gathering and collecting the relevant data for your task.
//  2. Cleaning and inspecting the data to better understand it.
//  3. Performing feature engineering to allow the algorithm to leverage the data in a suitable
//  form (e.g., converting the data to numerical vectors).
//  4. Using a portion of this data as a training set to train one or more algorithms to generate
//    some candidate models.
//  5. Evaluating and comparing models against your success criteria by objectively
//  measuring results on a subset of the same data that was not used for training. This
//  allows you to better understand how your model may perform in the wild.
//  6. Leveraging the insights from the above process and/or using the model to make
//    predictions, detect anomalies, or solve more general business challenges.

//Data collection
  //Naturally it’s hard to create a training set without first collecting data. Typically this means at
  //least gathering the datasets you’ll want to leverage to train your algorithm. Spark is an excellent
  //tool for this because of its ability to speak to a variety of data sources and work with data big and
  //small.
  //Data cleaning
  //After you’ve gathered the proper data, you’re going to need to clean and inspect it. This is
  //typically done as part of a process called exploratory data analysis, or EDA. EDA generally
  //means using interactive queries and visualization methods in order to better understand
  //distributions, correlations, and other details in your data. During this process you may notice you
  //need to remove some values that may have been misrecorded upstream or that other values may
  //be missing. Whatever the case, it’s always good to know what is in your data to avoid mistakes
  //down the road. The multitude of Spark functions in the structured APIs will provide a simple
  //way to clean and report on your data.
  //Feature engineering
  //Now that you collected and cleaned your dataset, it’s time to convert it to a form suitable for
  //machine learning algorithms, which generally means numerical features. Proper feature
  //engineering can often make or break a machine learning application, so this is one task you’ll
  //want to do carefully. The process of feature engineering includes a variety of tasks, such as
  //normalizing data, adding variables to represent the interactions of other variables, manipulating
  //categorical variables, and converting them to the proper format to be input into our machine
  //learning model. In MLlib, Spark’s machine learning library, all variables will usually have to be
  //input as vectors of doubles (regardless of what they actually represent). We cover the process of
  //feature engineering in great depth in Chapter 25. As you will see in that chapter, Spark provides
  //the essentials you’ll need to manipulate your data using a variety of machine learning statistical
  //techniques.

  //WARNING
  //Following proper training, validation, and test set best practices is essential to successfully using
  //machine learning. It’s easy to end up overfitting (training a model that does not generalize well to new
  //data) if we do not properly isolate these sets of data.

//  Transformers are functions that convert raw data in some way. This might be to create a new
//      interaction variable (from two other variables), normalize a column, or simply change an
//    Integer into a Double type to be input into a model. An example of a transformer is one that
//    converts string categorical variables into numerical values that can be used in MLlib.
//    Transformers are primarily used in preprocessing and feature engineering. Transformers take a
//  DataFrame as input and produce a new DataFrame as output,

 //sparse Vectors vs dense Vectors - sparse is useful when you data collection has many missing values
  // in Scala
  import org.apache.spark.ml.linalg.Vectors
  val denseVec = Vectors.dense(1.0, 2.0, 3.0, 0, 5.0, 0, 7)
  val size = 8
  val idx = Array(1,4,6) // locations of non-zero elements in vector
  val values = Array(2.0,3.0,4.0)
  val sparseVec = Vectors.sparse(size, idx, values)
  println(denseVec) //dense Vector is just like our friend Sequence
  println(sparseVec) //sparse only shows values where they exist

  //we can go back and worse as needed
  println(sparseVec.toDense) //so now we should see zeros as well
  println(denseVec.toSparse) //we should not see zeros here
}
