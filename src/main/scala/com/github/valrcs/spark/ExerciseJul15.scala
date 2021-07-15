package com.github.valrcs.spark

import org.apache.spark.ml.classification.{ RandomForestClassifier,GBTClassifier,LinearSVC,NaiveBayes}

object ExerciseJul15 extends App {
  //TODO load iris.data
  //TODO pick one classifier not yet used see above or can use a few more from documentation below
  //also https://spark.apache.org/docs/latest/ml-classification-regression.html#classification

  //prepare features and label columns (can use RFormula or Vectorizer and StringIndexer)
  //split irises in 75% - 25% split of train, test

  //train on the train set
  //evaluate accuracy on the test set
}
