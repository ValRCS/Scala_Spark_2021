package com.github.valrcs.spark

import org.apache.spark.sql.DataFrame

object Ch29Clustering extends App {
  //clustering falls under unsupervised learning - meaning we do not provide the labels/answers, we may not have them at all

  //here with irises we do have the answers but we will not use them

  //clustering is good for finding outliers in your data, some extreme values
  //also if you know that there should be specific number of groups then k-means will be a great choice

  val spark = SparkUtil.createSpark("irisesClustering")

  val filePath = "./src/resources/irises/iris.data"
  val df = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .load(filePath)
    //we can add column names upon loading
    .toDF("petal_width", "petal_height", "sepal_width", "sepal_height", "category")

  df.printSchema()
  df.describe().show(false)
  df.show(5, false)

  import org.apache.spark.ml.feature.RFormula
  val featureFormula = new RFormula()
    .setFormula("~ petal_width + petal_height + sepal_width + sepal_height") //so I will use all 4 features
  //you could play around and use less features

  val ndf = featureFormula.fit(df).transform(df)

  ndf.show(5)

  // in Scala
  import org.apache.spark.ml.clustering.KMeans

  val km = new KMeans().setK(3) //we know there are only 3 types
  println(km.explainParams())
  val kmModel = km.fit(ndf)

  val summary = kmModel.summary
  println("Cluster Sizes")
  summary.clusterSizes.foreach(println)
  println("Cluster Centers: ")
  kmModel.clusterCenters.foreach(println)

  def testKMeans(df: DataFrame, k: Integer): Unit = {
    println(s"Testing Kmeans with $k clusters")
    val km = new KMeans().setK(k) //we know there are only 3 types
    println(km.explainParams())
    val kmModel = km.fit(df)

    val summary = kmModel.summary
    println("Cluster Sizes")
    summary.clusterSizes.foreach(println)
    println("Cluster Centers: ")
    kmModel.clusterCenters.foreach(println)
  }

  (2 to 6).foreach(n => testKMeans(ndf, n))



}
