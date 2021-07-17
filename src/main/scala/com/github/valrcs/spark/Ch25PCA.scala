package com.github.valrcs.spark

import com.github.valrcs.spark.Ch29Clustering.df

object Ch25PCA extends App {

  val spark = SparkUtil.createSpark("irisesDimensionalityReduction")

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
  import org.apache.spark.ml.feature.PCA
  val pca = new PCA().setInputCol("features").setK(2) //so we are going from 4 to 2 features but losing some information
  val reducedDF  = pca
    .fit(ndf)
    .transform(ndf)

  reducedDF.show(10, false)

  val columnNames = reducedDF.columns
  println(columnNames(0))
  println(columnNames(columnNames.size-1)) //this works but it is sort of old school

  val lastColName = reducedDF.columns(reducedDF.columns.size-1) //one liner for last column name
  val renamedDF = reducedDF
    .withColumnRenamed(lastColName, "PCA_2")

  renamedDF.show(10, false)

  //again the last column represents 4 main feature columns reduced into 2 (with some information loss)

  //not covered in this course, we might want to scale the values into some range, sort of normalize
  //for example you might scale values into 0 to 1 range, or -1 to 1 range, some algorithms need that
//  https://spark.apache.org/docs/latest/ml-features.html#standardscaler
  import org.apache.spark.ml.feature.StandardScaler
  val scaler = new StandardScaler()
    .setInputCol("features")
//    .setInputCol("PCA_2")
    .setOutputCol("scaledFeatures")
    .setWithStd(true) //all values within 1 standard deviation
    .setWithMean(true) //mean has to be 0

  // Compute summary statistics by fitting the StandardScaler.
  val scalerModel = scaler.fit(renamedDF)

  // Normalize each feature to have unit standard deviation.
  val scaledData = scalerModel.transform(renamedDF)
  scaledData.show(10, false)

  //with irises scaler is not crucial as data is in similar range for ALL 4 features/columns
  //for data where there are say 2 columns one has features in range 0 to 10, one has features in range 1000 to 5000
  //the 2nd feature will dominate the first feature even if it does not warrant such a domination
  //so if all animals/features are equal then you should perform normalization / scaling

}
