package com.github.valrcs.spark

object Ch12RDD extends App {

  val spark = SparkUtil.createSpark("ch12RDD")

  //One of the easiest ways to get RDDs is from an existing DataFrame or Dataset. Converting these
  //to an RDD is simple: just use the rdd method on any of these data types. You’ll notice that if you
  //do a conversion from a Dataset[T] to an RDD, you’ll get the appropriate native type T back
  //(remember this applies only to Scala and Java):
  //// in Scala: converts a Dataset[Long] to RDD[Long]

  //so RDD is the lowest level and how we worked 10 years ago
  val myRDD = spark.range(500).rdd
  myRDD.take(5).foreach(println)

  val dataSet = spark.range(20)

  //again try to work in this level
  val df = spark.range(10).toDF()
  df.printSchema()
  df.show(5)

  //so if you encounter older code which works with RDDs, you might want to migrate some to DataFrames
  import spark.implicits._
  val myDF = myRDD.toDF()
  myDF.printSchema()
  myDF.show(5)
  myDF.describe().show(false) //so lets get some stats which in RDD you'd have to create your own

  // in Scala
  //From a Local Collection
  //To create an RDD from a collection, you will need to use the parallelize method on a
  //SparkContext (within a SparkSession). This turns a single node collection into a parallel
  //collection. When creating this parallel collection, you can also explicitly state the number of
  //partitions into which you would like to distribute this array. In this case, we are creating two
  //partitions
  val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
    .split(" ") //so split by whitespace

  val words = spark.sparkContext.parallelize(myCollection, 2) //if you had 100 machines you would enter 100 here :)

  val wordDF = words.toDF() //my pragmatic advice is to go to DataFrames and do your manipulations there
  wordDF.printSchema()
  wordDF.show()

  //it is possible to do Transformations with RDDs but there should be no need anymore
}
