package com.github.valrcs.spark

import org.apache.spark.mllib.clustering.LDA
import org.apache.spark.mllib.linalg.Vectors

object Ch29TopicClustering extends App {
  val spark = SparkUtil.createSpark("LDAclustering")
  val filePath = "./src/resources/mllib/sample_lda_data.txt"

  //http://spark.apache.org/docs/latest/mllib-clustering.html
  // Load and parse the data
  val data = spark.sparkContext.textFile(filePath) //this is lower level RDD datastructure
  val parsedData = data.map(s => Vectors.dense(s.trim.split(' ').map(_.toDouble)))

  // Index documents with unique IDs
  val corpus = parsedData.zipWithIndex.map(_.swap).cache()

  // Cluster the documents into three topics using LDA
  val ldaModel = new LDA().setK(3).run(corpus)

  // Output topics. Each is a distribution over words (matching word count vectors)
  println(s"Learned topics (as distributions over vocab of ${ldaModel.vocabSize} words):")
  val topics = ldaModel.topicsMatrix
  for (topic <- Range(0, 3)) {
    print(s"Topic $topic :")
    for (word <- Range(0, ldaModel.vocabSize)) {
      print(s"${topics(word, topic)}")
    }
    println()
  }
}
