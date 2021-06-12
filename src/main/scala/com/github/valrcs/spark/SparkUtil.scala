package com.github.valrcs.spark
import org.apache.spark.sql.SparkSession

object SparkUtil {
  //TODO docstrings
  def createSpark(appName:String, verbose:Boolean = true): SparkSession = {
    if (verbose) println(s"$appName with Scala version: ${util.Properties.versionNumberString}")

    val spark = SparkSession.builder().appName(appName).master("local").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", "5") //recommended for local, default is 200?
    if (verbose) println(s"Session started on Spark version ${spark.version}")
    spark
  }
}
