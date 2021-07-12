package com.github.valrcs.spark

import org.apache.spark.sql.functions.{col, regexp_extract}

object RegexTrick extends App {
  val spark = SparkUtil.createSpark("regexTrick")

  val filePath = "./src/resources/txt/tarzan_and_jane.txt"

  val df = spark.read.text(filePath)

  df.printSchema()
  df.show(false)

  val simpleMatch = "Tarzan"
  val alsoExactMatch = "\"Tarzan\"" //this is just a simple exact match of quoted Tarzan
  val noQuotesMatch = "\"Tarzan\"|(Tarzan)" //idea being that whatever we want is under Group 1 in Parenthesis
  //trick from http://rexegg.com/regex-best-trick.html

  df
    .withColumn("simpleMatch", regexp_extract(col("value"), simpleMatch, 0))
    .withColumn("exactMatch", regexp_extract(col("value"), alsoExactMatch, 0))
    .withColumn("withQuotes", regexp_extract(col("value"), noQuotesMatch, 0)) //so grouping 0 includes also grouping 1
    .withColumn("noDoubleQuotes", regexp_extract(col("value"), noQuotesMatch, 1))
    .show(false)
}
