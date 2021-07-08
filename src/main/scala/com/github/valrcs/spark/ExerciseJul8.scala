package com.github.valrcs.spark

import org.apache.spark.sql.functions.{col, regexp_extract}

object ExerciseJul8 extends App {
  //TODO load 1920_Aspazija.txt with Spark(not regular Scala)

  //TODO extract all titles of the poems
  //TODO save the poem titles in a new text file(actually folder) called 1920_Aspazija_titles.txt

  val spark = SparkUtil.createSpark("exerJul8AspazijaTitles")
  val filePath = "./src/resources/txt/1920_Aspaz.txt"

  val df = spark.read
    .textFile(filePath)

  df.printSchema()

  df.show(false)

//  val titleRegex = "^([A-ZĀČĒĢĪĶĻŅŠŪŽ]+)" //this will will pick first words of all Capitalized Lines
  val titleRegex = "^([A-ZĀČĒĢĪĶĻŅŠŪŽ]{2,}).*" //so we want WHOLE lines where they start with at LEAST 2 UPPERCASE LETTERS
  val exclamationRegex = ".*!$" //so we want ALL characters in the string as long as the string ends with !



  df
    .withColumn("title", regexp_extract(col("value"), titleRegex, 0))
    .select(col("title"))
    .na
    .drop
    .filter("title != ''")
    .write
    .mode("overwrite")
    .text("./src/resources/1920_Aspazija_titles.txt")

  df.withColumn("exclam", regexp_extract(col("value"), exclamationRegex, 0))
    .select("exclam")
    .filter("exclam != ''")
    .show(10, false)

  ////https://stackoverflow.com/questions/28156769/foreign-language-characters-in-regular-expression-in-c-sharp
  val titleAnyLanguageRegex = "^(\\p{Lu}{2,}).*" //so this will cover all languages (including letters from Chinese, Indian, Arabic etc)

  df.withColumn("title", regexp_extract(col("value"), titleAnyLanguageRegex, 0))
    .select(col("title"))
    .na
    .drop
    .filter("title != ''")
    .show(10, false)
}
