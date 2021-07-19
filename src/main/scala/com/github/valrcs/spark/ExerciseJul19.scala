package com.github.valrcs.spark

import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.functions.{avg, col, desc, expr, lit, round}

object ExerciseJul19 extends App {
  val spark = SparkUtil.createSpark("exerciseJul19")
  val filePath = "./src/resources/csv/stocks_2013_2018.csv"

  //TODO read the file, infer schema

  //TODO calculate the daily change in percentages for all rows
  //TODO group the results by day and calculate the average change for all stocks combined by day

  val df = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(filePath)
    .coalesce(5) //?

  df.printSchema()
  df.show(5, false)

  //TODO calculate the daily change in percentages for all rows

//  val change = df.withColumn("change", round(lit(100) * (col("close") - col("open"))/col("open"), 2))
//  change.show()
//  //change in %, % sign not shown
//
//  //TODO group the results by day and calculate the average change for all stocks combined by day
//  change.groupBy("date").avg("change").show(false) //we should probably do round at the end
  //if you do too much rounding in between you may actually start losing meaningful information

  val dfWithReturn = df
    .withColumn("daily_return", (col("close") - col("open"))/col("open")*100)

  dfWithReturn
    .groupBy("date")
//    .pivot("Name")
    .agg(avg("daily_return"))
//    .orderBy(desc("date"))
    .orderBy(desc("avg(daily_return)"))
    .show(10, false)

  dfWithReturn.orderBy(desc("daily_return"))
    .show(10, false) //days could be matching

  dfWithReturn.orderBy(desc("daily_return"))
    .where("Name != 'NFLX'")
    .show(10, false) //days could be matching

  val dfWithPreviousDay = df.withColumn("prevOpen", expr("" +
    "LAG (open,1,0) " +
    "OVER (PARTITION BY Name " +
    "ORDER BY date )"))
    .withColumn("prevClose", expr("" +
      "LAG (close,1,0) " +
      "OVER (PARTITION BY Name " +
      "ORDER BY date )"))
  dfWithPreviousDay.show(10, false)

  import org.apache.spark.ml.feature.RFormula
  val supervised = new RFormula()
    .setFormula("open ~ prevOpen + prevClose ")

  val ndf = supervised
    .fit(dfWithPreviousDay) //prepares the formula
    .transform(dfWithPreviousDay) //generally transform will create the new data

  ndf.show(10, false)

  val cleanDf = ndf.where("prevOpen != 0.0")
  cleanDf.show(10, false)

  val linReg = new LinearRegression()

  val Array(train,test) = cleanDf.randomSplit(Array(0.75,0.25))

  val lrModel = linReg.fit(train)

  val intercept = lrModel.intercept
  val coefficients = lrModel.coefficients
  val x1 = coefficients(0)
  val x2 = coefficients(1) //of course we would have to know how many x columns we have as features

  println(s"Intercept: $intercept and coefficient for x1 is $x1 and for x2 is $x2")
  //simple linear regression is unlikely to yield anything on financial data which follow random walk,
  //but the idea is what is important
  //here we see that the last day's closing price is dominating the previous day's opening price
  //which is natural since the opening day does correlate strongly with yesterday's closing

  val summary = lrModel.summary

  //to truly test this model we should be using different stocks or different dates for these 3 stocks

  val predictedDf = lrModel.transform(test)

  predictedDf.show(10, false)

}
