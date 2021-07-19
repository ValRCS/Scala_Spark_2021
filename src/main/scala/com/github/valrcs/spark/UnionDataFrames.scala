package com.github.valrcs.spark

import org.apache.spark.sql.DataFrame

object UnionDataFrames extends App {
  val filePaths = Seq(
    "./src/resources/csv/ACN_data.csv", //Accenture
    "./src/resources/csv/DIS_data.csv", //Disney
    "./src/resources/csv/NFLX_data.csv" //NFLX
  )
  val spark = SparkUtil.createSpark("dataUnion")

  def readCSV(filePath:String):DataFrame = spark.read
    .format("csv")
    .option("header", true)
    .option("inferSchema", true)
    .option("path", filePath)
    .load

  val dfs = filePaths.map(filePath => readCSV(filePath))

  dfs.foreach(df => {
    df.printSchema()
    df.describe().show(false)
    df.show(5, false)
  })

  //nice short way of writing but possible could get slow with many dataframes
  //https://stackoverflow.com/questions/37612622/spark-unionall-multiple-dataframes
  val bigDF = dfs.reduce(_ union _)

  bigDF.describe().show(false)

  bigDF.coalesce(1) //when we combine larger sets of data we might want to squeeze everything into partition
    .write
    .format("parquet")
    .mode("overwrite")
    .save("./src/resources/parquet/stocks_jul19")

  bigDF.coalesce(1)
    .write
    .format("csv")
    .option("header", true)
    .mode("overwrite")
    .save("./src/resources/csv/stocks_jul19")


}
