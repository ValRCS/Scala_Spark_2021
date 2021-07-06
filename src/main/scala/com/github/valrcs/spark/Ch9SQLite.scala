package com.github.valrcs.spark

import java.sql.DriverManager

object Ch9SQLite extends App {
  val spark = SparkUtil.createSpark("ch9DataSources")

  // in Scala
  val driver = "org.sqlite.JDBC"
  val path = "./src/resources/flight-data/jdbc/my-sqlite.db" //we want to use the relative path because projects can live in different places
  val url = s"jdbc:sqlite:${path}" //in other SQL database you would also add server, port, username, password here
  val tableName = "flight_info"

  //Sanity Check on connection

  //After you have defined the connection properties, you can test your connection to the database
  //itself to ensure that it is functional. This is an excellent troubleshooting technique to confirm that
  //your database is available to (at the very least) the Spark driver. This is much less relevant for
  //SQLite because that is a file on your machine but if you were using something like MySQL, you
  //could test the connection with the following:

  val connection = DriverManager.getConnection(url) //we are using generic Java JDBC driver here (no Spark at all)
  connection.isClosed()
  connection.close()

  // in Scala
  val dbDataFrame = spark.read.format("jdbc").option("url", url)
    .option("dbtable", tableName).option("driver", driver).load()

  dbDataFrame.printSchema()
  dbDataFrame.show(5, false)
}

