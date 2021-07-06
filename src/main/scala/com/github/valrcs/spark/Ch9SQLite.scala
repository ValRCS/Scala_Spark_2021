package com.github.valrcs.spark

import org.apache.spark.sql.functions.desc

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
  val dbDataFrame = spark
    .read
    .format("jdbc")
    .option("url", url)
    .option("dbtable", tableName)
    .option("driver", driver)
    .load

  dbDataFrame.printSchema()
  dbDataFrame.show(5, false)

  //SQLite has rather simple configurations (no users, for example). Other databases, like
  //PostgreSQL, require more configuration parameters. Let’s perform the same read that we just
  //performed, except using PostgreSQL this time:
  //// in Scala
  //val pgDF = spark.read
  //.format("jdbc")
  //.option("driver", "org.postgresql.Driver")
  //.option("url", "jdbc:postgresql://database_server")
  //.option("dbtable", "schema.tablename")
  //.option("user", "username").option("password","my-secret-password").load()
  //and also you would have to add the PostgreSQL driver using SBT

  dbDataFrame.select("DEST_COUNTRY_NAME")
    .distinct()
    .orderBy(desc("DEST_COUNTRY_NAME"))
    .show(5)

//QUERY PUSHDOWN

  //Query Pushdown
  //First, Spark makes a best-effort attempt to filter data in the database itself before creating the
  //DataFrame. For example, in the previous sample query, we can see from the query plan that it
  //selects only the relevant column name from the table:
  dbDataFrame.select("DEST_COUNTRY_NAME").distinct().explain

  //we can write SQL directly using pushdown queries
  //Spark can’t translate all of its own functions into the functions available in the SQL database in
  //which you’re working. Therefore, sometimes you’re going to want to pass an entire query into
  //your SQL that will return the results as a DataFrame. Now, this might seem like it’s a bit
  //complicated, but it’s actually quite straightforward. Rather than specifying a table name, you just
  //specify a SQL query. Of course, you do need to specify this in a special way; you must wrap the
  //query in parenthesis and rename it to something—in this case, I just gave it the same table name:

  // in Scala
  val pushdownQuery = """(SELECT DISTINCT(DEST_COUNTRY_NAME)
                       FROM flight_info
                       ORDER BY dest_country_name DESC)
        AS flight_info"""
  val dbDataFrame2 = spark.read.format("jdbc")
    .option("url", url).option("dbtable", pushdownQuery).option("driver", driver)
    .load()
  dbDataFrame2.show(10,false)

  //Reading from databases in parallel
  //All throughout this book, we have talked about partitioning and its importance in data
  //processing. Spark has an underlying algorithm that can read multiple files into one partition, or
  //conversely, read multiple partitions out of one file, depending on the file size and the
  //“splitability” of the file type and compression. The same flexibility that exists with files, also
  //exists with SQL databases except that you must configure it a bit more manually. What you can
  //configure, as seen in the previous options, is the ability to specify a maximum number of
  //partitions to allow you to limit how much you are reading and writing in parallel:

  // in Scala
  val dbDataFrame3 = spark.read
    .format("jdbc")
    .option("url", url)
    .option("dbtable", tableName)
    .option("driver", driver)
    .option("numPartitions", 10) //optimization which would be important for larger databases
    .load()
  dbDataFrame3.show(5)

  //There are several other optimizations that unfortunately only seem to be under another API set.
  //You can explicitly push predicates down into SQL databases through the connection itself. This
  //optimization allows you to control the physical location of certain data in certain partitions by
  //specifying predicates.

  // in Scala
  val props = new java.util.Properties
  props.setProperty("driver", "org.sqlite.JDBC")
  val predicates = Array(
    "DEST_COUNTRY_NAME = 'Sweden' OR ORIGIN_COUNTRY_NAME = 'Sweden'",
    "DEST_COUNTRY_NAME = 'Anguilla' OR ORIGIN_COUNTRY_NAME = 'Anguilla'")
  spark.read.jdbc(url, tableName, predicates, props).show()
  spark.read.jdbc(url, tableName, predicates, props).rdd.getNumPartitions // 2

  val parquetFilePath = "./src/resources/parquet/jul7"
  val fromParquetDF = spark
    .read
    .format("parquet")
    .load(parquetFilePath)
  fromParquetDF.show(5, false)

  //over/writing to a new SQL database
  val newPath = "jdbc:sqlite:./src/resources/sql/dirty-data.db"
  fromParquetDF
//    .na.drop //dropping rows where there is at least one null value
    .write
    .mode("overwrite")
    .jdbc(newPath, tableName, props)

  //TODO read Text files
}

