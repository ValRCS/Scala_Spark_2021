package com.github.valrcs.spark

object Ch11Datasets extends App {

  val spark = SparkUtil.createSpark("ch11Datasets")
  val filePath = "./src/resources/flight-data/parquet/2010-summary.parquet"

  val df = spark.read.format("parquet").load(filePath)

  df.printSchema()
  df.show(5)

  case class Flight(DEST_COUNTRY_NAME: String,
                    ORIGIN_COUNTRY_NAME: String, count: BigInt) //BigInt has practically no limit to its size

  import spark.implicits._
  val dataSet = df.as[Flight]

  dataSet.show(5)

  //so we can write a function to perform filtering
  //TIP
  //You’ll notice in the following example that we’re going to create a function to define this filter. This is
  //an important difference from what we have done thus far in the book. By specifying a function, we are
  //forcing Spark to evaluate this function on every row in our Dataset. This can be very resource
  //intensive. For simple filters it is always preferred to write SQL expressions. This will greatly reduce
  //the cost of filtering out the data while still allowing you to manipulate it as a Dataset later on

  def originIsDestination(flight_row: Flight): Boolean = {
    flight_row.ORIGIN_COUNTRY_NAME == flight_row.DEST_COUNTRY_NAME //so only USA to USA flights will be from this particular dataset
  }

  dataSet
    .filter(flight_row => originIsDestination(flight_row))
    .show(5)

  val destinations = dataSet.map(flight => flight.DEST_COUNTRY_NAME)

  destinations.show(5)

  dataSet.groupBy("DEST_COUNTRY_NAME").count().show(5) //this will be a DataFrame meaning we lose the types we defined

  //joins are also possible

  //again unless you need really strict type safety stick with DataFrames as much as possible and use the SQL like expressions
  //and use our org.apache.spark.sql.functions

}
