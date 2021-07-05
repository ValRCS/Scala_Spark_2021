package com.github.valrcs.spark
import org.apache.spark.sql.functions.{col, column}

object Ch8Joins extends App {
    //Join Types
  //Whereas the join expression determines whether two rows should join, the join type determines
  //what should be in the result set. There are a variety of different join types available in Spark for
  //you to use:
  //Inner joins (keep rows with keys that exist in the left and right datasets)
  //Outer joins (keep rows with keys in either the left or right datasets)
  //Left outer joins (keep rows with keys in the left dataset)
  //Right outer joins (keep rows with keys in the right dataset)
  //Left semi joins (keep the rows in the left, and only the left, dataset where the key
  //appears in the right dataset)
  //Left anti joins (keep the rows in the left, and only the left, dataset where they do not
  //appear in the right dataset)
  //Natural joins (perform a join by implicitly matching the columns between the two
  //datasets with the same names)
  //Cross (or Cartesian) joins (match every row in the left dataset with every row in the
  //right dataset)

  val spark = SparkUtil.createSpark("ch8")


  //some simple Datasets
  import spark.implicits._
  // in Scala
  val person = Seq(
    (0, "Bill Chambers", 0, Seq(100)),
    (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
    (2, "Michael Armbrust", 1, Seq(250, 100)),
  (3, "Valdis Saulespurens", 30, Seq(50)),
    (4, "Homer Simpson", 77, Seq(300,400))) //Homer just entered some wild numbers
    .toDF("id", "name", "graduate_program", "spark_status")
  val graduateProgram = Seq(
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley"),
    (30, "Masters", "CS", "University of Latvia"),
    (40, "Ph.D.", "CS", "Riga Technical University")
  )
    .toDF("id", "degree", "department", "school")
  val sparkStatus = Seq(
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor"),
    (50, "User")
  )
    .toDF("id", "status")

  person.show(false)
  graduateProgram.show(false)
  sparkStatus.show(false)

  //lets create some views!
  person.createOrReplaceTempView("person")
  graduateProgram.createOrReplaceTempView("graduateProgram")
  sparkStatus.createOrReplaceTempView("sparkStatus")

  //Inner Joins
  //Inner joins evaluate the keys in both of the DataFrames or tables and include (and join together)
  //only the rows that evaluate to true. In the following example, we join the graduateProgram
  //DataFrame with the person DataFrame to create a new DataFrame:
  //// in Scala
  val joinExpression = person.col("graduate_program") === graduateProgram.col("id")


  //Keys that do not exist in both DataFrames will not show in the resulting DataFrame. For
  //example, the following expression would result in ZERO values in the resulting DataFrame:
  //// in Scala
  //val wrongJoinExpression = person.col("name") === graduateProgram.col("school")

  //so we will add to person table description of the graduate program
  person.join(graduateProgram, joinExpression).show(false)
  //same with sql syntax - need tempViews for both tables!
  spark.sql("""SELECT * FROM person
              |JOIN graduateProgram
              |ON person.graduate_program = graduateProgram.id""".stripMargin)
    .show(false)

//  We can also specify this explicitly by passing in a third parameter, the joinType:
  // in Scala
  var joinType = "inner"
  person.join(graduateProgram, joinExpression, joinType).show(false)


  //OUTER Joins
  //Outer joins evaluate the keys in both of the DataFrames or tables and includes (and joins
  //together) the rows that evaluate to true or false. If there is no equivalent row in either the left or
  //right DataFrame, Spark will insert null:

  //so persons with non existant graduate programs will show up
  //also graduate programs which no on is using will show up here
  joinType = "outer"
  person.join(graduateProgram, joinExpression, joinType).show(false)
  //same as above
  spark.sql("""SELECT * FROM person FULL OUTER JOIN graduateProgram
              |ON graduate_program = graduateProgram.id""".stripMargin)
    .show(false)


  //less common joins

//  Left Outer Joins
//  Left outer joins evaluate the keys in both of the DataFrames or tables and includes all rows from
//    the left DataFrame as well as any rows in the right DataFrame that have a match in the left
//  DataFrame. If there is no equivalent row in the right DataFrame, Spark will insert null
  //so left side of row has to exist, but right side of row might not

  joinType = "left_outer"
  //all graduate programs even without finishers should show here
  graduateProgram.join(person, joinExpression, joinType).show(false)
  //Homer should show up below
  person.join(graduateProgram, joinExpression, joinType).show(false)

//  Right Outer Joins
//  Right outer joins evaluate the keys in both of the DataFrames or tables and includes all rows
//  from the right DataFrame as well as any rows in the left DataFrame that have a match in the right
//  DataFrame. If there is no equivalent row in the left DataFrame, Spark will insert null:

  joinType = "right_outer"
  //so here Homer will not exist since he has no valid school id,
  // schools will show up even if no one goes there
  person.join(graduateProgram, joinExpression, joinType).show(false)

  //now Homer will show up, but schools will not
  graduateProgram.join(person, joinExpression, joinType).show(false)
  //same as above
  spark.sql("""SELECT * FROM graduateProgram RIGHT OUTER JOIN person
              |ON person.graduate_program = graduateProgram.id""".stripMargin)
    .show(false)


  //left semi join - you do not get any values from the right side - sort of like a filter
  //Left Semi Joins
  //Semi joins are a bit of a departure from the other joins. They do not actually include any values
  //from the right DataFrame. They only compare values to see if the value exists in the second
  //DataFrame. If the value does exist, those rows will be kept in the result, even if there are
  //duplicate keys in the left DataFrame. Think of left semi joins as filters on a DataFrame, as
  //opposed to the function of a conventional join:

  joinType = "left_semi"
  graduateProgram.join(person, joinExpression, joinType).show(false)



  //TODO let's join all 3 tables together! we start with person we have graduate_program which we just did
  //also we have spark status
}
