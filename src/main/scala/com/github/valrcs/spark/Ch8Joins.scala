package com.github.valrcs.spark
import org.apache.spark.sql.functions.{col, column, expr}

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

  // in Scala
  val gradProgram2 = graduateProgram.union(Seq(
    (0, "Masters", "Duplicated Row", "Duplicated School")).toDF())
  gradProgram2.createOrReplaceTempView("gradProgram2")

  //so if there are multiple matches on the id, then all of those rows will be shown
  spark.sql("""SELECT * FROM gradProgram2
              |LEFT SEMI JOIN person
              |ON gradProgram2.id = person.graduate_program""".stripMargin)
    .show(false)

  //Left Anti Joins
  //Left anti joins are the opposite of left semi joins. Like left semi joins, they do not actually
  //include any values from the right DataFrame. They only compare values to see if the value exists
  //in the second DataFrame. However, rather than keeping the values that exist in the second
  //DataFrame, they keep only the values that do not have a corresponding key in the second
  //DataFrame. Think of anti joins as a NOT IN SQL-style filter:
  joinType = "left_anti" //so we will see those rows from the left table which did not have a join

  graduateProgram.join(person, joinExpression, joinType).show(false)
  //same in SQL
  spark.sql("""SELECT * FROM graduateProgram
              |LEFT ANTI JOIN person
              |ON graduateProgram.id = person.graduate_program""".stripMargin)
    .show(false)

  //WARNING
  //There exists a Natural join which is handy if you have same column names in different tables
  //otherwise avoid since it will join on the wrong columns!!!
  //Implicit is always dangerous! The following query will give us incorrect results because the two
  //DataFrames/tables share a column name (id), but it means different things in the datasets. You should
  //always use this join with caution
  spark.sql("""SELECT * FROM graduateProgram NATURAL JOIN person""")
    .show(false) //so this will be nonsense join because it will join on ids

  //finally there is a Cartesean join meaning it will match everything without checking any match condition so lots of rows
  //so if you have 2 1000 row tables you will get 1 million row Carteasean join table
  //Cross (Cartesian) Joins
  //The last of our joins are cross-joins or cartesian products. Cross-joins in simplest terms are inner
  //joins that do not specify a predicate. Cross joins will join every single row in the left DataFrame
  //to ever single row in the right DataFrame. This will cause an absolute explosion in the number of
  //rows contained in the resulting DataFrame. If you have 1,000 rows in each DataFrame, the cross-
  //join of these will result in 1,000,000 (1,000 x 1,000) rows. For this reason, you must very
  //explicitly state that you want a cross-join by using the cross join keyword

  joinType = "cross"
  graduateProgram.join(person, joinExpression, joinType).show(false) //so 5 persons * 5 school programs = 25

  spark.sql("""SELECT * FROM graduateProgram
              |CROSS JOIN person
              |ON graduateProgram.id = person.graduate_program""".stripMargin)
    .show(false)
  //so SPARK protected us from our crazyness, we have to explicitly call out that we want a cross join

  person.crossJoin(graduateProgram).show(25,false) //so now we get our 25 rows

  spark.sql("""SELECT * FROM graduateProgram CROSS JOIN person""").show(25, false)

  //Joins on Complex Types
  //Even though this might seem like a challenge, it’s actually not. Any expression is a valid join
  //expression, assuming that it returns a Boolean:

  person
    .withColumnRenamed("id", "personId")
    .join(sparkStatus, expr("array_contains(spark_status, id)"))
    .show(false)

  spark.sql("""SELECT * FROM
              |(select id as personId, name, graduate_program, spark_status FROM person)
              |INNER JOIN sparkStatus ON array_contains(spark_status, id)""".stripMargin)
    .show(false)

  joinType = "inner"
  person
    .withColumnRenamed("id", "personId")
    .join(sparkStatus, expr("array_contains(spark_status, id)"))
    .join(graduateProgram, joinExpression, joinType)
    .show(false)
  //also we have spark status

  //Handling Duplicate Column Names
  //One of the tricky things that come up in joins is dealing with duplicate column names in your
  //results DataFrame. In a DataFrame, each column has a unique ID within Spark’s SQL Engine,
  //Catalyst. This unique ID is purely internal and not something that you can directly reference.
  //This makes it quite difficult to refer to a specific column when you have a DataFrame with
  //duplicate column names.
  //This can occur in two distinct situations:
  //The join expression that you specify does not remove one key from one of the input
  //DataFrames and the keys have the same column name
  //Two columns on which you are not performing the join have the same name

  val gradProgramDupe = graduateProgram.withColumnRenamed("id", "graduate_program")
  val joinExpr = gradProgramDupe.col("graduate_program") === person.col(
    "graduate_program")

  person.join(gradProgramDupe, joinExpr).show(false)

  //this join will fail because of ambiguity
//  person.join(gradProgramDupe, joinExpr).select("graduate_program").show(false)

  //Approach 1: Different join expression
  //When you have two keys that have the same name, probably the easiest fix is to change the join
  //expression from a Boolean expression to a string or sequence. This automatically removes one of
  //the columns for you during the join:

  person.join(gradProgramDupe,"graduate_program").show(false)
  person.join(gradProgramDupe,"graduate_program").select("graduate_program").show()

//  Approach 2: Dropping the column after the join
//    Another approach is to drop the offending column after the join. When doing this, we need to
//  refer to the column via the original source DataFrame. We can do this if the join uses the same
//  key names or if the source DataFrames have columns that simply have the same name
  person.join(gradProgramDupe, joinExpr).drop(person.col("graduate_program"))
    .select("graduate_program").show()
  val joinExpr2 = person.col("graduate_program") === graduateProgram.col("id")
  person.join(graduateProgram, joinExpr2).drop(graduateProgram.col("id")).show()

  //Approach 3: Renaming a column before the join
  //We can avoid this issue altogether if we rename one of our columns before the join:
  val gradProgram3 = graduateProgram.withColumnRenamed("id", "grad_id")
  val joinExpr3 = person.col("graduate_program") === gradProgram3.col("grad_id")
  person.join(gradProgram3, joinExpr3).show()

  //small table to big table via broadcast (instead of shuffle join)
  //At the beginning of this join will be a large communication, just like in the previous type of join.
  //However, immediately after that first, there will be no further communication between nodes.
  //This means that joins will be performed on every single node individually, making CPU the
  //biggest bottleneck. For our current set of data, we can see that Spark has automatically set this up
  //as a broadcast join by looking at the explain plan:

  val joinExpr4 = person.col("graduate_program") === graduateProgram.col("id")
  person.join(graduateProgram, joinExpr4).explain()

  //With the DataFrame API, we can also explicitly give the optimizer a hint that we would like to
  //use a broadcast join by using the correct function around the small DataFrame in question. In this
  //example, these result in the same plan we just saw; however, this is not always the case:
  //import org.apache.spark.sql.functions.broadcast
  //val joinExpr = person.col("graduate_program") === graduateProgram.col("id")
  //person.join(broadcast(graduateProgram), joinExpr).explain()
  //The SQL interface also includes the ability to provide hints to perform joins. These are not
  //enforced, however, so the optimizer might choose to ignore them. You can set one of these hints
  //by using a special comment syntax. MAPJOIN, BROADCAST, and BROADCASTJOIN all do the same
  //thing and are all supported:
  //-- in SQL
  //SELECT /*+ MAPJOIN(graduateProgram) */ * FROM person JOIN graduateProgram
  //ON person.graduate_program = graduateProgram.id
  //This doesn’t come for free either: if you try to broadcast something too large, you can crash your
  //driver node (because that collect is expensive). This is likely an area for optimization in the
  //future.
  //Little table–to–little table
  //When performing joins with small tables, it’s usually best to let Spark decide how to join them.
  //You can always force a broadcast join if you’re noticing strange behavior.
}
