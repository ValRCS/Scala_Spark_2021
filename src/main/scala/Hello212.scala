import org.apache.spark.sql.SparkSession

object Hello212 extends App {
  println(s"Testing Scala version: ${util.Properties.versionNumberString}")
  println("Testing")
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  //session is also commonly used instead of spark as a value name
  println(s"Session started on Spark version ${spark.version}")
}
