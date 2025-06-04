package org.example
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object SimpleDataFrameApp {

  def main(args: Array[String]): Unit = {
    // Step 1: Create SparkSession
    val spark = SparkSession.builder()
      .appName("Simple DataFrame Example")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Step 2: Create DataFrame from a Sequence
    val peopleDF = Seq(
      (1, "Alice", 29),
      (2, "Bob", 34),
      (3, "Cathy", 25)
    ).toDF("id", "name", "age")

    // Step 3: Show the DataFrame
    println("Full DataFrame:")
    peopleDF.show()

    // Step 4: Filter rows where age > 30
    println("Filtered (age > 30):")
    peopleDF.filter($"age" > 30).show()

    // Step 5: Group by age and count
    println("Grouped by age:")
    peopleDF.groupBy("age").count().show()

    // Step 6: Stop Spark
    spark.stop()
  }
}
