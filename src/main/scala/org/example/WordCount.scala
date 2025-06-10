package org.example

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("WordCount_2025_04_12")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    // Step 1: Read input text file
    val textRdd = sc.textFile("hdfs://namenode:9000/reviews.txt")

    // Step 2: Split lines into words (flatMap)
    //  For a single word, we'll create an RDD directly from a list
    val singleWordRdd = sc.parallelize(List("hello"))

    //  Map each word to a key-value pair (word, 1)
    val wordCountsRdd = singleWordRdd.map(word => (word, 1))

    //  ReduceByKey to count the occurrences of each word
    val totalCountsRdd = wordCountsRdd.reduceByKey((a, b) => a + b)

    //  Print the results (you'd usually save this to a file)
    totalCountsRdd.collect().foreach(println)

    sc.stop()
  }

}
