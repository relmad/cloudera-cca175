package com.github.relmad.cloudera.cca175

import org.apache.spark.{SparkConf, SparkContext}
import com.github.relmad.cloudera.cca175.utils._

object SparkPractice {
  def main(args: Array[String]) {

    // Create Spark Context
    val conf = new SparkConf(true)
      .setAppName("Practice")
      .setMaster("local[4]")
    val sc = new SparkContext(conf)

    ///////////////////////////////////////////////
    // Load data from HDFS and store results back to HDFS using Spark
    ///////////////////////////////////////////////

    // Here we use local fs, but it works transparently with HDFS
    val textIn = "src/main/resources/input/fake_records.tsv"
    val textOut = "src/main/resources/input/fake_records_out.tsv"
    deleteDir(textOut)
    val textFile = sc.textFile(textIn)
    textFile.saveAsTextFile(textOut)

    // Note that the records contain 21 fields, the last one being an int in [0, 100)

    ///////////////////////////////////////////////
    // Join disparate datasets together using Spark
    ///////////////////////////////////////////////

    // Let us create another RDD for later use
    val list = 0 until 100
    val numbers = sc.parallelize(list)

    val rightSide = numbers.map(num => (num, "cca175"))

    // choose the key from the complex tsv input
    val join = textFile.keyBy(_.split("\t")(20).toInt)
        .join(rightSide)

    join.collect().foreach(println)

    ///////////////////////////////////////////////
    // Calculate aggregate statistics (e.g., average or sum) using Spark
    ///////////////////////////////////////////////

    // Sum is built in
    val sum = numbers.sum()
    println(s"Sum: $sum")

    // Average is a little more hustle
    val accumulator = numbers
      .map(num => (num, 1))
      .reduce{case ((a, x), (b, y)) => (a + b, x + y)}

    val avg = accumulator._1.toDouble / accumulator._2
    println(s"Avg: $avg")

    // Note that if we do the average by key then we use reduceByKey
    // instead of reduce, which returns an RDD

    ///////////////////////////////////////////////
    // Filter data into a smaller dataset using Spark
    ///////////////////////////////////////////////
    val out = numbers.map(_ * 2).filter(_ < 10)
    out.collect().foreach(println)

    ///////////////////////////////////////////////
    // Write a query that produces ranked or sorted data using Spark
    ///////////////////////////////////////////////
    numbers.sortBy(x => x).collect().foreach(println)
  }
}
