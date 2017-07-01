package org.examples.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object WordCount {

  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setAppName("WordCount")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val test = sc.textFile("food.txt")
    test.flatMap { 
      line => line.split(" ") 
    }
    .map { 
      word => (word,1) 
    }
    .reduceByKey(_ + _)
    .saveAsTextFile("food.count.txt")
  
  }
}