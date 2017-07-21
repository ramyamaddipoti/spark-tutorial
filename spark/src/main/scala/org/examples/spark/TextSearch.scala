package org.examples.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


object TextSearch {
  
  def main(args: Array[String]) {
    
    val conf = new SparkConf()
      .setAppName("Text Search").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    val sqlContext= new SQLContext(sc)
    import sqlContext.implicits.stringRddToDataFrameHolder
    val textFile = sc.textFile("input/errors.txt")
    // Creates a DataFrame having a single column named "line"
    val df = textFile.toDF("line")
    val errors = df.filter(col("line").like("%ERROR%"))
    // Counts all the errors
    errors.count()
    // Counts errors mentioning MySQL
    val sqlErrors = errors.filter(col("line").like("%MySQL%"))
    // Counts the SQL errors
    sqlErrors.count()
    // Fetches the MySQL errors as an array of strings
    sqlErrors.collect().foreach(println)
    
  }
}