package org.examples.spark

import org.apache.spark.{ SparkConf, SparkContext }
import java.io.File
import org.apache.commons.io.FileUtils 

object UberDataAnalysis {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Uber Data Analysis").setMaster("local[*]")
    val sc = new SparkContext(conf);

    val dataset = sc.textFile("input/uber");
    val header = dataset.first();
    val format = new java.text.SimpleDateFormat("MM/dd/yyyy");
    val days = Array("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat")
    val eliminate = dataset.filter(line => line != header)
    val split = eliminate.map(line => line.split(",")) 
     .map(x => (x(0), format.parse(x(1)), x(3)))
    val combined = split.map(x => (x._1 + " " + days(x._2.getDay), x._3.toInt))
    val arrange = combined.reduceByKey(_ + _).map(item => item.swap).sortByKey(false)
      //.collect.foreach(println)
    
    val outputPath = "output/uber";
    FileUtils.deleteQuietly(new File(outputPath))
    arrange.saveAsTextFile(outputPath)  

  }
}