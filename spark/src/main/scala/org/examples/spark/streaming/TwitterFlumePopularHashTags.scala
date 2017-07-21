package org.examples.spark.streaming

import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.twitter._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume._
 
  /*
 * A Spark Streaming application that receives tweets on certain 
 * keywords from twitter datasource and find the popular hashtags
 * 
 * Arguments: <comsumerKey> <consumerSecret> <accessToken> <accessTokenSecret> <keyword_1> ... <keyword_n>
 * <comsumerKey>        - Twitter consumer key 
 * <consumerSecret>     - Twitter consumer secret
 * <accessToken>        - Twitter access token
 * <accessTokenSecret>  - Twitter access token secret
 * <keyword_1>          - The keyword to filter tweets
 * <keyword_n>          - Any number of keywords to filter tweets
 */
  

object TwitterFlumePopularHashTags {
 
  val conf = new SparkConf().setAppName("Flume source - Twitter Popular Hashtags")
      .setMaster("local[6]")
  val sc = new SparkContext(conf)
  
  def main(args: Array[String]) {
    
    sc.setLogLevel("WARN")
    
    
    val ssc = new StreamingContext(sc, Seconds(5))
    val filters = args.takeRight(args.length)

    val stream = FlumeUtils.createStream(ssc, "osboxes", 9988)
    val tweets = stream.map(e => new String(e.event.getBody.array))
    tweets.print()
    
    val hashTags = tweets.flatMap(status => status.split(" ").filter(_.startsWith("#")))
    
    val topCounts60 = hashTags.map((_,1)).reduceByKeyAndWindow(_+_, Seconds(60))
        .map{ case (topic,count) => (count,topic) }
        .transform(_.sortByKey(false))
        
    val topCounts10 = hashTags.map((_,1)).reduceByKeyAndWindow(_+_, Seconds(10))
        .map{ case (topic,count) => (count,topic) }
        .transform(_.sortByKey(false))
   
    topCounts60.foreachRDD( rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total): ".format(rdd.count()))
      topList.foreach{ case (count,tag) => println("%s (%s tweets)".format(tag, count)) }   
    })
    
    topCounts10.foreachRDD( rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total): ".format(rdd.count()))
      topList.foreach{ case (count,tag) => println("%s (%s tweets)".format(tag, count)) }   
    })
    
    stream.count().map(cnt => "Received " + cnt + " flume events.").print()
    ssc.start()
    ssc.awaitTermination()
    
  }
  
}